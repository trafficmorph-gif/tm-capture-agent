package com.trafficmorph.capture;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Multi-producer / single-consumer (MPSC), bounded, lock-free ring
 * buffer of {@link CaptureEvent}s. Implements the canonical "per-slot
 * sequence number" pattern (the same pattern that underpins the LMAX
 * Disruptor's MPSC mode) with minimal scaffolding.
 *
 * <h2>Why this pattern</h2>
 * <ul>
 *   <li>{@code ArrayBlockingQueue} would work functionally but
 *       serialises every offer/poll through one {@code ReentrantLock}
 *       — under N producer threads the lock becomes the bottleneck
 *       exactly when this logger is most under pressure.</li>
 *   <li>{@code ConcurrentLinkedQueue} / {@code LinkedTransferQueue}
 *       are unbounded — they'd grow indefinitely on a stuck writer
 *       and turn back-pressure into OOM.</li>
 *   <li>This pattern: producers claim a slot via a single CAS; reads
 *       and writes go through {@code AtomicLongArray} sequence
 *       markers (volatile semantics, no locks). The single consumer
 *       is the writer thread; its consumerSeq is also AtomicLong so
 *       a producer can race with it for eviction (see
 *       {@link #offerOrEvict}).</li>
 * </ul>
 *
 * <h2>Slot lifecycle</h2>
 * <p>Each slot has a sequence number that encodes its state relative
 * to the producer claim position {@code pos}:
 * <ul>
 *   <li>{@code seq == pos}     → slot ready to be claimed by a producer
 *                                expecting that {@code pos}.</li>
 *   <li>{@code seq == pos + 1} → producer has published its event;
 *                                consumer at index {@code pos} may
 *                                read it.</li>
 *   <li>{@code seq == pos + capacity} → consumer has drained the slot;
 *                                ready for the next round of producers
 *                                (whose {@code pos} will be
 *                                {@code originalPos + capacity}).</li>
 * </ul>
 *
 * <h2>Memory ordering</h2>
 * <p>The producer writes {@code slots[i] = event} BEFORE publishing
 * {@code sequences.set(i, pos+1)}. {@code AtomicLongArray.set} is a
 * volatile store (release semantics under the JMM), so the consumer's
 * subsequent acquire-read of the sequence happens-before the slot
 * write — the consumer is guaranteed to see the event reference.
 *
 * <h2>Bounded contract</h2>
 * <p>{@link #offer(CaptureEvent)} returns {@code false} immediately
 * when the slot the next claim would target hasn't yet been drained
 * by the consumer — i.e. the ring is full. No spinning, no blocking.
 * For DROP_OLD semantics, callers use {@link #offerOrEvict} which
 * evicts the oldest event(s) under back-pressure until the new event
 * fits.
 */
final class EventRingBuffer {

    private final CaptureEvent[] slots;
    private final AtomicLongArray sequences;
    private final int mask;
    private final int capacity;

    /** Next position a producer would claim. */
    private final AtomicLong producerSeq = new AtomicLong(0);

    /**
     * Next position the consumer would drain. AtomicLong so producer-
     * side eviction (DROP_OLD) can race with the writer thread's
     * normal poll — whoever wins the CAS owns the slot. The
     * non-evicting code path is otherwise identical to a plain-long
     * single-consumer counter; the CAS adds ~2-5ns on the consumer's
     * hot path, acceptable for the eviction feature it enables.
     */
    private final AtomicLong consumerSeq = new AtomicLong(0);

    EventRingBuffer(int capacity) {
        if (capacity <= 0 || Integer.bitCount(capacity) != 1) {
            throw new IllegalArgumentException(
                    "capacity must be a positive power of two, was " + capacity);
        }
        this.capacity = capacity;
        this.mask = capacity - 1;
        this.slots = new CaptureEvent[capacity];
        this.sequences = new AtomicLongArray(capacity);
        // Initial state: slot i is ready for the producer claiming pos=i.
        for (int i = 0; i < capacity; i++) {
            sequences.set(i, i);
        }
    }

    int capacity() {
        return capacity;
    }

    /**
     * Producer side, non-evicting. Returns {@code true} on success,
     * {@code false} if the ring is full (caller drops + counts).
     *
     * <p>Lock-free; multiple producers can be in flight simultaneously
     * and never block each other. Each retry costs one re-read of
     * {@code producerSeq} plus one re-read of the target slot's
     * sequence — very cheap on modern CPUs.
     */
    boolean offer(CaptureEvent event) {
        for (;;) {
            long pos = producerSeq.get();
            int idx = (int) (pos & mask);
            long seq = sequences.get(idx);
            long diff = seq - pos;
            if (diff == 0) {
                // Slot is in the "ready for producer at pos" state.
                // Try to claim pos for ourselves.
                if (producerSeq.compareAndSet(pos, pos + 1)) {
                    // Won the race. We own slot idx for this pos.
                    slots[idx] = event;
                    // Publish: volatile store flushes the slot write
                    // out under release semantics. Consumer that sees
                    // sequences[idx] == pos+1 is guaranteed to also
                    // see slots[idx] == event.
                    sequences.set(idx, pos + 1);
                    return true;
                }
                // Lost CAS — another producer claimed pos. Retry.
            } else if (diff < 0) {
                // sequences[idx] < pos means the slot still holds an
                // event from a previous round (capacity ago) that the
                // consumer hasn't drained yet. Ring is full.
                return false;
            }
            // diff > 0: another producer claimed pos and has already
            // published — our producerSeq.get() was stale. Re-read.
        }
    }

    /**
     * Producer side, DROP_OLD policy. Tries to make room by evicting
     * the oldest event(s); returns information about the outcome so
     * the caller can do correct stats accounting.
     *
     * <p>Per-eviction logic: CAS-advance {@link #consumerSeq} by one
     * to atomically "steal" the oldest pending slot from the writer
     * thread. The writer's {@link #poll()} also CASes its claim, so
     * either party can win on a given slot — whoever loses sees a
     * {@code null} or stale read and retries on the next round.
     *
     * <p>Bounded loop: an upper limit on eviction attempts prevents
     * a pathological livelock if many producers concurrently evict
     * faster than they offer. The limit is generous (2 ×
     * {@code capacity}) so legitimate back-pressure always succeeds.
     *
     * <h2>Return value encoding</h2>
     * <p>One {@code int} carries two pieces of information so the
     * hot path doesn't allocate a result record:
     * <ul>
     *   <li><b>{@code result >= 0}</b> — success. {@code result} is
     *       the number of evictions performed (may be 0). The new
     *       event was enqueued; caller bumps {@code logged} by 1
     *       and {@code dropped} by {@code result}.</li>
     *   <li><b>{@code result < 0}</b> — gave up. The new event was
     *       NOT enqueued. The number of evictions that occurred
     *       before giving up is {@code -result - 1}. Caller bumps
     *       {@code dropped} by ({@code -result - 1 + 1}) — the
     *       extra +1 accounts for the new event itself, which is
     *       lost.</li>
     * </ul>
     */
    int offerOrEvict(CaptureEvent event) {
        // Cap iterations defensively at 2 × capacity. Compute in
        // LONG so a large valid {@code queueCapacity} (Builder
        // allows up to {@code 1 << 30}) doesn't wrap to a negative
        // int and silently short-circuit the retry loop. The prior
        // bug: {@code int maxAttempts = capacity * 2} for
        // {@code capacity = 1 << 30} produced
        // {@code Integer.MIN_VALUE}, the loop never executed, and
        // every DROP_OLD call returned the give-up sentinel
        // regardless of whether eviction could have succeeded.
        return offerOrEvict(event, (long) capacity * 2L);
    }

    /**
     * Test seam. Same contract as the one-arg form but with an
     * explicit upper bound on retry attempts. Calling with
     * {@code maxAttempts == 0} forces an immediate give-up and is
     * the deterministic way for tests to exercise the negative-
     * sentinel return path without constructing pathological
     * timing scenarios.
     *
     * <p>Package-private so production callers don't accidentally
     * starve their producers by passing a too-low bound.
     */
    int offerOrEvict(CaptureEvent event, long maxAttempts) {
        int evictions = 0;
        for (long attempt = 0; attempt < maxAttempts; attempt++) {
            if (offer(event)) return evictions;
            if (evictOldest()) {
                evictions++;
            }
            // If evictOldest returned false (writer is mid-drain, or
            // another evictor took our target slot), loop and retry.
        }
        // Defensive fallthrough: couldn't make room despite many
        // attempts. Event was NOT enqueued. Encode "gave up with N
        // evictions performed" as -(N + 1) so the caller can recover
        // both the failure bit AND the eviction count from one int.
        return -(evictions + 1);
    }

    /**
     * Try to evict the slot at the current {@link #consumerSeq}.
     * Returns {@code true} if we successfully advanced past one
     * pending event; {@code false} if the slot isn't consumer-ready
     * (writer may be mid-drain; the next attempt will pick up the
     * fresher state).
     */
    private boolean evictOldest() {
        long cs = consumerSeq.get();
        int idx = (int) (cs & mask);
        long seq = sequences.get(idx);
        // Slot must be in the "consumer-ready" state (sequence = cs+1)
        // for us to evict it. Otherwise the writer is in the middle
        // of a drain (sequence already at cs+capacity) or the slot
        // simply hasn't been written yet — either way, retry from
        // the freshest state.
        if (seq != cs + 1) return false;
        // CAS-claim the consumer position. If we lose, another evictor
        // (or the writer) beat us to this slot; caller's outer loop
        // will retry against the new consumerSeq.
        if (!consumerSeq.compareAndSet(cs, cs + 1)) return false;
        // We own this slot. Help GC by clearing the event, then mark
        // the slot ready for the next round of producers (whose pos
        // will be cs + capacity).
        slots[idx] = null;
        sequences.set(idx, cs + capacity);
        return true;
    }

    /**
     * Consumer side. Returns the next event in FIFO order, or
     * {@code null} when the ring is empty.
     *
     * <p>Now also lock-free against producer-side eviction: claim
     * via CAS on {@link #consumerSeq}. If the CAS loses, a
     * producer evicted the slot first — return {@code null} and let
     * the caller probe again. Steady-state (no eviction contention)
     * cost: one extra CAS on the writer's hot path vs. the old
     * plain-long version.
     */
    CaptureEvent poll() {
        long pos = consumerSeq.get();
        int idx = (int) (pos & mask);
        long seq = sequences.get(idx);
        if (seq != pos + 1) return null;
        // CAS-claim. Lost CAS → an evicting producer beat us; the
        // event at this position is gone. Return null so the caller
        // retries (which reads the fresh consumerSeq).
        if (!consumerSeq.compareAndSet(pos, pos + 1)) return null;
        CaptureEvent e = slots[idx];
        slots[idx] = null;
        sequences.set(idx, pos + capacity);
        return e;
    }

    /**
     * Approximate count of unwritten events. Producer / consumer can
     * race against each other, so the result may transiently be off
     * by the number of in-flight {@code offer}s; clamped to
     * {@code [0, capacity]}.
     */
    int approximateSize() {
        long p = producerSeq.get();
        long c = consumerSeq.get();
        long size = p - c;
        if (size < 0) return 0;
        if (size > capacity) return capacity;
        return (int) size;
    }

    /**
     * Best-effort age of the oldest pending event, in seconds (per
     * the supplied {@link TimeSource}'s clock). Returns 0 when the
     * ring is empty or when a race causes the sampled slot to be
     * cleared between reads.
     *
     * <p>Used by {@link CaptureLogger#stats()} to populate
     * {@link CaptureLoggerStats#writerLagMs}. Approximate by design
     * — stats is documented as a non-atomic snapshot.
     */
    double oldestPendingAgeSeconds(TimeSource clock) {
        long pos = consumerSeq.get();
        int idx = (int) (pos & mask);
        long seq = sequences.get(idx);
        if (seq != pos + 1) return 0d;       // slot not consumer-ready: ring empty here
        CaptureEvent e = slots[idx];
        if (e == null) return 0d;            // race: drained between reads
        double now = clock.seconds();
        double age = now - e.tSeconds();
        return age > 0 ? age : 0d;            // clamp negative (wallclock NTP jumps)
    }
}
