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
 *       is the writer thread, so its side is plain field access.</li>
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
 * The caller (the producer in {@code CaptureLogger.log}) interprets
 * that as overflow and bumps the dropped counter.
 */
final class EventRingBuffer {

    private final CaptureEvent[] slots;
    private final AtomicLongArray sequences;
    private final int mask;
    private final int capacity;

    /** Next position a producer would claim. */
    private final AtomicLong producerSeq = new AtomicLong(0);

    /**
     * Next position the consumer would drain. Only the writer thread
     * reads/writes this — no synchronisation needed. Stays a plain
     * field; the per-slot sequences carry the cross-thread visibility.
     */
    private long consumerSeq = 0;

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
     * Producer side. Returns {@code true} on success, {@code false}
     * if the ring is full (caller drops + counts).
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
     * Consumer side. Returns the next event in FIFO order, or
     * {@code null} when the ring is empty. Single-threaded — must
     * only be called from the writer thread.
     */
    CaptureEvent poll() {
        long pos = consumerSeq;
        int idx = (int) (pos & mask);
        long seq = sequences.get(idx);
        long diff = seq - (pos + 1);
        if (diff == 0) {
            // Producer published at this slot. Read event, clear slot,
            // mark the slot ready for the NEXT round of producers
            // (their pos will be the current pos + capacity).
            CaptureEvent e = slots[idx];
            slots[idx] = null;                          // help GC
            sequences.set(idx, pos + capacity);         // release: slot empty
            consumerSeq = pos + 1;
            return e;
        }
        // diff < 0: producer hasn't published yet → empty for this slot.
        // (diff > 0 can't happen with a single consumer.)
        return null;
    }

    /**
     * Approximate count of unwritten events. Producer / consumer can
     * race against each other, so the result may transiently be off
     * by the number of in-flight {@code offer}s; clamped to
     * {@code [0, capacity]}.
     */
    int approximateSize() {
        long p = producerSeq.get();
        long c = consumerSeq;
        long size = p - c;
        if (size < 0) return 0;
        if (size > capacity) return capacity;
        return (int) size;
    }
}
