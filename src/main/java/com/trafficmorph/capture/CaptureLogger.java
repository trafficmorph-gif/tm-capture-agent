package com.trafficmorph.capture;

import com.trafficmorph.capture.sink.EventSink;
import com.trafficmorph.capture.sink.StdoutSink;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/**
 * Non-blocking, low-overhead emitter of TrafficMorph-compatible JSONL
 * traffic captures. Designed to be called from hot paths (request
 * handlers, RTB bid pipelines, etc.) where every microsecond counts.
 *
 * <h2>Hot-path contract</h2>
 * <ul>
 *   <li>{@link #log(String, String)} performs no I/O, takes no locks,
 *       and returns in {@code O(100ns)} on warm CPUs. Bounded allocation
 *       per call (one short-lived {@link CaptureEvent} record).</li>
 *   <li>When the internal ring buffer is full the call returns
 *       immediately — the event is dropped and counted in
 *       {@link #stats()}. <strong>The producer thread is never blocked
 *       on disk I/O.</strong></li>
 *   <li>JSON formatting and the actual write happen on a single
 *       daemon writer thread off the hot path.</li>
 * </ul>
 *
 * <h2>Pipeline</h2>
 * <pre>
 *   producer threads ──► offer() ──► MPSC ring buffer ──► writer thread
 *                          │                                   │
 *                          └─ false → dropped++                ├─ format → JSONL line
 *                                                              └─ EventSink.write(line)
 * </pre>
 *
 * <h2>Lifecycle</h2>
 * <p>Construct once per process via {@link #builder()}; close on
 * shutdown to flush the buffer and join the writer thread. Calling
 * {@code log()} after {@link #close()} silently drops the event —
 * the counter it lands in depends on the input:
 * <ul>
 *   <li>If the event is well-formed (non-null, non-blank method and
 *       url), it's counted in {@code dropped} alongside overflow
 *       drops.</li>
 *   <li>If the event is malformed (null or blank method / url), it's
 *       counted in {@code invalidDropped}. Validation runs BEFORE
 *       the closed check so caller-side contract violations remain
 *       diagnosable through shutdown.</li>
 * </ul>
 */
public final class CaptureLogger implements AutoCloseable {

    private final int queueCapacity;
    private final OverflowPolicy overflowPolicy;
    private final EventSink sink;
    private final RedactionPolicy headerRedaction;
    private final int maxBodyLength;
    private final EventRingBuffer ring;
    private final Thread writerThread;
    private final long startNanos = System.nanoTime();

    /**
     * Periodic flush cadence (events). Writer calls {@code sink.flush()}
     * every N writes to bound the loss window if the JVM crashes between
     * an event landing in the OS buffer and reaching the disk. Trade-off:
     * smaller N = more durable, more syscalls. 256 is a defensible
     * default — about 0.5 ms of work at 500k events/s.
     */
    private static final int FLUSH_EVERY = 256;

    /**
     * When the ring is empty the writer parks briefly instead of
     * spinning. 200µs is small enough that bursts drain promptly,
     * large enough that idle CPU usage stays in the noise.
     */
    private static final long IDLE_PARK_NANOS = 200_000L;

    // LongAdder rather than AtomicLong / volatile long: this logger is
    // designed for multi-threaded hot paths where every producer might
    // be on a different core. AtomicLong's single cache line would hot-
    // contend; volatile long ++ would silently lose increments
    // (read-modify-write is not atomic just because the read is
    // volatile). LongAdder shards the counter across cells so writers
    // rarely collide; the small extra cost on .sum() at read-time only
    // affects the (rare) stats() call, never the hot path.
    private final LongAdder logged = new LongAdder();
    private final LongAdder dropped = new LongAdder();
    private final LongAdder invalidDropped = new LongAdder();
    private final LongAdder writeFailed = new LongAdder();

    /**
     * Counts producers that have entered {@code log()} but not yet
     * returned. Used as a barrier by both {@link #close()} (waits
     * until 0 before unparking the writer) AND by the writer's
     * exit check (refuses to exit while any producer is in flight,
     * even if {@code closed} is set and the ring looks empty).
     *
     * <p>Must be {@link AtomicInteger}, NOT {@link LongAdder}.
     * {@code LongAdder.sum()} is not linearizable — it can briefly
     * read 0 while an increment is in flight on another core, which
     * would let the writer exit prematurely and lose the
     * not-yet-published event. {@code AtomicInteger.get()} is a
     * fully linearizable volatile read.
     *
     * <p>Two atomic ops on the hot path (~3-10ns each). Higher
     * contention cost than LongAdder but unavoidable for correct
     * shutdown coordination — and well inside the 200ns hot-path
     * budget.
     */
    private final AtomicInteger inFlight = new AtomicInteger();

    private volatile boolean closed;

    private CaptureLogger(Builder b) {
        this.queueCapacity = b.queueCapacity;
        this.overflowPolicy = b.overflowPolicy;
        this.sink = b.sink;
        this.headerRedaction = b.headerRedaction;
        this.maxBodyLength = b.maxBodyLength;
        this.ring = new EventRingBuffer(queueCapacity);
        this.writerThread = new Thread(this::writerLoop, "tm-capture-writer");
        this.writerThread.setDaemon(true);
        this.writerThread.start();
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Log a request event by method + URL alone. Cheapest overload —
     * use this on extremely hot paths where headers / body aren't
     * needed for replay (or aren't safe to capture verbatim).
     *
     * @param method HTTP method (e.g. {@code "POST"}). Non-null,
     *               non-blank.
     * @param url    full request URL including scheme + host. Non-null,
     *               non-blank.
     */
    public void log(String method, String url) {
        log(method, url, null, null);
    }

    /**
     * Log a request event with optional headers and body. Headers may
     * be {@code null} (omitted from output); body may be {@code null}
     * (omitted from output).
     *
     * @param method  HTTP method.
     * @param url     full request URL.
     * @param headers header name → value map; may be {@code null}.
     * @param body    raw request body; may be {@code null}.
     */
    public void log(String method, String url, Map<String, String> headers, String body) {
        // Increment in-flight FIRST. close() reads this counter as a
        // strict barrier — and so does the writer's exit check (see
        // writerLoop). The decrement lives in finally so any
        // unexpected exception still leaves the counter honest.
        inFlight.incrementAndGet();
        try {
            // The OUTER try/catch protects the hot-path "never
            // throws" contract: caller-side bugs (mutating the
            // headers map mid-copy, etc.) or surprise runtime
            // failures during snapshot / offer route to a counter,
            // not up the request handler's stack. We charge the
            // event to invalidDropped because the failure
            // categorisation is "caller broke the contract"
            // (concurrent map mutation IS a caller bug).
            try {
                // Validate BEFORE the closed check so caller-side
                // contract violations stay diagnosable even after
                // shutdown.
                if (method == null || url == null || method.isBlank() || url.isBlank()) {
                    invalidDropped.increment();
                    return;
                }
                if (closed) {
                    dropped.increment();
                    return;
                }
                double t = (System.nanoTime() - startNanos) / 1_000_000_000d;
                // Defensive snapshot of the caller's headers map,
                // with redaction applied inline. Caller may mutate /
                // recycle their map at any time after we return; we
                // hand the writer a stable, already-redacted copy.
                //
                // LinkedHashMap preserves the caller's iteration
                // order so header round-tripping stays deterministic.
                //
                // The iteration itself can throw if the caller is
                // mutating the map concurrently with this call
                // (CME from the source iterator, IOOB from a racing
                // resize, ...) — the outer catch turns those into
                // invalidDropped rather than letting them propagate
                // up the request thread.
                Map<String, String> headersSnapshot = null;
                if (headers != null && !headers.isEmpty()) {
                    headersSnapshot = new LinkedHashMap<>(headers.size());
                    for (Map.Entry<String, String> entry : headers.entrySet()) {
                        String hName = entry.getKey();
                        // Null-key entries are legal in HashMap-style
                        // maps but meaningless in an HTTP context AND
                        // would violate the RedactionPolicy contract
                        // ({@code headerName} is documented non-null).
                        // Skip them silently here so policies and the
                        // formatter can both assume non-null names.
                        if (hName == null) continue;
                        String hVal = entry.getValue();
                        String redacted = headerRedaction.redact(hName, hVal);
                        // Identity compare: only the DROP sentinel
                        // skips the entry. Any other return value
                        // (including null, which preserves a
                        // null-valued header) is stored as-is.
                        if (redacted != RedactionPolicy.DROP) {
                            headersSnapshot.put(hName, redacted);
                        }
                    }
                    if (headersSnapshot.isEmpty()) {
                        // Policy dropped every header (or every
                        // entry had a null key) — emit no headers
                        // field at all rather than an empty object,
                        // matching the no-headers shape.
                        headersSnapshot = null;
                    }
                }
                // Body size cap. Truncating on the producer side
                // bounds the in-memory footprint of queued events
                // (a huge body would otherwise pin a ring slot
                // until the writer formatted it). Common case
                // (under cap): no allocation, single length probe.
                //
                // The maxBodyLength==0 case is checked FIRST so an
                // empty body still gets dropped — the contract says
                // "0 disables body capture entirely", and a length-
                // greater-than check would let body="" sneak through
                // (length 0 is not > 0).
                String boundedBody = body;
                if (body != null) {
                    if (maxBodyLength == 0) {
                        boundedBody = null;          // bodies disabled
                    } else if (body.length() > maxBodyLength) {
                        int truncatedChars = body.length() - maxBodyLength;
                        boundedBody = body.substring(0, maxBodyLength)
                                + "...[truncated " + truncatedChars + " chars]";
                    }
                }
                CaptureEvent event = new CaptureEvent(t, method, url, headersSnapshot, boundedBody);
                if (!ring.offer(event)) {
                    // Ring full. Only DROP_NEW is supported today
                    // (Step 6 adds DROP_OLD); the Builder rejects
                    // DROP_OLD up front so we don't have to branch.
                    dropped.increment();
                    return;
                }
                logged.increment();
            } catch (RuntimeException re) {
                // Hot-path never throws on caller bugs. Concurrent
                // map mutation during the snapshot (CME, ISE),
                // unexpected IAE on event construction, etc. — the
                // request handler keeps going; this event is
                // charged to invalidDropped.
                //
                // We deliberately do NOT catch Error: OOM,
                // StackOverflowError, LinkageError, etc. signal
                // that the JVM is in a corrupted state. Swallowing
                // them here would let the process keep running on
                // a foundation that's already on fire AND hide the
                // root cause behind "look, invalidDropped went up".
                // Let them propagate up so the request thread's
                // own uncaught handler / health probe surfaces them.
                invalidDropped.increment();
            }
        } finally {
            inFlight.decrementAndGet();
        }
    }

    /**
     * Snapshot of producer-side counters. Cheap; safe from any thread.
     *
     * <p>{@link CaptureLoggerStats#dropped()} counts events lost
     * because the logger refused to enqueue them (overflow, or
     * post-close). {@link CaptureLoggerStats#invalidDropped()}
     * counts events lost because the caller passed an invalid
     * shape (null / blank method or url).
     *
     * <p>The three counters partition every completed {@code log()}
     * call eventually, BUT this method does NOT take an atomic
     * snapshot — each {@code LongAdder.sum()} is read independently,
     * so under live concurrent logging the three values can be
     * sampled at slightly different instants. Operators treating
     * the totals as approximate (looking at trends / ratios) are
     * fine; callers that need {@code logged + dropped + invalidDropped}
     * to add up to a specific total instant should quiesce traffic
     * before sampling, or build their own atomic snapshot above
     * this API.
     */
    public CaptureLoggerStats stats() {
        return new CaptureLoggerStats(
                logged.sum(),
                dropped.sum(),
                invalidDropped.sum(),
                writeFailed.sum(),
                ring.approximateSize(),
                /* writerLagMs */ 0L);
    }

    /**
     * Total budget for {@link #close()} from start to return,
     * splitting between waiting on in-flight producers and joining
     * the writer thread. After this many nanoseconds {@code close()}
     * returns regardless — the writer thread is a daemon, so the
     * JVM will tear it down at exit, and we'd rather have a slightly
     * unclean shutdown than block process exit indefinitely.
     *
     * <p>5 seconds is comfortably longer than any reasonable
     * combination of in-flight producer (microseconds) + remaining
     * ring drain (milliseconds even for pathologically slow sinks),
     * but short enough to keep CI / container shutdown snappy.
     */
    private static final long SHUTDOWN_BUDGET_NANOS = 5_000_000_000L;

    private volatile boolean shutdownTimedOut;

    /**
     * Stop accepting new events, drain the ring buffer, flush the
     * sink, join the writer thread. Subsequent {@code log()} calls
     * count toward {@code dropped} (or {@code invalidDropped}).
     *
     * <p>Idempotent — a second call is a no-op.
     *
     * <h2>Bounded shutdown</h2>
     * <p>This method is bounded by {@link #SHUTDOWN_BUDGET_NANOS}
     * (5 seconds). The two waits share that budget:
     * <ol>
     *   <li>Wait for in-flight producers to finish {@code log()}
     *       (microseconds normally).</li>
     *   <li>Join the writer thread (milliseconds even on a slow
     *       sink, once the queue has drained).</li>
     * </ol>
     * If the budget is exhausted — typically because a producer is
     * stuck (debugger pause, deadlock with caller code, GC pause)
     * or the sink's write is genuinely hanging — {@code close()}
     * returns and {@link #shutdownTimedOut()} flips to {@code true}.
     * The writer thread is a daemon so the JVM will reap it; any
     * events still queued at that point are lost.
     *
     * <p>(Note: {@link CaptureLoggerStats#writerLagMs()} is reserved
     * for the actual "age of oldest queued event" metric, which is
     * not yet computed — currently always 0. Step 6 wires that up;
     * for now {@link #shutdownTimedOut()} is the authoritative
     * unclean-shutdown signal.)
     *
     * <h2>Race-free correctness when the budget isn't exhausted</h2>
     * <p>Flip {@code closed}, spin-wait (with brief parks) until
     * {@link AtomicInteger#get() inFlight.get()} reads zero, THEN
     * signal the writer. Any producer currently inside {@code log()}
     * is observed via {@code inFlight} and given a chance to finish
     * (either enqueuing — its event will be drained — or taking the
     * dropped branch on its volatile read of {@code closed}).
     * Producers that START after {@code closed=true} read the flag
     * via volatile acquire and go straight to {@code dropped}
     * without enqueuing.
     */
    @Override
    public void close() {
        if (closed) return;
        closed = true;
        final long start = System.nanoTime();
        final long deadline = start + SHUTDOWN_BUDGET_NANOS;
        // Phase 1: wait for in-flight producers. Microseconds normally.
        while (inFlight.get() > 0 && System.nanoTime() < deadline) {
            LockSupport.parkNanos(50_000L);  // 50µs
        }
        // Wake the writer (parked if idle); it sees closed=true and
        // — once inFlight is zero too — drains and exits.
        LockSupport.unpark(writerThread);
        // Phase 2: join the writer with whatever budget remains.
        // join(0) means "wait forever", so we clamp to at least 1 ms
        // — if we're already over the deadline, just probe whether
        // the writer is alive.
        long remainingMs = Math.max(0L, (deadline - System.nanoTime()) / 1_000_000L);
        try {
            if (remainingMs > 0) {
                writerThread.join(remainingMs);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        if (writerThread.isAlive()) {
            // Budget exhausted with the writer still running. Flag
            // it via stats so callers can detect unclean shutdown,
            // then return — daemon thread won't block JVM exit.
            shutdownTimedOut = true;
        }
    }

    /**
     * Whether the last {@link #close()} exhausted its time budget
     * before the writer thread joined. Useful for shutdown-health
     * probes — a {@code true} value indicates either a stuck
     * producer or a hanging sink, both worth investigating.
     */
    public boolean shutdownTimedOut() {
        return shutdownTimedOut;
    }

    // ── Writer thread ────────────────────────────────────────────────

    /**
     * Single writer thread's drain loop. Polls the ring, formats each
     * event, hands it to the sink. Flushes periodically (every
     * {@link #FLUSH_EVERY} writes) and on shutdown. Parks briefly
     * when the ring is empty so an idle logger doesn't burn a CPU.
     *
     * <p>Robustness: per-event sink failures ({@code IOException}
     * from {@code sink.write()}, or a {@code RuntimeException} from
     * a misbehaving custom sink / formatter) are caught and counted
     * via {@code writeFailed} — separate from {@code dropped}, which
     * is reserved for producer-side enqueue refusals. One bad write
     * shouldn't poison the whole pipeline. {@code Error} subclasses
     * (OOM, LinkageError, ...) deliberately propagate up to the
     * thread's uncaught-exception handler so JVM-level corruption
     * is surfaced rather than hidden behind a counter.
     */
    private void writerLoop() {
        JsonlFormatter formatter = new JsonlFormatter();
        int sinceFlush = 0;
        while (true) {
            CaptureEvent e = ring.poll();
            if (e == null) {
                // Exit only when ALL of these hold:
                //   1. closed == true        (no new producers will enqueue past their closed-check)
                //   2. inFlight == 0         (no producer is currently inside log())
                //   3. ring re-poll == null  (no event published between our prev poll and the inFlight check)
                //
                // Without check (2), the writer could exit while a
                // producer was ghosting between its inFlight++ and
                // its ring.offer() — its event would land in the
                // ring after the writer was gone, never written but
                // still counted as logged. close()'s own wait for
                // inFlight==0 isn't sufficient because this loop
                // runs concurrently with close() and could observe
                // (closed && empty) on its own schedule.
                if (closed && inFlight.get() == 0) {
                    e = ring.poll();
                    if (e == null) {
                        safelyFlush();
                        safelyClose();
                        return;
                    }
                    // Fall through to write the event; loop again to
                    // drain any further publications.
                } else {
                    if (sinceFlush > 0) {
                        safelyFlush();
                        sinceFlush = 0;
                    }
                    // Park briefly when truly idle. Producers don't
                    // wake us — paying that CAS on every offer would
                    // dwarf the savings vs. this micro-park. The
                    // shutdown path unparks us explicitly.
                    LockSupport.parkNanos(this, IDLE_PARK_NANOS);
                    continue;
                }
            }
            try {
                sink.write(formatter.format(e));
            } catch (IOException ioe) {
                // Sink rejected this line (transient disk full, broken
                // network pipe, etc.). Count separately from `dropped`
                // since this event WAS accepted into the ring and
                // counted as logged — it's a sink-side loss, not an
                // enqueue refusal. Continue: the next line may succeed.
                writeFailed.increment();
            } catch (RuntimeException re) {
                // Formatter bug or sink misbehaving (e.g. an
                // IllegalArgumentException from a custom sink that
                // didn't bother declaring IOException). Drain
                // continues so one bad event can't stop the pipeline.
                //
                // As on the producer side: Error subclasses
                // (OOM, LinkageError, ...) are deliberately NOT
                // caught — let the JVM's uncaught-exception handler
                // see them so operators know the process state is
                // actually broken.
                writeFailed.increment();
            }
            if (++sinceFlush >= FLUSH_EVERY) {
                safelyFlush();
                sinceFlush = 0;
            }
        }
    }

    private void safelyFlush() {
        try {
            sink.flush();
        } catch (IOException ignored) {
            // A flush failure means the OS-level buffer didn't make it
            // to disk; the events themselves are already accepted into
            // the sink. Nothing actionable here — operator alerts off
            // OS-level disk metrics, not from us.
        }
    }

    private void safelyClose() {
        try {
            sink.close();
        } catch (IOException ignored) {
            // Same reasoning as flush — closing failures during JVM
            // shutdown shouldn't disrupt the rest of shutdown.
        }
    }

    /**
     * Fluent builder for {@link CaptureLogger}. All knobs have
     * sensible defaults so {@code CaptureLogger.builder().build()}
     * yields a usable instance writing to {@code System.out}.
     */
    public static final class Builder {
        private int queueCapacity = 65_536;
        private OverflowPolicy overflowPolicy = OverflowPolicy.DROP_NEW;
        private EventSink sink;
        private RedactionPolicy headerRedaction = RedactionPolicy.defaultSafelist();
        private int maxBodyLength = 16_384;

        private Builder() {}

        /**
         * Bounded ring-buffer capacity (in events, not bytes). Must
         * be a positive power of two — the ring uses mask-based
         * indexing. Defaults to {@code 65 536}.
         */
        public Builder queueCapacity(int capacity) {
            if (capacity <= 0 || Integer.bitCount(capacity) != 1) {
                throw new IllegalArgumentException(
                        "queueCapacity must be a positive power of two, was " + capacity);
            }
            this.queueCapacity = capacity;
            return this;
        }

        /**
         * What to do when the ring buffer is full. Defaults to
         * {@link OverflowPolicy#DROP_NEW}.
         *
         * <p>{@link OverflowPolicy#DROP_OLD} is declared but NOT yet
         * implemented — the MPSC ring's single-consumer invariant
         * makes producer-side eviction non-trivial, and rather than
         * silently treat it as DROP_NEW we reject it here. Step 6
         * adds real DROP_OLD behaviour; until then,
         * {@code overflowPolicy(DROP_OLD)} throws so misconfigured
         * deployments fail loudly at startup rather than silently
         * getting the wrong policy.
         */
        public Builder overflowPolicy(OverflowPolicy policy) {
            Objects.requireNonNull(policy, "policy");
            if (policy == OverflowPolicy.DROP_OLD) {
                throw new IllegalArgumentException(
                        "DROP_OLD is not yet implemented — the MPSC ring's "
                                + "single-consumer invariant requires more careful "
                                + "eviction logic, scheduled for Step 6. Use DROP_NEW "
                                + "or wait for the upgrade.");
            }
            this.overflowPolicy = policy;
            return this;
        }

        /**
         * Where formatted JSONL lines are written. Defaults to
         * {@link StdoutSink} — convenient for development and for
         * containerised deployments where a log aggregator harvests
         * stdout. Use a custom {@link EventSink} for file output
         * (Step 5 adds a turnkey rolling file sink), network
         * destinations, or test capture.
         */
        public Builder sink(EventSink sink) {
            this.sink = Objects.requireNonNull(sink, "sink");
            return this;
        }

        /**
         * Per-header value transform applied during the snapshot
         * pass in {@link #log}. Defaults to
         * {@link RedactionPolicy#defaultSafelist()} so common
         * credential-carrying headers (Authorization, Cookie, …)
         * are redacted without ceremony. Pass
         * {@link RedactionPolicy#none()} for an internal-network
         * capture where redaction overhead isn't wanted.
         */
        public Builder headerRedaction(RedactionPolicy policy) {
            this.headerRedaction = Objects.requireNonNull(policy, "policy");
            return this;
        }

        /**
         * Maximum body length (in {@code char}s, not bytes) retained
         * by {@link #log}. Bodies longer than this are truncated to
         * the first {@code maxBodyLength} chars with a {@code "...
         * [truncated N chars]"} marker appended; the truncation
         * happens on the producer thread so the ring slot's memory
         * footprint is bounded even when a caller passes a 10 MB
         * payload.
         *
         * <p>Default is 16384 chars — fits typical JSON bodies, RTB
         * bid requests, and similar payloads with margin. Pass
         * {@code 0} to drop bodies entirely (kept as {@code null}
         * in the captured event). Negative values are rejected at
         * build time so a typo can't disable capture silently.
         */
        public Builder maxBodyLength(int chars) {
            if (chars < 0) {
                throw new IllegalArgumentException(
                        "maxBodyLength must be >= 0, was " + chars);
            }
            this.maxBodyLength = chars;
            return this;
        }

        public CaptureLogger build() {
            if (sink == null) sink = new StdoutSink();
            return new CaptureLogger(this);
        }
    }
}
