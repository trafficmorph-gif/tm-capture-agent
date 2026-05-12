package com.trafficmorph.capture;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

/**
 * Non-blocking, low-overhead emitter of TrafficMorph-compatible JSONL
 * traffic captures. Designed to be called from hot paths (request
 * handlers, RTB bid pipelines, etc.) where every microsecond counts.
 *
 * <h2>Hot-path contract</h2>
 * <ul>
 *   <li>{@link #log(String, String)} performs no I/O, takes no locks
 *       beyond a single lock-free enqueue, and returns in {@code O(100ns)}
 *       on warm CPUs. Bounded allocation per call (one short-lived
 *       event record).</li>
 *   <li>When the internal ring buffer is full the call returns
 *       immediately — the event is dropped per the configured
 *       {@link OverflowPolicy} and counted in {@link #stats()}.
 *       <strong>The producer thread is never blocked on disk I/O.</strong></li>
 *   <li>JSON formatting and the actual write happen on a single
 *       daemon writer thread off the hot path.</li>
 * </ul>
 *
 * <h2>Skeleton (Step 1)</h2>
 * <p>This is the public-surface scaffold. The ring buffer + writer
 * thread + sink are wired in Step 2; today {@code log()} is a counted
 * no-op so the API can be reviewed and exercised independently of the
 * pipeline implementation.
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
    private volatile boolean closed;

    private CaptureLogger(Builder b) {
        this.queueCapacity = b.queueCapacity;
        this.overflowPolicy = b.overflowPolicy;
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
     *               non-blank. Uppercased by downstream parsing.
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
     * <p>Important: the {@code body} parameter is the request payload
     * <em>as a string</em>. For servlets, capturing it requires
     * wrapping the request input stream — see
     * {@code CaptureServletFilter} (Step 7) for a turnkey integration.
     *
     * @param method  HTTP method.
     * @param url     full request URL.
     * @param headers header name → value map; may be {@code null}.
     * @param body    raw request body; may be {@code null}.
     */
    public void log(String method, String url, Map<String, String> headers, String body) {
        // Validate BEFORE the closed check so caller-side contract
        // violations stay diagnosable even after shutdown. The
        // logger-never-throws contract is preserved: blank inputs
        // are silently counted as invalidDropped rather than
        // raising IllegalArgumentException. Throwing on a hot path
        // would be expensive (stack-trace synthesis) AND could
        // disable the caller's request handler — a far worse
        // outcome than losing one captured row.
        if (method == null || url == null || method.isBlank() || url.isBlank()) {
            invalidDropped.increment();
            return;
        }
        if (closed) {
            dropped.increment();
            return;
        }
        // Step 1 scaffold: count + return. Real enqueue (which will
        // actually consume `headers` and `body`) lands in Step 2.
        logged.increment();
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
                /* queueDepth */ 0,
                /* writerLagMs */ 0L);
    }

    @Override
    public void close() {
        // Step 1 scaffold. Step 2 will drain the queue + join the
        // writer thread + close the sink here.
        closed = true;
    }

    /**
     * Fluent builder for {@link CaptureLogger}. All knobs have
     * sensible defaults so {@code CaptureLogger.builder().build()}
     * yields a usable instance (today: a counted no-op; from Step 2
     * onward: a stdout-writing instance).
     */
    public static final class Builder {
        private int queueCapacity = 65_536;
        private OverflowPolicy overflowPolicy = OverflowPolicy.DROP_NEW;

        private Builder() {}

        /**
         * Bounded ring-buffer capacity (in events, not bytes). Must
         * be a positive power of two — the writer uses a mask-based
         * index. Defaults to {@code 65 536}.
         */
        public Builder queueCapacity(int capacity) {
            if (capacity <= 0 || Integer.bitCount(capacity) != 1) {
                throw new IllegalArgumentException(
                        "queueCapacity must be a positive power of two, was " + capacity);
            }
            this.queueCapacity = capacity;
            return this;
        }

        /** What to do when the ring buffer is full. Defaults to {@link OverflowPolicy#DROP_NEW}. */
        public Builder overflowPolicy(OverflowPolicy policy) {
            this.overflowPolicy = Objects.requireNonNull(policy, "policy");
            return this;
        }

        public CaptureLogger build() {
            return new CaptureLogger(this);
        }
    }
}
