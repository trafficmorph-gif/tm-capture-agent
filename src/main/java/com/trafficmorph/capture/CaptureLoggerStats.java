package com.trafficmorph.capture;

/**
 * Snapshot of {@link CaptureLogger}'s internal counters at the moment
 * {@link CaptureLogger#stats()} was called. Fields are deliberately
 * a value snapshot (a record) so callers can sample once and reason
 * about the values consistently — repeatedly reading them off the
 * live logger could see different counts for related fields.
 *
 * <p>{@link #logged}, {@link #dropped}, and {@link #invalidDropped}
 * cover every completed {@code log()} call eventually — each call
 * lands in exactly one of those three buckets. {@link #writeFailed}
 * is an <em>independent</em> counter: it's bumped by the writer
 * thread when the sink rejects an already-accepted-and-logged event,
 * so an event can be counted in {@code logged} AND
 * {@code writeFailed}. The bound is {@code writeFailed <= logged}.
 *
 * <p><strong>This record is NOT an atomic snapshot across counters:</strong>
 * {@link CaptureLogger#stats()} reads each {@code LongAdder} sum
 * independently, so under live concurrent logging the values can be
 * sampled at slightly different instants. Treat the values as
 * approximate / trend-bearing rather than as a strict partition
 * algebra; if you need a precise instantaneous total, quiesce
 * traffic before sampling.
 *
 * @param logged          total events accepted into the ring buffer
 *                        (queued for writing) since the logger started.
 * @param dropped         total events the logger refused to enqueue
 *                        — full ring buffer (overflow) or
 *                        post-{@link CaptureLogger#close() close}.
 *                        A growing value means the writer thread
 *                        can't keep up — bump {@code queueCapacity},
 *                        switch to a faster sink, or downsample at
 *                        the call site.
 * @param invalidDropped  total events lost because the caller passed
 *                        an invalid event shape (null or blank
 *                        {@code method} / {@code url}). A non-zero
 *                        value is a CALLER bug, not an operational
 *                        condition.
 * @param writeFailed     total events the sink failed to write
 *                        (raised {@code IOException}). These are
 *                        events that WERE accepted (counted in
 *                        {@code logged}) but couldn't be persisted —
 *                        typically transient disk-full, broken pipe
 *                        on a network sink, etc. Independent of the
 *                        producer-side partition; bound is
 *                        {@code writeFailed <= logged}.
 * @param queueDepth      current number of unwritten events in the
 *                        ring buffer at the snapshot instant.
 * @param writerLagMs     RESERVED — intended to expose the
 *                        approximate age of the oldest unwritten
 *                        event in milliseconds, for back-pressure
 *                        alerting. Currently always {@code 0L}; the
 *                        actual age tracking lands in Step 6 along
 *                        with the rest of the stats polish. Until
 *                        then, use {@link #queueDepth} as a proxy
 *                        for back-pressure and
 *                        {@link CaptureLogger#shutdownTimedOut()}
 *                        as the authoritative unclean-shutdown
 *                        signal. The field is included in the
 *                        record now so the contract doesn't change
 *                        shape when the implementation arrives.
 */
public record CaptureLoggerStats(
        long logged,
        long dropped,
        long invalidDropped,
        long writeFailed,
        int queueDepth,
        long writerLagMs) {
}
