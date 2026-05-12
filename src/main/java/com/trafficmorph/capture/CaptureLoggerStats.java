package com.trafficmorph.capture;

/**
 * Snapshot of {@link CaptureLogger}'s internal counters at the moment
 * {@link CaptureLogger#stats()} was called. Fields are deliberately
 * a value snapshot (a record) so callers can sample once and reason
 * about the values consistently — repeatedly reading them off the
 * live logger could see different counts for related fields.
 *
 * <p>The three count fields ({@link #logged}, {@link #dropped},
 * {@link #invalidDropped}) cover every completed {@code log()} call
 * eventually — each call lands in exactly one of them. <strong>This
 * record is NOT an atomic snapshot across all three counters:</strong>
 * {@link CaptureLogger#stats()} reads each {@code LongAdder} sum
 * independently, so under live concurrent logging the three values
 * can be sampled at slightly different instants and their sum can
 * trail the true total by the number of {@code log()} calls in
 * flight. Treat the values as approximate / trend-bearing rather
 * than as a strict partition algebra.
 *
 * @param logged          total events accepted into the ring buffer
 *                        (queued for writing) since the logger started.
 * @param dropped         total events lost because the logger refused
 *                        to enqueue them — full ring buffer (overflow)
 *                        or post-{@link CaptureLogger#close() close}.
 *                        A non-zero, growing value means the writer
 *                        thread can't keep up — bump
 *                        {@code queueCapacity}, switch to a faster
 *                        sink, or downsample at the call site.
 * @param invalidDropped  total events lost because the caller passed
 *                        an invalid event shape (null or blank
 *                        {@code method} / {@code url}). A non-zero
 *                        value is a CALLER bug, not an operational
 *                        condition — the logger silently swallows
 *                        these to keep the hot-path
 *                        logger-never-throws contract intact, and
 *                        counts them separately so operators can
 *                        tell input bugs apart from buffer pressure.
 * @param queueDepth      current number of unwritten events in the
 *                        ring buffer at the snapshot instant. Bounded
 *                        by {@code queueCapacity}.
 * @param writerLagMs     approximate age of the oldest unwritten
 *                        event in milliseconds at the snapshot
 *                        instant; 0 when the queue is empty. Useful
 *                        for back-pressure alerts.
 */
public record CaptureLoggerStats(
        long logged,
        long dropped,
        long invalidDropped,
        int queueDepth,
        long writerLagMs) {
}
