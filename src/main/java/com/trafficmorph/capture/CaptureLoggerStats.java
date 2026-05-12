package com.trafficmorph.capture;

/**
 * Snapshot of {@link CaptureLogger}'s internal counters at the moment
 * {@link CaptureLogger#stats()} was called. Fields are deliberately
 * a value snapshot (a record) so callers can sample once and reason
 * about the values consistently — repeatedly reading them off the
 * live logger could see different counts for related fields.
 *
 * <p>How the counters partition events depends on the configured
 * {@link OverflowPolicy}:
 * <ul>
 *   <li><b>DROP_NEW</b>: each {@code log()} call lands in exactly
 *       one of {@link #logged}, {@link #dropped}, {@link #invalidDropped}
 *       — strict partition.</li>
 *   <li><b>DROP_OLD</b>: a single {@code log()} call can contribute
 *       to BOTH {@link #logged} (the new event was enqueued) AND
 *       {@link #dropped} (one or more older events were displaced
 *       to make room). So {@code logged + dropped + invalidDropped}
 *       can <em>exceed</em> the total number of {@code log()} calls.
 *       Read {@link #dropped} as "events lost, by any mechanism"
 *       rather than "calls refused" under this policy.</li>
 * </ul>
 *
 * <p>{@link #writeFailed} is independent of the partition: it's
 * bumped by the writer thread when the sink rejects an already-
 * accepted-and-logged event, so an event can be counted in
 * {@code logged} AND {@code writeFailed}. The bound is
 * {@code writeFailed <= logged}.
 *
 * <p><strong>This record is NOT an atomic snapshot across counters:</strong>
 * {@link CaptureLogger#stats()} reads each {@code LongAdder} sum
 * independently, so under live concurrent logging the values can be
 * sampled at slightly different instants. Treat the values as
 * approximate / trend-bearing; if you need a precise instantaneous
 * total, quiesce traffic before sampling.
 *
 * @param logged          total events accepted into the ring buffer
 *                        (queued for writing) since the logger started.
 * @param dropped         total events lost on the producer side
 *                        for any reason: overflow refusals
 *                        (DROP_NEW), post-{@link CaptureLogger#close() close}
 *                        refusals, AND under {@link OverflowPolicy#DROP_OLD}
 *                        the evictions of older events to make
 *                        room for newer ones. A growing value
 *                        means the writer thread can't keep up —
 *                        bump {@code queueCapacity}, switch to a
 *                        faster sink, or downsample at the call
 *                        site.
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
 * @param writerLagMs     Approximate age of the oldest unwritten
 *                        event in milliseconds, sampled via the
 *                        logger's {@link TimeSource}. 0 when the
 *                        ring is empty. Best-effort: a race
 *                        between the stats sample and the writer's
 *                        drain can briefly report 0 even with
 *                        events in flight. Treat as a trend signal
 *                        for back-pressure alerts, not an exact
 *                        per-event measurement. With
 *                        {@link TimeSource#wallclockEpochSeconds()},
 *                        an NTP-driven clock jump backward briefly
 *                        clamps this to 0 rather than reporting a
 *                        negative value.
 */
public record CaptureLoggerStats(
        long logged,
        long dropped,
        long invalidDropped,
        long writeFailed,
        int queueDepth,
        long writerLagMs) {
}
