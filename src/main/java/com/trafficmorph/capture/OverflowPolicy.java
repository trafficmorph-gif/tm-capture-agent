package com.trafficmorph.capture;

/**
 * What the producer does when the writer hasn't drained the buffer
 * fast enough and there's no slot for a new event.
 *
 * <p>Both policies are non-blocking — the producer never waits for
 * the writer thread. The choice is between losing the newest event
 * (preserves the older history; useful when the steady-state cadence
 * is what matters) or losing the oldest event (preserves the most
 * recent state; useful when traffic shape is changing rapidly and
 * the latest events are most informative).
 *
 * <p>Both policies bump the {@code dropped} counter exposed via
 * {@link CaptureLogger#stats()} so the operator can tell when the
 * buffer is undersized.
 */
public enum OverflowPolicy {

    /**
     * If the buffer is full, drop the <em>incoming</em> event. Older
     * events in the buffer continue to drain to the sink as written.
     * Default. Choose this when the historical pattern matters more
     * than the latest tick — e.g. RPS-curve derivation.
     */
    DROP_NEW,

    /**
     * If the buffer is full, evict the oldest queued event to make
     * room for the new one. Choose this when the most recent traffic
     * is most informative — e.g. monitoring an evolving anomaly.
     */
    DROP_OLD
}
