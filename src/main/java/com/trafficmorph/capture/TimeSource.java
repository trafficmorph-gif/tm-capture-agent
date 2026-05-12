package com.trafficmorph.capture;

/**
 * Pluggable clock for the {@code t} field emitted on each captured
 * event. Decoupled from the logger so callers can pick the timestamp
 * style that matches their downstream tooling:
 * <ul>
 *   <li>{@link #relativeMonotonic()} — seconds since the logger
 *       started, monotonic, immune to system-clock adjustments.
 *       Default. Matches the bundled sample fixture's shape and
 *       feeds the import-side curve deriver directly.</li>
 *   <li>{@link #wallclockEpochSeconds()} — seconds since Unix
 *       epoch, milli-precise. Useful when captures are merged across
 *       multiple processes / hosts and need cross-correlation, or
 *       when downstream consumers expect absolute time.</li>
 * </ul>
 *
 * <h2>Contract</h2>
 * <ul>
 *   <li>Must be thread-safe — called concurrently from arbitrary
 *       producer threads.</li>
 *   <li>Must NOT throw — anything thrown lands in the producer's
 *       outer catch and the event is counted as {@code invalidDropped}.</li>
 *   <li>Should be cheap (target: tens of nanoseconds). Both built-in
 *       implementations use {@code System.nanoTime} /
 *       {@code System.currentTimeMillis}, which are
 *       intrinsified-or-fast on every mainstream JVM.</li>
 *   <li>Returned values are seconds (a {@code double}). The
 *       formatter renders three decimal places, so sub-millisecond
 *       precision is discarded — implementations don't need to be
 *       more precise than that.</li>
 * </ul>
 *
 * <p>Custom implementations are useful for tests (mock time
 * advances deterministically) and for niche cases like
 * replaying captures from a fixed start (rewrap an existing
 * source with a sentinel offset).
 */
@FunctionalInterface
public interface TimeSource {

    /** Current event timestamp in seconds. */
    double seconds();

    /**
     * Monotonic clock, seconds since the source was constructed.
     * The factory call captures the start instant via
     * {@code System.nanoTime()}, so the very first event timestamp
     * is some small positive value (the time between this call and
     * the first {@code log()}).
     *
     * <p>This is the historical default behaviour. Use it when you
     * intend the resulting capture to be self-contained — the
     * import-side analyser only cares about deltas between events
     * and treats the first {@code t} as the origin, so absolute
     * wallclock alignment isn't needed.
     */
    static TimeSource relativeMonotonic() {
        return new RelativeMonotonic(System.nanoTime());
    }

    /**
     * Wallclock seconds since the Unix epoch (1970-01-01T00:00:00Z),
     * millisecond-precise. Backed by {@link System#currentTimeMillis()},
     * which is thread-safe and around 20ns per call on a modern JVM.
     *
     * <p>Caveat: {@code System.currentTimeMillis()} reflects the
     * system clock and CAN jump backwards / forwards if NTP adjusts
     * the clock during the capture. The import-side analyser still
     * sorts by {@code t}, so a backwards jump shows up as
     * out-of-order events — acceptable for typical captures, fatal
     * for sub-second sequencing under aggressive NTP.
     */
    static TimeSource wallclockEpochSeconds() {
        return WallclockEpochSeconds.INSTANCE;
    }

    /**
     * Monotonic-relative implementation. Package-private — consumers
     * construct via {@link #relativeMonotonic()}.
     */
    final class RelativeMonotonic implements TimeSource {
        private final long startNanos;

        RelativeMonotonic(long startNanos) {
            this.startNanos = startNanos;
        }

        @Override
        public double seconds() {
            return (System.nanoTime() - startNanos) / 1_000_000_000d;
        }
    }

    /**
     * Singleton wallclock implementation. Stateless — no per-source
     * start instant, every call samples the system clock directly.
     */
    final class WallclockEpochSeconds implements TimeSource {
        static final WallclockEpochSeconds INSTANCE = new WallclockEpochSeconds();

        private WallclockEpochSeconds() {}

        @Override
        public double seconds() {
            return System.currentTimeMillis() / 1000d;
        }
    }
}
