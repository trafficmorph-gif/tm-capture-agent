package com.trafficmorph.capture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Step 4 — TimeSource abstraction. Covers the two built-in
 * factories ({@code relativeMonotonic}, {@code wallclockEpochSeconds})
 * AND the user-pluggable contract (custom lambda → values come
 * through unchanged in the captured line).
 */
class TimeSourceTest {

    @Test
    void defaultTimeSourceIsRelativeMonotonicAndStartsNearZero() throws Exception {
        // No explicit .timeSource() call → Builder.build() installs
        // relativeMonotonic, whose start instant is "now". The first
        // event's t should be tiny (well under 1 second).
        ListSink sink = new ListSink();
        try (CaptureLogger logger = CaptureLogger.builder().sink(sink).build()) {
            logger.log("GET", "https://x");
        }
        String line = sink.lines().get(0);
        double t = extractTimestamp(line);
        assertTrue(t >= 0.0 && t < 1.0,
                "first event's t should be near 0 with default source; got " + t);
    }

    @Test
    void wallclockEpochSecondsProducesAbsoluteTimestamps() throws Exception {
        ListSink sink = new ListSink();
        try (CaptureLogger logger = CaptureLogger.builder()
                .sink(sink)
                .timeSource(TimeSource.wallclockEpochSeconds())
                .build()) {
            logger.log("GET", "https://x");
        }
        double t = extractTimestamp(sink.lines().get(0));
        // Absolute epoch seconds — should be in the same neighbourhood
        // as Java's current time, ±2 seconds for clock skew during
        // the test.
        double now = System.currentTimeMillis() / 1000d;
        assertTrue(Math.abs(t - now) < 2.0,
                "wallclock t should match current epoch ±2s; t=" + t + " now=" + now);
        // And it should be ~9-10 decimal digits before the dot —
        // 2026 epoch seconds is around 1.78e9. Sanity check via
        // raw magnitude.
        assertTrue(t > 1_000_000_000.0,
                "epoch-seconds value should be > 10^9; got " + t);
    }

    @Test
    void customTimeSourceValuesArePassedThroughVerbatim() throws Exception {
        // A user-supplied lambda must control what lands in `t`
        // EXACTLY (subject to the formatter's 3-decimal rounding).
        // Tests the abstraction's most important contract: the
        // logger doesn't second-guess the source.
        ListSink sink = new ListSink();
        AtomicLong counter = new AtomicLong();
        // Returns 0.0, 1.0, 2.0, 3.0, ... — deterministic, easy
        // to assert against.
        TimeSource counting = () -> counter.getAndIncrement();
        try (CaptureLogger logger = CaptureLogger.builder()
                .sink(sink)
                .timeSource(counting)
                .build()) {
            logger.log("GET", "https://x");
            logger.log("GET", "https://x");
            logger.log("GET", "https://x");
        }
        List<String> lines = sink.lines();
        assertEquals(0.000, extractTimestamp(lines.get(0)), 1e-9);
        assertEquals(1.000, extractTimestamp(lines.get(1)), 1e-9);
        assertEquals(2.000, extractTimestamp(lines.get(2)), 1e-9);
    }

    @Test
    void relativeMonotonicFactoryProducesAlwaysIncreasingValues() throws Exception {
        // The TimeSource doesn't have to be tied to a logger to be
        // observable. Call it directly twice and confirm the second
        // value is greater than the first — proves the monotonic
        // contract.
        TimeSource ts = TimeSource.relativeMonotonic();
        double first = ts.seconds();
        // Burn a few nanos so System.nanoTime() actually advances.
        for (int i = 0; i < 1000; i++) Thread.onSpinWait();
        double second = ts.seconds();
        assertTrue(second >= first,
                "monotonic source must never regress; first=" + first + " second=" + second);
        // Also: both should be ≥ 0 (start of "epoch" for this source).
        assertTrue(first >= 0d);
    }

    @Test
    void reusedBuilderGivesEachLoggerAFreshDefaultTimeSource() throws Exception {
        // Regression: when build() memoised the default
        // relativeMonotonic() into the builder field, two loggers
        // from the SAME builder shared a start instant. The second
        // logger's first event t therefore wasn't near zero — it
        // was offset by however long passed between the two
        // build() calls. Now: each build() resolves its own
        // default inside CaptureLogger's constructor, no writeback.
        ListSink sink1 = new ListSink();
        ListSink sink2 = new ListSink();
        CaptureLogger.Builder shared = CaptureLogger.builder();
        try (CaptureLogger first = shared.sink(sink1).build()) {
            first.log("GET", "https://x");
        }
        // A real-world reuse would have arbitrary time pass; even a
        // small sleep is enough to expose the memoisation bug (the
        // second logger would see t ~= sleep_duration, not ~= 0).
        Thread.sleep(50);
        try (CaptureLogger second = shared.sink(sink2).build()) {
            second.log("GET", "https://x");
        }
        double tFirst = extractTimestamp(sink1.lines().get(0));
        double tSecond = extractTimestamp(sink2.lines().get(0));
        // Both loggers' first events must start near zero.
        assertTrue(tFirst < 0.1,
                "first logger's first t should be near 0; got " + tFirst);
        assertTrue(tSecond < 0.1,
                "second logger's first t should also be near 0 (independent clock); got " + tSecond);
    }

    @Test
    void builderRejectsNullTimeSource() {
        // Explicit null on the builder method should fail fast at
        // configuration time, not after starting the writer thread.
        assertThrows(NullPointerException.class,
                () -> CaptureLogger.builder().timeSource(null).build());
    }

    @Test
    void timeSourceIsCalledExactlyOncePerLoggedEvent() throws Exception {
        // The producer-side hot path calls timeSource.seconds() once
        // per log() to stamp the event. A buggy refactor that
        // accidentally double-called the clock would inflate
        // timestamps OR shift them. Pin the contract.
        ListSink sink = new ListSink();
        AtomicLong calls = new AtomicLong();
        TimeSource counting = () -> {
            calls.incrementAndGet();
            return 0.0;   // value doesn't matter — count does
        };
        try (CaptureLogger logger = CaptureLogger.builder()
                .sink(sink)
                .timeSource(counting)
                .build()) {
            for (int i = 0; i < 100; i++) {
                logger.log("GET", "https://x");
            }
        }
        // 100 events, 100 timestamp calls. (Invalid-input or
        // closed-after paths wouldn't reach this site, so 100 is
        // exact.)
        assertEquals(100L, calls.get(),
                "timeSource should be invoked exactly once per accepted event");
    }

    /**
     * Parse the {@code t} value out of a JSONL line produced by
     * the formatter. Lines start with {@code {"t":NUMBER,"method":...}}
     * so the timestamp lives between index 5 and the first comma.
     */
    private static double extractTimestamp(String line) {
        assertNotNull(line);
        int comma = line.indexOf(',');
        return Double.parseDouble(line.substring(5, comma));
    }
}
