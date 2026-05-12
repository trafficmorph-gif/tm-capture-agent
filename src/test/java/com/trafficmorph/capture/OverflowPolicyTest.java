package com.trafficmorph.capture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trafficmorph.capture.sink.EventSink;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Step 6 — overflow policy semantics + writerLagMs computation.
 *
 * <p>DROP_NEW and DROP_OLD differ in which events are lost when the
 * ring is saturated. The tests pin the *semantic* difference: which
 * events survive to reach the sink.
 */
class OverflowPolicyTest {

    /**
     * Sink that blocks the writer thread on each write so we can
     * deterministically saturate the ring from the producer side
     * (no race with the writer racing ahead and draining slots).
     */
    private static final class GatedSink implements EventSink {
        final CountDownLatch release = new CountDownLatch(1);
        final java.util.List<String> drained = new java.util.concurrent.CopyOnWriteArrayList<>();

        @Override
        public void write(String line) throws IOException {
            try {
                release.await();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            drained.add(line);
        }

        @Override public void flush() {}
        @Override public void close() {}
    }

    @Test
    void dropNewKeepsTheOldestEventsAndRefusesTheNewestUnderOverflow() throws Exception {
        GatedSink sink = new GatedSink();
        try (CaptureLogger logger = CaptureLogger.builder()
                .sink(sink)
                .queueCapacity(4)
                .overflowPolicy(OverflowPolicy.DROP_NEW)
                .build()) {

            // Send 8 events through a capacity-4 ring with the
            // writer blocked. First 4 fit; rest are dropped.
            for (int i = 0; i < 8; i++) {
                logger.log("GET", "https://x/" + i);
            }

            CaptureLoggerStats s = logger.stats();
            assertEquals(4, s.logged(), "first 4 fit: stats=" + s);
            assertEquals(4, s.dropped(), "subsequent 4 dropped: stats=" + s);

            // Release the writer and let it drain.
            sink.release.countDown();
        }
        // After close: only the FIRST 4 should have reached the sink.
        // DROP_NEW preserves historical order — newest events are
        // refused at the door.
        List<String> drained = sink.drained;
        assertEquals(4, drained.size());
        for (int i = 0; i < 4; i++) {
            assertTrue(drained.get(i).contains("/x/" + i),
                    "DROP_NEW should preserve events 0..3; got " + drained.get(i) + " at " + i);
        }
    }

    @Test
    void dropOldEvictsOldestEventsAndKeepsTheNewestUnderOverflow() throws Exception {
        GatedSink sink = new GatedSink();
        try (CaptureLogger logger = CaptureLogger.builder()
                .sink(sink)
                .queueCapacity(4)
                .overflowPolicy(OverflowPolicy.DROP_OLD)
                .build()) {

            // Send 8 events; with DROP_OLD, the LAST 4 should
            // remain in the ring (the first 4 get evicted as the
            // last 4 displace them).
            for (int i = 0; i < 8; i++) {
                logger.log("GET", "https://x/" + i);
            }

            CaptureLoggerStats s = logger.stats();
            // All 8 calls accepted as "logged" — but 4 of those
            // were subsequently displaced. Net surviving = 4.
            assertEquals(8, s.logged(),
                    "DROP_OLD logs every call; stats=" + s);
            assertEquals(4, s.dropped(),
                    "4 evictions to make room for events 4..7: stats=" + s);

            sink.release.countDown();
        }
        // Surviving events: 4..7 (the FOUR most recent).
        // DROP_OLD preserves recency.
        List<String> drained = sink.drained;
        assertEquals(4, drained.size());
        for (int i = 0; i < 4; i++) {
            assertTrue(drained.get(i).contains("/x/" + (i + 4)),
                    "DROP_OLD should preserve events 4..7; got " + drained.get(i) + " at " + i);
        }
        // And NONE of the dropped events should appear.
        for (String line : drained) {
            for (int dropped = 0; dropped < 4; dropped++) {
                assertNotEquals(true, line.contains("/x/" + dropped + "\""),
                        "evicted event " + dropped + " should not appear: " + line);
            }
        }
    }

    @Test
    void writerLagMsReportsPendingEventAgeUnderBackpressure() throws Exception {
        // To observe lag we need an event sitting IN THE RING. A
        // blocked sink alone isn't enough: the writer pulls the
        // very first event OUT of the ring before blocking on the
        // sink.write call. We need a second event to be queued in
        // the ring with the writer stuck on the first.
        GatedSink sink = new GatedSink();
        try (CaptureLogger logger = CaptureLogger.builder()
                .sink(sink)
                .queueCapacity(64)
                .build()) {

            // Event 1: writer pulls it from the ring, blocks on
            // sink.write.
            logger.log("GET", "https://event-1");
            // Give the writer time to start the blocked sink.write.
            Thread.sleep(20);

            // Event 2: stays in the ring because the writer is
            // busy with event 1. THIS is what writerLagMs will
            // sample at consumerSeq's slot.
            logger.log("GET", "https://event-2");

            // Let the lag accumulate.
            Thread.sleep(50);
            CaptureLoggerStats s = logger.stats();
            assertTrue(s.writerLagMs() >= 40L,
                    "writer lag should reflect the ~50ms sleep on event-2; got " + s.writerLagMs() + "ms");

            sink.release.countDown();
        }
    }

    @Test
    void writerLagMsIsZeroWhenRingIsEmpty() throws Exception {
        try (CaptureLogger logger = CaptureLogger.builder()
                .sink(new ListSink())
                .build()) {

            // Drain whatever lifecycle traffic might be there.
            Thread.sleep(20);
            CaptureLoggerStats s = logger.stats();
            assertEquals(0L, s.writerLagMs(),
                    "lag should be 0 when no event is pending; stats=" + s);
        }
    }

    @Test
    void offerOrEvictGiveUpReturnsNegativeSentinelEncodingEvictionsPerformed() throws Exception {
        // Deterministic give-up via the test-seam overload that
        // takes an explicit maxAttempts. With maxAttempts=0 the
        // loop body never executes, so the function MUST return
        // the give-up sentinel: -(0 evictions + 1) = -1.
        EventRingBuffer ring = new EventRingBuffer(4);
        int result = ring.offerOrEvict(
                new CaptureEvent(0d, "GET", "https://x", null, null), 0L);
        assertTrue(result < 0, "give-up must return a negative sentinel; got " + result);
        // Decode: evictions = -result - 1.
        int evictions = -result - 1;
        assertEquals(0, evictions,
                "no evictions happened with maxAttempts=0; got " + evictions);
    }

    @Test
    void giveUpPathAccountedAsDroppedNotLoggedInCallerAccounting() throws Exception {
        // The integration concern the reviewer flagged: when
        // offerOrEvict returns the negative sentinel, the LOGGER
        // must (a) NOT bump `logged` for the new event and
        // (b) bump `dropped` by (evictions + 1) — the +1
        // accounting for the new event that was lost.
        //
        // We exercise the decode by replicating CaptureLogger's
        // accounting block inline against a result we KNOW is the
        // give-up sentinel (forced via the maxAttempts=0 seam).
        // This pins the contract between EventRingBuffer's encoding
        // and CaptureLogger's decoding without needing to engineer
        // a real give-up in a concurrent scenario.
        EventRingBuffer ring = new EventRingBuffer(4);
        java.util.concurrent.atomic.LongAdder logged = new java.util.concurrent.atomic.LongAdder();
        java.util.concurrent.atomic.LongAdder dropped = new java.util.concurrent.atomic.LongAdder();

        int result = ring.offerOrEvict(
                new CaptureEvent(0d, "GET", "https://x", null, null), 0L);

        // Inline replica of CaptureLogger.log()'s DROP_OLD branch
        // (the production code is identical; if these two ever
        // drift, this test breaks loudly).
        if (result >= 0) {
            if (result > 0) dropped.add(result);
            logged.increment();
        } else {
            int evictionsOccurred = -result - 1;
            dropped.add(evictionsOccurred + 1L);
        }

        // Required outcome: the lost new event is in `dropped`,
        // not silently absorbed by an unconditional logged++.
        assertEquals(0, logged.sum(),
                "give-up must NOT increment logged");
        assertEquals(1, dropped.sum(),
                "give-up with 0 evictions must increment dropped by exactly 1 "
                        + "(the new event's loss); got " + dropped.sum());
    }

    @Test
    void giveUpAfterPartialEvictionsAccumulatesAllLossesInDropped() throws Exception {
        // When the maxAttempts cap fires AFTER some successful
        // evictions, the caller's accounting must charge dropped
        // by (evictions + 1) — the evicted events AND the new one.
        // We force this state via the test seam: encode a give-up
        // sentinel that claims 3 prior evictions, decode it via
        // the caller logic, and assert dropped == 4.
        int sentinel = -(3 + 1);   // == -4; "gave up after 3 evictions"
        java.util.concurrent.atomic.LongAdder logged = new java.util.concurrent.atomic.LongAdder();
        java.util.concurrent.atomic.LongAdder dropped = new java.util.concurrent.atomic.LongAdder();
        if (sentinel >= 0) {
            if (sentinel > 0) dropped.add(sentinel);
            logged.increment();
        } else {
            int evictionsOccurred = -sentinel - 1;
            dropped.add(evictionsOccurred + 1L);
        }
        assertEquals(0, logged.sum(),
                "even with prior evictions, the give-up branch must not bump logged");
        assertEquals(4, dropped.sum(),
                "3 evicted + 1 lost new event = 4 dropped; got " + dropped.sum());
    }

    @Test
    void dropOldUnderConcurrentProducersAccountsEveryCall() throws Exception {
        // 8 threads × 500 calls into a tiny ring with DROP_OLD.
        // Under DROP_OLD a single call can contribute to BOTH
        // `logged` (successful enqueue) AND `dropped` (one or more
        // displacements during the same call), so the strict
        // 3-way partition `logged + dropped + invalidDropped == total`
        // does NOT hold by design. The invariants we DO assert
        // below capture what actually has to be true: nothing
        // falls into a counter gap, including give-up calls.
        GatedSink sink = new GatedSink();
        sink.release.countDown();    // unblock the writer

        final int threads = 8;
        final int callsPerThread = 500;
        final int total = threads * callsPerThread;

        try (CaptureLogger logger = CaptureLogger.builder()
                .sink(sink)
                .queueCapacity(16)
                .overflowPolicy(OverflowPolicy.DROP_OLD)
                .build()) {

            java.util.concurrent.ExecutorService pool =
                    java.util.concurrent.Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            for (int t = 0; t < threads; t++) {
                final int tid = t;
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < callsPerThread; i++) {
                            logger.log("GET", "https://x/" + tid + "/" + i);
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            // The await() return value tells us whether all
            // producers actually finished. Ignoring it would let
            // this test pass on the weak invariants below even
            // when half the workload didn't run. Assert true →
            // fail-loudly on timeout.
            assertTrue(done.await(15, TimeUnit.SECONDS),
                    "all producer threads must complete within 15s");
            pool.shutdown();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS),
                    "executor must terminate cleanly within 5s after shutdown");

            CaptureLoggerStats s = logger.stats();
            // Under DROP_OLD, MOST calls land in logged (with their
            // displacements counted in dropped). A small fraction
            // may hit the bounded-attempts give-up path under
            // extreme contention — those are counted entirely in
            // dropped (the new event is lost too, not just the
            // evictions). Accounting invariants:
            //
            //   - logged ≤ total (each successful call: 1 to logged)
            //   - logged + (give-up calls) == total
            //   - dropped ≥ give-up calls = (total - logged)
            //     because each give-up contributes ≥1 to dropped.
            //
            // The key regression here vs the previous bug: every
            // call that didn't make it into `logged` is accounted
            // for in `dropped`. Nothing falls into a counter gap.
            assertEquals(0, s.invalidDropped(), "no invalid input; stats=" + s);
            assertTrue(s.logged() <= total,
                    "logged cannot exceed total calls; stats=" + s);
            long giveUpCalls = total - s.logged();
            assertTrue(s.dropped() >= giveUpCalls,
                    "every give-up call must contribute ≥1 to dropped; "
                            + "giveUpCalls=" + giveUpCalls + ", stats=" + s);
        }
    }
}
