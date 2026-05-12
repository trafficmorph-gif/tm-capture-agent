package com.trafficmorph.capture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Public-surface tests for {@link CaptureLogger}: builder validation,
 * counter shape, close semantics, blank-input handling, thread-safety
 * of counters. Pipeline / formatting / FIFO concerns live in
 * {@link CaptureLoggerPipelineTest}.
 *
 * <p>All tests route through an in-memory {@link ListSink} rather
 * than the default stdout sink, so the test runner output stays
 * clean.
 */
class CaptureLoggerSurfaceTest {

    @Test
    void builderRejectsNonPowerOfTwoCapacity() {
        // Mask-based ring-buffer indexing in Step 2 will require a
        // power-of-two capacity; we enforce it at build time so the
        // failure happens at startup, not on first overflow.
        assertThrows(IllegalArgumentException.class,
                () -> CaptureLogger.builder().queueCapacity(1000).build());
        assertThrows(IllegalArgumentException.class,
                () -> CaptureLogger.builder().queueCapacity(0).build());
        assertThrows(IllegalArgumentException.class,
                () -> CaptureLogger.builder().queueCapacity(-1).build());
    }

    @Test
    void builderRejectsDropOldUntilImplemented() {
        // DROP_OLD is declared on the enum but Step 2 doesn't
        // implement true oldest-eviction in the MPSC ring. Until
        // Step 6, the Builder rejects it at startup so deployments
        // can't silently get the wrong policy. The enum stays
        // public so callers writing "policy=DROP_OLD" don't compile-
        // break the moment Step 6 lands.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> CaptureLogger.builder()
                        .overflowPolicy(OverflowPolicy.DROP_OLD)
                        .build());
        assertTrue(ex.getMessage().contains("DROP_OLD"),
                "error message should name the policy: " + ex.getMessage());
    }

    @Test
    void builderAcceptsPowersOfTwo() {
        // 64, 65 536, 1 048 576 — should all be accepted.
        CaptureLogger.builder().queueCapacity(64).build().close();
        CaptureLogger.builder().queueCapacity(65_536).build().close();
        CaptureLogger.builder().queueCapacity(1 << 20).build().close();
    }

    @Test
    void logCountsAcceptedEventsAndDroppedAfterClose() {
        try (CaptureLogger logger = CaptureLogger.builder().sink(new ListSink()).build()) {
            logger.log("GET", "https://api.example.com/x");
            logger.log("POST", "https://api.example.com/y");
            CaptureLoggerStats s = logger.stats();
            assertEquals(2, s.logged());
            assertEquals(0, s.dropped());
            assertEquals(0, s.invalidDropped());
        }
    }

    @Test
    void logAfterCloseIncrementsDroppedCounter() {
        CaptureLogger logger = CaptureLogger.builder().sink(new ListSink()).build();
        logger.close();
        logger.log("GET", "https://api.example.com/x");
        CaptureLoggerStats s = logger.stats();
        assertEquals(0, s.logged());
        assertEquals(1, s.dropped());
        assertEquals(0, s.invalidDropped());
    }

    @Test
    void blankOrNullInputIncrementsInvalidDroppedNotLogged() {
        // Javadoc promises method/url are non-blank. The contract
        // is enforced quietly: blank input → invalidDropped++, never
        // an exception (logger-never-throws on hot paths).
        try (CaptureLogger logger = CaptureLogger.builder().sink(new ListSink()).build()) {
            logger.log(null, "https://x");
            logger.log("GET", null);
            logger.log("", "https://x");
            logger.log("GET", "");
            logger.log("   ", "https://x");
            logger.log("GET", "   ");

            CaptureLoggerStats s = logger.stats();
            assertEquals(0, s.logged(), "blank/null inputs must not be counted as logged");
            assertEquals(6, s.invalidDropped(), "every invalid call must bump invalidDropped");
            assertEquals(0, s.dropped(), "invalid inputs are not overflow drops");
        }
    }

    @Test
    void invalidInputCountedEvenAfterClose() {
        // Validation runs BEFORE the closed check so caller-side
        // contract violations remain diagnosable through shutdown.
        CaptureLogger logger = CaptureLogger.builder().sink(new ListSink()).build();
        logger.close();
        logger.log("", "https://x");
        logger.log("GET", "https://x");  // valid but logger closed → dropped
        CaptureLoggerStats s = logger.stats();
        assertEquals(0, s.logged());
        assertEquals(1, s.dropped(), "valid-after-close goes to dropped");
        assertEquals(1, s.invalidDropped(), "blank-after-close goes to invalidDropped");
    }

    @Test
    void statsSnapshotIsNonNullEvenWithNoTraffic() {
        try (CaptureLogger logger = CaptureLogger.builder().sink(new ListSink()).build()) {
            CaptureLoggerStats s = logger.stats();
            assertNotNull(s);
            assertEquals(0, s.logged());
            assertEquals(0, s.dropped());
            assertEquals(0, s.invalidDropped());
            assertEquals(0, s.queueDepth());
            assertEquals(0L, s.writerLagMs());
        }
    }

    @Test
    void countersAreSafeUnderConcurrentLoggers() throws InterruptedException {
        // Regression test for the volatile-long-plus-plus bug: under
        // contention from many threads, `logged++` on a plain
        // volatile field silently loses increments. With LongAdder,
        // every call must be accounted for. Pick numbers large enough
        // that the bug would be observable (small N hides it).
        final int threads = 16;
        final int callsPerThread = 25_000;
        final int total = threads * callsPerThread;

        try (CaptureLogger logger = CaptureLogger.builder().sink(new ListSink()).build()) {
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    try {
                        // Park until everyone's ready, so the racing
                        // window is as wide as possible.
                        start.await();
                        for (int i = 0; i < callsPerThread; i++) {
                            logger.log("GET", "https://x");
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            done.await(10, TimeUnit.SECONDS);
            pool.shutdown();

            // Post-join, traffic is quiesced — the three counters
            // partition all calls exactly. With the no-op Step 1
            // pipeline this used to be `logged == total`; with the
            // real Step 2 pipeline and a default queue, the writer
            // can't always keep up with a 16-thread burst, so some
            // calls land in `dropped` via overflow. Either way, no
            // call may be lost from the bookkeeping — the original
            // bug (volatile long ++) would have shown up as a sum
            // strictly less than `total` because increments
            // disappeared, not just relocated.
            CaptureLoggerStats s = logger.stats();
            assertEquals(total, s.logged() + s.dropped() + s.invalidDropped(),
                    "every concurrent log() call must be counted in exactly one bucket; stats=" + s);
        }
    }
}
