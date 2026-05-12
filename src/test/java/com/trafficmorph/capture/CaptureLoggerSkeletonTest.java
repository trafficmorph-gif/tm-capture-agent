package com.trafficmorph.capture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Step 1 smoke tests — pin down the public surface (builder
 * validation, counter shape, close-after-log semantics, blank-input
 * handling, thread-safety of counters) before the async pipeline
 * lands in Step 2.
 *
 * <p>The actual write path is a no-op in Step 1; these tests will
 * grow into real pipeline tests as later steps add the ring buffer,
 * writer thread, formatter, and sinks.
 */
class CaptureLoggerSkeletonTest {

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
    void builderAcceptsPowersOfTwo() {
        // 64, 65 536, 1 048 576 — should all be accepted.
        CaptureLogger.builder().queueCapacity(64).build().close();
        CaptureLogger.builder().queueCapacity(65_536).build().close();
        CaptureLogger.builder().queueCapacity(1 << 20).build().close();
    }

    @Test
    void logCountsAcceptedEventsAndDroppedAfterClose() {
        try (CaptureLogger logger = CaptureLogger.builder().build()) {
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
        CaptureLogger logger = CaptureLogger.builder().build();
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
        try (CaptureLogger logger = CaptureLogger.builder().build()) {
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
        CaptureLogger logger = CaptureLogger.builder().build();
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
        try (CaptureLogger logger = CaptureLogger.builder().build()) {
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

        try (CaptureLogger logger = CaptureLogger.builder().build()) {
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

            CaptureLoggerStats s = logger.stats();
            assertEquals(total, s.logged(),
                    "every concurrent log() call must be counted exactly once");
        }
    }
}
