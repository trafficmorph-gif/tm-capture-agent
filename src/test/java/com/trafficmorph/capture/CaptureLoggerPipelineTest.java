package com.trafficmorph.capture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trafficmorph.capture.sink.EventSink;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Step 2 pipeline tests: ring-buffer + writer-thread + JSONL formatter
 * end-to-end. All tests use {@link ListSink} so assertions can be made
 * against the actual formatted lines without disk/stdout flakiness.
 */
class CaptureLoggerPipelineTest {

    @Test
    void simpleLogProducesJsonlLineInSink() throws Exception {
        ListSink sink = new ListSink();
        try (CaptureLogger logger = CaptureLogger.builder().sink(sink).build()) {
            logger.log("GET", "https://api.example.com/x");
            // close() forces the writer to drain before returning,
            // so we can assert against the sink right after.
        }
        List<String> lines = sink.lines();
        assertEquals(1, lines.size(), "exactly one line emitted");
        String line = lines.get(0);
        // Required JSONL fields present, in order, with the right shapes.
        assertTrue(line.startsWith("{\"t\":"), "line starts with t: " + line);
        assertTrue(line.contains("\"method\":\"GET\""), "method present: " + line);
        assertTrue(line.contains("\"url\":\"https://api.example.com/x\""), "url present: " + line);
        // Optional fields absent when caller didn't supply them.
        assertFalse(line.contains("\"headers\""), "headers absent: " + line);
        assertFalse(line.contains("\"body\""), "body absent: " + line);
        // Trailing newline so the sink/reader doesn't have to add it.
        assertTrue(line.endsWith("}\n"), "trailing newline: " + line);
    }

    @Test
    void headersAndBodyMakeItToTheLine() throws Exception {
        ListSink sink = new ListSink();
        // LinkedHashMap so we can predict the header iteration order.
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("X-Trace-Id", "abc-123");
        try (CaptureLogger logger = CaptureLogger.builder().sink(sink).build()) {
            logger.log("POST", "https://api.example.com/api/bid",
                    headers, "{\"id\":\"x-1\"}");
        }
        String line = sink.lines().get(0);
        assertTrue(line.contains("\"headers\":{\"Content-Type\":\"application/json\","
                        + "\"X-Trace-Id\":\"abc-123\"}"),
                "headers JSON-encoded in iteration order: " + line);
        // Body strings are JSON-escaped, so the inner quotes become \" .
        assertTrue(line.contains("\"body\":\"{\\\"id\\\":\\\"x-1\\\"}\""),
                "body JSON-escaped: " + line);
    }

    @Test
    void specialCharactersAreJsonEscapedInOutput() throws Exception {
        ListSink sink = new ListSink();
        try (CaptureLogger logger = CaptureLogger.builder().sink(sink).build()) {
            // Header value with a quote and a backslash.
            Map<String, String> h = Map.of("X-Note", "she said \"hi\" \\ done");
            logger.log("POST", "https://api.example.com/x", h,
                    "line1\nline2\ttab");
        }
        String line = sink.lines().get(0);
        assertTrue(line.contains("she said \\\"hi\\\" \\\\ done"),
                "quote + backslash escaped: " + line);
        assertTrue(line.contains("line1\\nline2\\ttab"),
                "newline + tab escaped: " + line);
        // And the WHOLE line still has exactly one literal newline
        // — the trailing one. The \n inside the body becomes the
        // two-character escape "\n", not a real newline.
        long actualNewlines = line.chars().filter(c -> c == '\n').count();
        assertEquals(1, actualNewlines, "only the trailing newline is literal");
    }

    @Test
    void eventsArriveInFifoOrder() throws Exception {
        ListSink sink = new ListSink();
        try (CaptureLogger logger = CaptureLogger.builder().sink(sink).build()) {
            for (int i = 0; i < 50; i++) {
                logger.log("GET", "https://api.example.com/seq/" + i);
            }
        }
        List<String> lines = sink.lines();
        assertEquals(50, lines.size());
        for (int i = 0; i < 50; i++) {
            assertTrue(lines.get(i).contains("/seq/" + i),
                    "line " + i + " out of order or wrong content: " + lines.get(i));
        }
    }

    @Test
    void tinyQueueDropsEventsUnderBurst() throws Exception {
        ListSink sink = new ListSink();
        // queueCapacity=2 with a slow consumer ⇒ a burst overflows.
        // We rely on a sink that doesn't block to avoid back-pressure;
        // the writer thread will drain as fast as it can, but offer()
        // is faster than poll() so a tight burst will overflow.
        ListSink slowSink = new ListSink() {
            @Override
            public void write(String line) {
                try {
                    Thread.sleep(2);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                super.write(line);
            }
        };
        CaptureLogger logger = CaptureLogger.builder()
                .sink(slowSink)
                .queueCapacity(2)
                .build();
        for (int i = 0; i < 100; i++) {
            logger.log("GET", "https://x/" + i);
        }
        logger.close();
        CaptureLoggerStats s = logger.stats();
        assertTrue(s.dropped() > 0, "expected overflow drops; stats=" + s);
        assertEquals(100, s.logged() + s.dropped() + s.invalidDropped(),
                "every log() call accounted for; stats=" + s);
    }

    @Test
    void closeFlushesAllEnqueuedEventsBeforeReturning() throws Exception {
        ListSink sink = new ListSink();
        CaptureLogger logger = CaptureLogger.builder().sink(sink).build();
        for (int i = 0; i < 500; i++) {
            logger.log("GET", "https://api.example.com/x/" + i);
        }
        logger.close();
        // The close() contract: when it returns, every event the ring
        // accepted has been written to the sink. With LOGGED == 500
        // and zero overflow (default queue is 65k), exactly 500 lines.
        CaptureLoggerStats s = logger.stats();
        assertEquals(500, s.logged());
        assertEquals(0, s.dropped(), "shouldn't overflow with default queue");
        assertEquals(500, sink.lines().size(),
                "close() must drain the ring before returning; sink lines=" + sink.lines().size());
        // Final flush also happened during close.
        assertTrue(sink.flushes() >= 1, "expected at least the final flush");
    }

    @Test
    void closeIsIdempotent() throws Exception {
        ListSink sink = new ListSink();
        CaptureLogger logger = CaptureLogger.builder().sink(sink).build();
        logger.log("GET", "https://x");
        logger.close();
        logger.close(); // second close: no-op, no exception
        assertEquals(1, sink.lines().size());
    }

    @Test
    void timestampIsMonotonicAcrossEvents() throws Exception {
        // Format renders t with 3 decimal places; consecutive events
        // logged in the same millisecond may share a timestamp value
        // (rounding), which is fine — the FIFO order via the ring
        // keeps them sorted. What we DON'T want is decreasing t.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = CaptureLogger.builder().sink(sink).build()) {
            for (int i = 0; i < 20; i++) {
                logger.log("GET", "https://x");
                // Tiny sleep to advance the clock so each iteration
                // surfaces a distinct t value most of the time.
                Thread.sleep(1);
            }
        }
        double prev = -1d;
        for (String line : sink.lines()) {
            // Naive parse: t starts at index 5 (after {"t":) up to the
            // first comma.
            int comma = line.indexOf(',');
            double t = Double.parseDouble(line.substring(5, comma));
            assertTrue(t >= prev, "timestamp non-monotonic: " + prev + " → " + t);
            prev = t;
        }
    }

    @Test
    void closeIsBoundedWhenAProducerIsStuck() throws Exception {
        // Pin close() to its 5s budget: a "producer" sits inside
        // log() forever (here simulated by hand-bumping the
        // in-flight counter via a hostile header map whose iterator
        // never returns). close() must still return within the
        // budget, and the timed-out flag must be set.
        //
        // The trick to "make a producer get stuck inside log()" in
        // a unit test: pass a Map whose entrySet().iterator() blocks.
        // log() snapshots headers via new LinkedHashMap<>(headers),
        // which invokes the iterator — and the producer parks there.
        // Since this happens AFTER inFlight.incrementAndGet(), it
        // genuinely exercises the close()-vs-stuck-producer path.
        ListSink sink = new ListSink();
        CountDownLatch producerParked = new CountDownLatch(1);
        CountDownLatch releaseProducer = new CountDownLatch(1);

        Map<String, String> blockingHeaders = new java.util.AbstractMap<>() {
            @Override
            public java.util.Set<Map.Entry<String, String>> entrySet() {
                return new java.util.AbstractSet<>() {
                    @Override
                    public java.util.Iterator<Map.Entry<String, String>> iterator() {
                        producerParked.countDown();
                        try {
                            // Block longer than the shutdown budget
                            // so we know it doesn't return on its own.
                            releaseProducer.await(30, TimeUnit.SECONDS);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                        // Empty after release so the snapshot completes.
                        return java.util.Collections.emptyIterator();
                    }
                    @Override
                    public int size() { return 1; }
                };
            }
        };

        CaptureLogger logger = CaptureLogger.builder().sink(sink).build();
        Thread producer = new Thread(() ->
                logger.log("POST", "https://x", blockingHeaders, "{}"),
                "test-stuck-producer");
        producer.setDaemon(true);
        producer.start();
        // Wait until the producer is demonstrably stuck inside log().
        assertTrue(producerParked.await(5, TimeUnit.SECONDS),
                "producer should reach the snapshot iterator");

        // close() must return within its budget despite the stuck
        // producer. Time it.
        long before = System.nanoTime();
        logger.close();
        long elapsedMs = (System.nanoTime() - before) / 1_000_000L;

        // 5s budget + slack for test scheduling; an unbounded close()
        // would hang at producer.join() effectively forever.
        assertTrue(elapsedMs < 8_000L,
                "close() should respect its shutdown budget; took " + elapsedMs + "ms");
        // And it explicitly flags the timeout for the caller.
        assertTrue(logger.shutdownTimedOut(),
                "shutdownTimedOut should be true when a producer is stuck");

        // Free the stuck producer so the test thread can clean up.
        releaseProducer.countDown();
        producer.join(5_000);
    }

    @Test
    void exceptionDuringHeaderSnapshotDoesNotEscapeLog() throws Exception {
        // Simulates the worst-case "caller mutated the map mid-copy"
        // scenario: pass a Map whose entrySet().iterator().next()
        // throws a runtime exception during the snapshot's
        // LinkedHashMap copy constructor. log() must catch it and
        // count it as invalidDropped — NOT propagate up the request
        // thread (which is the whole point of the hot-path
        // never-throws contract).
        ListSink sink = new ListSink();
        Map<String, String> hostile = new java.util.AbstractMap<>() {
            @Override
            public java.util.Set<Map.Entry<String, String>> entrySet() {
                // LinkedHashMap's copy constructor iterates entrySet.
                // Throwing from iterator() simulates a CME from a
                // racing mutator (or any other corruption).
                return new java.util.AbstractSet<>() {
                    @Override
                    public java.util.Iterator<Map.Entry<String, String>> iterator() {
                        throw new java.util.ConcurrentModificationException("racing mutator");
                    }
                    @Override
                    public int size() {
                        return 1;     // non-empty so the snapshot branch is taken
                    }
                };
            }
        };

        try (CaptureLogger logger = CaptureLogger.builder().sink(sink).build()) {
            // The call must NOT throw — that's the assertion.
            logger.log("POST", "https://x", hostile, "{}");
            CaptureLoggerStats s = logger.stats();
            assertEquals(0, s.logged(),
                    "broken-headers event must not be counted as logged; stats=" + s);
            assertEquals(1, s.invalidDropped(),
                    "broken-headers event must land in invalidDropped; stats=" + s);
        }
        // And nothing reached the sink, because we never offered.
        assertEquals(0, sink.lines().size());
    }

    @Test
    void mutatingHeadersAfterLogDoesNotCorruptOutput() throws Exception {
        ListSink sink = new ListSink();
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("X-A", "original-a");
        headers.put("X-B", "original-b");

        CaptureLogger logger = CaptureLogger.builder().sink(sink).build();
        logger.log("POST", "https://api.example.com/x", headers, "{}");

        // Mutate the caller's map IMMEDIATELY after log() returns,
        // exactly the failure mode the reviewer flagged: corrupt
        // output and/or ConcurrentModificationException on the
        // writer thread, killing the pipeline.
        headers.put("X-C", "added-after-log");
        headers.put("X-A", "MUTATED");
        headers.clear();   // worst case: empty map by the time writer iterates

        logger.close();

        assertEquals(1, sink.lines().size());
        String line = sink.lines().get(0);
        // Snapshot must have captured the original key/value pairs.
        assertTrue(line.contains("\"X-A\":\"original-a\""),
                "snapshot must preserve original X-A; got: " + line);
        assertTrue(line.contains("\"X-B\":\"original-b\""),
                "snapshot must preserve original X-B; got: " + line);
        // And nothing from the post-return mutation.
        assertFalse(line.contains("MUTATED"),
                "mutation after log() must not leak; got: " + line);
        assertFalse(line.contains("X-C"),
                "post-log keys must not appear; got: " + line);
    }

    @Test
    void sinkWriteFailureCountsAsWriteFailedNotDropped() throws Exception {
        // A sink that always throws IOException on write so we can
        // exercise the writer's error-handling branch. Implementing
        // EventSink directly (rather than extending ListSink) because
        // ListSink.write doesn't declare IOException — its override
        // can't add it.
        EventSink failing = new EventSink() {
            @Override
            public void write(String line) throws IOException {
                throw new IOException("disk full (simulated)");
            }

            @Override
            public void flush() {}

            @Override
            public void close() {}
        };
        CaptureLogger logger = CaptureLogger.builder().sink(failing).build();
        for (int i = 0; i < 10; i++) {
            logger.log("GET", "https://x/" + i);
        }
        logger.close();

        CaptureLoggerStats s = logger.stats();
        // All 10 calls were accepted by the producer side (queue
        // wasn't full), so they're counted in logged. Sink rejected
        // each → writeFailed=10. dropped stays clean — this is NOT
        // an enqueue refusal, it's a sink-side I/O loss.
        assertEquals(10, s.logged(), "all calls accepted at the producer: stats=" + s);
        assertEquals(0, s.dropped(),
                "sink I/O failure must not bump dropped (overflow / refused enqueue); stats=" + s);
        assertEquals(10, s.writeFailed(),
                "sink I/O failure must bump writeFailed; stats=" + s);
        assertEquals(0, s.invalidDropped());
    }

    @Test
    void closeIsRaceFreeUnderLiveProducers() throws Exception {
        // The actual race: close() is called WHILE producers are
        // still inside log(). The in-flight counter (+ the writer's
        // matching exit check) must ensure that every producer who
        // returned with `logged` incremented has its event written
        // to the sink — even those that were ghosting between
        // inFlight.incrementAndGet() and ring.offer() at the moment
        // close() flipped `closed`.
        ListSink sink = new ListSink();
        final int threads = 8;
        final int callsPerThread = 2_000;

        CaptureLogger logger = CaptureLogger.builder()
                .sink(sink)
                .queueCapacity(1 << 14)
                .build();

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(threads);
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
                    finished.countDown();
                }
            });
        }
        start.countDown();
        // Sleep just long enough that producers are demonstrably
        // running (so close() races their log() calls), but short
        // enough that they almost certainly haven't all finished.
        // 5ms is comfortably less than the time it takes 8 threads
        // to make 2 000 log() calls each on any non-stalled JVM.
        Thread.sleep(5);
        // Close WHILE producers are still in log(). Producers that
        // observed closed=false before close() flipped the flag
        // proceed to enqueue; producers that see closed=true
        // afterwards take the dropped branch. Either way, the
        // counter math has to balance out.
        logger.close();
        // Now drain the producer threads — they'll all return
        // promptly because subsequent log() calls hit the dropped
        // branch (closed=true). Assert true on the timed waits so
        // a stuck worker fails the test loudly instead of being
        // hidden behind subsequent weak invariants.
        assertTrue(finished.await(15, TimeUnit.SECONDS),
                "all producer threads must return after close");
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS),
                "executor must terminate cleanly within 5s");

        CaptureLoggerStats s = logger.stats();
        // Strict invariant: every event counted as `logged` must
        // have a corresponding line in the sink. Under the bug,
        // ghosting producers' events could land in the ring after
        // the writer exited — counted in `logged` but never
        // written, breaking this equality.
        assertEquals(s.logged(), sink.lines().size(),
                "every accepted event must reach the sink; stats=" + s
                        + ", sinkLines=" + sink.lines().size());
        // Strict invariant: producer call count is partitioned by
        // the three counters. We hit close() mid-burst so a healthy
        // mix of logged + dropped is expected — the value of this
        // assertion is that NO call escaped the bookkeeping.
        assertEquals(threads * callsPerThread,
                s.logged() + s.dropped() + s.invalidDropped(),
                "every log() call must land in exactly one counter; stats=" + s);
        // After racing close(), most production scenarios will see
        // dropped > 0 (some producers ran into the closed flag).
        // We don't assert dropped > 0 because timing varies; the
        // important assertion is the partition above.
    }

    @Test
    void concurrentProducersAllAccountedFor() throws Exception {
        // Confirms (a) the MPSC ring is correct under contention and
        // (b) close() drains every accepted event. Sum across the
        // three counters must equal total calls after quiescence.
        ListSink sink = new ListSink();
        final int threads = 8;
        final int callsPerThread = 5_000;
        final int total = threads * callsPerThread;

        // Skipping try-with-resources here so the logger reference is
        // still in scope for the post-close stats assertion.
        CaptureLogger logger = CaptureLogger.builder()
                .sink(sink)
                // Generous queue so the writer can keep up — this
                // isn't an overflow test, it's a correctness test
                // for the MPSC ring under contention.
                .queueCapacity(1 << 20)
                .build();

        ExecutorService pool = Executors.newFixedThreadPool(threads);
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
        // Timed awaits return false on timeout — assert true so a
        // stuck producer fails the test instead of letting it
        // continue with partial execution.
        assertTrue(done.await(15, TimeUnit.SECONDS),
                "all producer threads must complete within 15s");
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS),
                "executor must terminate cleanly within 5s");
        // All producers have returned; counters are stable.
        logger.close();
        // close() drained the ring synchronously, so every accepted
        // event is now in the sink.

        CaptureLoggerStats s = logger.stats();
        assertEquals(total, s.logged(), "every call should have been logged; stats=" + s);
        assertEquals(0, s.dropped(), "no overflow expected with the generous queue");
        assertEquals(total, sink.lines().size(),
                "every accepted event must have been written; stats=" + s);
    }
}
