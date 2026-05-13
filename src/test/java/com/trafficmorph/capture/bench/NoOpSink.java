package com.trafficmorph.capture.bench;

import com.trafficmorph.capture.sink.EventSink;

/**
 * Sink that discards every write. Used by the JMH benchmarks
 * to isolate the logger's hot-path cost — the producer-side
 * cost we care about — from any sink-side I/O / allocation.
 *
 * <p>The writer thread will still pop events from the ring,
 * format them as JSONL, and call {@link #write(String)} here,
 * so the formatter and ring-drain costs ARE measured;
 * everything past the write() call is not. That's the right
 * boundary: real consumers wire FileSink or similar, but the
 * benchmark's job is to characterise the agent itself, not
 * the storage substrate.
 *
 * <p>Package-private to the bench package; not intended for
 * production use (use {@code FileSink} or your own
 * {@link EventSink} implementation).
 */
final class NoOpSink implements EventSink {

    @Override
    public void write(String line) {
        // Deliberate no-op. JIT will inline this and treat the
        // line argument as unused. The cost of constructing
        // `line` (JSONL formatting in the writer thread) is
        // still paid, which is what we want to measure.
    }

    @Override
    public void flush() {
        // No-op — nothing buffered.
    }

    @Override
    public void close() {
        // No-op — nothing held.
    }
}
