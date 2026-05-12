package com.trafficmorph.capture.sink;

import java.io.IOException;

/**
 * Destination for formatted JSONL lines emitted by
 * {@code CaptureLogger}'s writer thread. All methods are called
 * from a single writer thread, so implementations DO NOT need
 * internal synchronisation against concurrent {@code write}s.
 *
 * <p>Implementations MUST:
 * <ul>
 *   <li>Accept lines that already include their trailing newline
 *       (the formatter appends {@code \n}).</li>
 *   <li>Defer I/O failures to the caller by throwing {@link IOException}
 *       — the writer thread will log and continue (one bad write
 *       shouldn't stall the pipeline).</li>
 *   <li>Make {@link #flush()} durable enough that, on a clean
 *       {@link #close()}, every previously-written line is observable
 *       to readers of the sink's backing store. {@code close()} is
 *       always preceded by a {@code flush()}.</li>
 * </ul>
 *
 * <p>The interface is intentionally minimal so consumers can plug in
 * non-file sinks: a metric counter, an in-memory buffer for tests,
 * a network socket for streaming captures to a collector, etc.
 */
public interface EventSink extends AutoCloseable {

    /**
     * Write one already-formatted JSONL line (including its trailing
     * {@code '\n'}). Called from the single writer thread.
     */
    void write(String line) throws IOException;

    /**
     * Force any buffered output through to the backing store. Called
     * periodically by the writer thread (bounded by event count or
     * elapsed time) and unconditionally before {@link #close()}.
     */
    void flush() throws IOException;

    /**
     * Release any underlying resources. Preceded by a flush call;
     * implementations should still be idempotent in case a caller
     * invokes close twice on a faulty shutdown path.
     */
    @Override
    void close() throws IOException;
}
