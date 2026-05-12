package com.trafficmorph.capture.sink;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

/**
 * Sink that writes JSONL lines to {@code System.out}. Convenient as
 * the default for development, for tests, and for containerised
 * deployments where a log aggregator already harvests stdout.
 *
 * <p>Wraps {@code System.out} in a {@link BufferedWriter} so
 * per-event syscall cost is amortised. {@link #close()} flushes
 * but does NOT close {@code System.out} — closing the JVM's
 * shared stdout would break every other component that writes
 * to it. The buffer is the only thing this sink owns.
 */
public final class StdoutSink implements EventSink {

    private final BufferedWriter writer;
    private volatile boolean closed;

    public StdoutSink() {
        // 8 KB matches BufferedWriter's default and is the common
        // sweet spot for line-oriented output: small enough to flush
        // promptly on backpressure, big enough to absorb bursts.
        this.writer = new BufferedWriter(
                new OutputStreamWriter(System.out, StandardCharsets.UTF_8),
                8192);
    }

    @Override
    public void write(String line) throws IOException {
        if (closed) return;
        writer.write(line);
    }

    @Override
    public void flush() throws IOException {
        if (closed) return;
        writer.flush();
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        // Flush the wrapper buffer but DO NOT close it — that would
        // close the underlying OutputStreamWriter and, transitively,
        // System.out. Other parts of the JVM still need stdout.
        writer.flush();
    }
}
