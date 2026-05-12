package com.trafficmorph.capture;

import com.trafficmorph.capture.sink.EventSink;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Test-only sink that captures every written line into an in-memory
 * list. CopyOnWriteArrayList so test threads can read the buffer
 * concurrently with the writer thread appending to it without
 * stamping on each other; reads are O(N) but test sizes are small.
 *
 * <p>Used as the destination for {@link CaptureLogger} in unit tests
 * so assertions can run against the actual formatted lines without
 * I/O cost or flakiness from disk/stdout timing.
 */
class ListSink implements EventSink {

    private final CopyOnWriteArrayList<String> lines = new CopyOnWriteArrayList<>();
    private volatile int flushes;

    @Override
    public void write(String line) {
        lines.add(line);
    }

    @Override
    public void flush() {
        flushes++;
    }

    @Override
    public void close() {
        // Nothing to release; lines stay accessible for post-test
        // assertions even after close.
    }

    List<String> lines() {
        return Collections.unmodifiableList(lines);
    }

    int flushes() {
        return flushes;
    }
}
