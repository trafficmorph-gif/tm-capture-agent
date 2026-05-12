package com.trafficmorph.capture;

import java.util.Map;

/**
 * One queued capture event flowing from a producer thread through the
 * ring buffer to the writer thread.
 *
 * <p>Deliberately a record (immutable, value-like). Per-call allocation
 * pressure is acceptable because:
 * <ul>
 *   <li>Modern JVM escape analysis frequently stack-allocates records
 *       that don't escape the call site — and in many call shapes the
 *       producer's reference to the event becomes unreachable as soon
 *       as the ring buffer takes ownership.</li>
 *   <li>If JMH benchmarks (Step 8) show allocation hurting p99, we
 *       can swap to a pool of pre-allocated mutable event slots
 *       (LMAX Disruptor style) without changing the public API.
 *       Today's records are an honest baseline.</li>
 * </ul>
 *
 * <p>Package-private — not part of the public surface. Consumers see
 * only the {@code log(...)} entry points and the formatted JSONL
 * output via their sink.
 *
 * @param tSeconds  monotonic timestamp in seconds relative to the
 *                  logger's start. Step 4 will replace this with a
 *                  configurable time source; Step 2 uses
 *                  {@link System#nanoTime()} normalised to seconds.
 * @param method    HTTP method (already validated non-blank by
 *                  {@link CaptureLogger}).
 * @param url       full URL (already validated non-blank).
 * @param headers   optional header name → value map; {@code null}
 *                  when the caller didn't supply one. May be
 *                  redacted by Step 3.
 * @param body      optional raw request body string; {@code null}
 *                  when the caller didn't supply one.
 */
record CaptureEvent(
        double tSeconds,
        String method,
        String url,
        Map<String, String> headers,
        String body) {
}
