package com.trafficmorph.capture.bench;

import com.trafficmorph.capture.CaptureLogger;
import com.trafficmorph.capture.RedactionPolicy;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Hot-path benchmarks for {@link CaptureLogger}. The whole
 * design contract for the agent is "log() returns in O(100ns)
 * and never blocks the producer thread on disk." These
 * benchmarks let us verify and track that.
 *
 * <h2>What's measured</h2>
 * <ul>
 *   <li><b>Latency</b> ({@code @Mode.AverageTime},
 *       {@code @OutputTimeUnit.NANOSECONDS}): producer-side
 *       cost of a single {@code log()} call across four
 *       representative shapes — cheapest (method + URL only),
 *       typical (POST with 4 headers + small JSON body),
 *       fat body (~4 KiB body — exercises the body-cap and
 *       JSONL formatter), and redacted (same as typical but
 *       with the default header-redaction policy applied).</li>
 *   <li><b>Throughput</b> ({@code @Mode.Throughput},
 *       {@code @OutputTimeUnit.SECONDS}): sustained ops/s with
 *       1 / 4 / 8 producer threads contending on the ring.
 *       Reveals how the MPSC ring buffer behaves under
 *       contention — the curve from one to many producers
 *       characterises the agent's scalability limit.</li>
 * </ul>
 *
 * <h2>What's NOT measured</h2>
 * Sink I/O cost: the benchmarks use {@link NoOpSink}, so disk
 * latency / kernel buffer behaviour is excluded. The writer
 * thread still drains the ring and runs the JSONL formatter,
 * so formatting and ring-drain are in scope; the storage
 * substrate is not.
 *
 * <h2>How to run</h2>
 * <pre>{@code
 *   mvn -Pbench test-compile exec:exec
 * }</pre>
 * (See the {@code bench} profile in pom.xml — it binds
 * {@code exec:exec}, not {@code exec:java}, because JMH's
 * worker JVMs need a fresh process to inherit the test
 * classpath at the OS level.) Or run {@link BenchmarkRunner#main}
 * directly from an IDE.
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class CaptureLoggerBenchmark {

    /**
     * Shared across all benchmark threads — the whole point of
     * the throughput-with-contention benchmarks is to exercise
     * the ring buffer under concurrent producers. Single-thread
     * latency benchmarks happen to also use this instance but
     * see no contention.
     */
    public CaptureLogger plainLogger;

    /**
     * Same shape as {@link #plainLogger} but with the default
     * header-redaction policy attached, so the redaction
     * benchmark exercises the regex/iteration cost on the
     * producer's hot path.
     */
    public CaptureLogger redactingLogger;

    public Map<String, String> typicalHeaders;
    public Map<String, String> sensitiveHeaders;
    public String smallBody;
    public String largeBody;

    @Setup
    public void setUp() {
        // Both loggers share the no-op sink and a generously-
        // sized ring so back-pressure / eviction never kicks in
        // during the measured window — we're characterising
        // STEADY-STATE producer cost, not overflow behaviour
        // (which has its own dedicated tests).
        plainLogger = CaptureLogger.builder()
                .sink(new NoOpSink())
                .headerRedaction(RedactionPolicy.none())
                .queueCapacity(65_536)
                .build();

        redactingLogger = CaptureLogger.builder()
                .sink(new NoOpSink())
                // .headerRedaction() defaults to the standard
                // sensitive-headers policy when not overridden;
                // making the choice explicit here for clarity.
                .queueCapacity(65_536)
                .build();

        typicalHeaders = Map.of(
                "Content-Type", "application/json",
                "User-Agent", "tm-capture-bench/1.0",
                "X-Trace-Id", "trace-abc-123",
                "Accept", "application/json");

        // Includes Authorization / Cookie — headers the default
        // redaction policy is meant to scrub. The redaction
        // benchmark hits these to exercise the actual scrubbing
        // path, not the no-redaction-needed fast path.
        sensitiveHeaders = Map.of(
                "Content-Type", "application/json",
                "Authorization", "Bearer sk-very-secret-token-abc123",
                "Cookie", "session=abc; csrf=def",
                "X-API-Key", "secret-key-123");

        smallBody = "{\"id\":\"x-1\",\"action\":\"bid\",\"price\":1.25}";
        // ~4 KiB of body — large enough to exercise the
        // body-cap truncation path and the JSONL formatter's
        // string-escape loop, small enough to fit comfortably
        // under the default 16 KiB cap.
        largeBody = "{\"payload\":\"" + "x".repeat(4_000) + "\"}";
    }

    @TearDown
    public void tearDown() {
        plainLogger.close();
        redactingLogger.close();
    }

    // ── Latency: average nanoseconds per call ─────────────────

    /**
     * Cheapest possible {@code log()} call — method + URL only,
     * no headers, no body. Establishes the floor: anything below
     * a few hundred nanoseconds is a contention-free CAS into
     * the ring plus a getNanoTime() call.
     */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void logCheap_latency() {
        plainLogger.log("GET", "https://example.com/api/x");
    }

    /**
     * Realistic POST with 4 headers + small JSON body. This is
     * the shape the agent was designed for — RTB bid requests,
     * REST API traffic, etc. Should still land under O(1µs)
     * for the producer.
     */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void logTypical_latency() {
        plainLogger.log("POST", "https://example.com/api/bid", typicalHeaders, smallBody);
    }

    /**
     * Same as typical but with a ~4 KiB body. Exercises the
     * body-cap truncation path and the JSONL formatter's
     * char-escape inner loop.
     */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void logFatBody_latency() {
        plainLogger.log("POST", "https://example.com/api/upload", typicalHeaders, largeBody);
    }

    /**
     * Typical-shape POST with the default header-redaction
     * policy. Exercises the redaction regex / iteration on
     * Authorization / Cookie / X-API-Key. The delta from
     * {@link #logTypical_latency()} is the redaction cost.
     */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void logTypicalWithRedaction_latency() {
        redactingLogger.log("POST", "https://example.com/api/auth", sensitiveHeaders, smallBody);
    }

    // ── Throughput: ops per second under concurrent producers ─

    /**
     * Single-producer throughput baseline — no ring contention,
     * upper bound on ops/s for one CPU's worth of producer work.
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Threads(1)
    public void logTypical_throughput_1thread() {
        plainLogger.log("POST", "https://example.com/api/bid", typicalHeaders, smallBody);
    }

    /**
     * Four-producer throughput — typical mid-range web app
     * concurrent-request shape. The MPSC ring's CAS-based
     * enqueue should still scale near-linearly here.
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Threads(4)
    public void logTypical_throughput_4threads() {
        plainLogger.log("POST", "https://example.com/api/bid", typicalHeaders, smallBody);
    }

    /**
     * Eight-producer throughput — heavy contention shape. The
     * curve from 1 → 4 → 8 reveals where ring-buffer contention
     * starts to dominate. Real deployments behind a load
     * balancer will sit somewhere on this curve.
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Threads(8)
    public void logTypical_throughput_8threads() {
        plainLogger.log("POST", "https://example.com/api/bid", typicalHeaders, smallBody);
    }
}
