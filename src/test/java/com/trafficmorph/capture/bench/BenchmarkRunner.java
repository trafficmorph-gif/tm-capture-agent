package com.trafficmorph.capture.bench;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Entry point for the JMH suite. Programmatic invocation of
 * the runner is the lightest setup that works without a Maven
 * profile per benchmark — point at the {@link CaptureLoggerBenchmark}
 * class, accept the @Fork / @Warmup / @Measurement annotations
 * baked into the benchmark class, and run.
 *
 * <p>How to invoke (from the capture-agent module root):
 * <pre>{@code
 *   mvn -Pbench test-compile exec:exec
 * }</pre>
 * The bench profile binds {@code exec:exec} (NOT {@code exec:java})
 * because JMH spawns worker JVMs for each benchmark and the
 * worker classpath must be set up at the OS process level —
 * {@code exec:java} runs in Maven's own JVM and the workers
 * can't see the test classpath that way.
 *
 * <p>Or, from inside an IDE, run this class's {@code main}
 * directly with the test classpath.
 *
 * <p>Filter to a subset of benchmarks via {@code -Dbench=<regex>}.
 * JMH's {@code include()} is a case-sensitive regex; method names
 * use lowercase suffixes ({@code ..._latency},
 * {@code ..._throughput_*}), so the filter must match that case:
 * <pre>{@code
 *   mvn -Pbench test-compile exec:exec -Dbench=latency
 *   mvn -Pbench test-compile exec:exec -Dbench=throughput
 *   mvn -Pbench test-compile exec:exec -Dbench=logCheap_latency
 * }</pre>
 */
public final class BenchmarkRunner {

    private BenchmarkRunner() {
        // Utility class — main only.
    }

    public static void main(String[] args) throws RunnerException {
        // The system property is optional; without it we run
        // every benchmark in CaptureLoggerBenchmark.
        String filter = System.getProperty("bench",
                CaptureLoggerBenchmark.class.getSimpleName());

        // JMH forks a fresh JVM per benchmark by default to
        // avoid JIT-state contamination between methods. The
        // forked JVM doesn't inherit the launcher's classpath
        // — when invoked via exec-maven-plugin's exec:exec
        // goal, the fork can't find ForkedMain / the @Benchmark
        // stubs unless we explicitly pass the test classpath.
        // Capturing and forwarding the parent's java.class.path
        // is the simplest fix: exec:exec sets it correctly on
        // the launcher's command line, JMH's fork inherits it
        // via -cp, every benchmark stub loads cleanly.
        String classpath = System.getProperty("java.class.path");

        Options opt = new OptionsBuilder()
                .include(filter)
                .jvmArgs("-cp", classpath)
                .shouldFailOnError(true)
                .shouldDoGC(true)
                .build();

        new Runner(opt).run();
    }
}
