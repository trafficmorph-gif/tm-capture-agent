# TrafficMorph Capture Agent

```
████████╗██████╗  █████╗ ███████╗███████╗██╗ ██████╗
╚══██╔══╝██╔══██╗██╔══██╗██╔════╝██╔════╝██║██╔════╝
   ██║   ██████╔╝███████║█████╗  █████╗  ██║██║
   ██║   ██╔══██╗██╔══██║██╔══╝  ██╔══╝  ██║██║
   ██║   ██║  ██║██║  ██║██║     ██║     ██║╚██████╗
   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝     ╚═╝ ╚═════╝
        ███╗   ███╗ ██████╗ ██████╗ ██████╗ ██╗  ██╗
        ████╗ ████║██╔═══██╗██╔══██╗██╔══██╗██║  ██║
        ██╔████╔██║██║   ██║██████╔╝██████╔╝███████║
        ██║╚██╔╝██║██║   ██║██╔══██╗██╔═══╝ ██╔══██║
        ██║ ╚═╝ ██║╚██████╔╝██║  ██║██║     ██║  ██║
        ╚═╝     ╚═╝ ╚═════╝ ╚═╝  ╚═╝╚═╝     ╚═╝  ╚═╝
```

[![Maven Central](https://img.shields.io/maven-central/v/com.trafficmorph/tm-capture-agent?label=Maven%20Central)](https://central.sonatype.com/artifact/com.trafficmorph/tm-capture-agent)
[![Latest tag](https://img.shields.io/github/v/tag/trafficmorph-gif/tm-capture-agent?sort=semver&label=latest)](https://github.com/trafficmorph-gif/tm-capture-agent/tags)
[![Java version](https://img.shields.io/badge/java-17%2B-blue)](#install)
[![License](https://img.shields.io/github/license/trafficmorph-gif/tm-capture-agent)](LICENSE)

Low-overhead, non-blocking Java library that emits JSONL traffic captures
consumable by TrafficMorph's capture-import flow. Designed to live on hot
paths: `log()` returns in O(100ns), I/O happens on a separate daemon
writer thread, the producer never blocks on disk.

- Java 17 baseline (lower than the main app's Java 21 for wider consumer reach).
- Zero transitive deps in the production jar (Servlet API is `provided`/`optional`).
- Published to Maven Central — drop it in alongside your other deps.

---

## Install

### Maven

```xml
<dependency>
    <groupId>com.trafficmorph</groupId>
    <artifactId>tm-capture-agent</artifactId>
    <version>0.1.1</version>
</dependency>
```

### Gradle (Kotlin DSL)

```kotlin
implementation("com.trafficmorph:tm-capture-agent:0.1.1")
```

### Gradle (Groovy)

```groovy
implementation 'com.trafficmorph:tm-capture-agent:0.1.1'
```

---

## Build from source

```bash
cd capture-agent
mvn package
```

Produces `target/tm-capture-agent-<version>.jar`. To install into your
local Maven repo for cross-project use:

```bash
mvn install
```

---

## Usage

### 1. Programmatic API — log directly from application code

```java
import com.trafficmorph.capture.CaptureLogger;
import com.trafficmorph.capture.sink.StdoutSink;
import java.util.Map;

try (CaptureLogger logger = CaptureLogger.builder()
        .sink(new StdoutSink())
        .build()) {

    // Cheapest call — method + URL only.
    logger.log("GET", "https://api.example.com/health");

    // Full call — method, URL, headers, body. None of these block;
    // log() returns in O(100ns) and the writer thread drains the
    // ring buffer to the sink asynchronously.
    logger.log(
            "POST",
            "https://api.example.com/bid",
            Map.of(
                    "Content-Type", "application/json",
                    "X-Trace-Id", "trace-abc-123"),
            "{\"id\":\"x-1\",\"action\":\"bid\"}");

}   // close() blocks until the writer drains the ring, then stops the daemon thread.
```

### 2. Rolling file sink with redaction

```java
import com.trafficmorph.capture.CaptureLogger;
import com.trafficmorph.capture.RedactionPolicy;
import com.trafficmorph.capture.sink.FileSink;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

FileSink sink = FileSink.builder(Path.of("/var/log/tm-capture.jsonl"))
        .maxAge(Duration.ofHours(1))   // rotate every hour
        .maxBytes(64L * 1024 * 1024)   // OR when the file hits 64 MiB
        .build();

CaptureLogger logger = CaptureLogger.builder()
        .sink(sink)
        // Scrub Authorization, Cookie, X-API-Key, etc. — the default
        // safelist covers the obvious sensitive headers. Use
        // RedactionPolicy.none() to disable redaction (e.g. in tests).
        .headerRedaction(RedactionPolicy.defaultSafelist())
        // Bound the per-event body string size at 16 KiB.
        .maxBodyLength(16 * 1024)
        // Bounded MPSC ring — controls back-pressure behaviour.
        .queueCapacity(8192)
        .build();

logger.log("POST", "https://api.example.com/auth",
        Map.of("Authorization", "Bearer super-secret-token"),
        "{\"user\":\"alex\"}");

// On shutdown — typically a Spring @Bean with destroyMethod="close",
// or your DI container's equivalent.
logger.close();
```

### 3. Servlet filter (Spring Boot 3 / jakarta.servlet 5+)

Drop the filter in front of every request to capture method, URL, headers,
and (opt-in) body for every inbound HTTP request:

```java
import com.trafficmorph.capture.CaptureLogger;
import com.trafficmorph.capture.servlet.CaptureServletFilter;
import com.trafficmorph.capture.sink.FileSink;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;

@Configuration
public class CaptureConfig {

    @Bean(destroyMethod = "close")
    public CaptureLogger captureLogger() throws IOException {
        return CaptureLogger.builder()
                .sink(FileSink.builder(Path.of("/var/log/tm-capture.jsonl"))
                        .maxAge(Duration.ofHours(1))
                        .build())
                .build();
    }

    @Bean
    public FilterRegistrationBean<CaptureServletFilter> captureFilter(
            CaptureLogger logger) {
        // Body capture is opt-in: true buffers the request body so
        // it appears on the capture line; false skips body reads
        // entirely (the right choice for streaming uploads, large
        // files, or WebSocket upgrade routes).
        CaptureServletFilter filter = new CaptureServletFilter(
                logger,
                /* captureBody */ true,
                /* maxBodyBytes */ 16 * 1024);
        return new FilterRegistrationBean<>(filter);
    }
}
```

The filter passes through every request untouched if capture instrumentation
fails (truncated upload, hostile client, sink failure) — capture must
never break a request. Multipart requests bypass body capture
automatically so downstream `getPart(s)` works unchanged.

### 4. Advanced filter config

For deployments needing more control:

```java
import com.trafficmorph.capture.CaptureLogger;
import com.trafficmorph.capture.servlet.CaptureServletFilter;
import jakarta.servlet.http.HttpServletRequest;
import java.nio.charset.Charset;
import java.util.Set;
import java.util.function.Predicate;

// logger from elsewhere — typically a @Bean(destroyMethod = "close")
// returning a CaptureLogger (see snippet 3 above).
CaptureLogger logger = /* ... */ null;

Predicate<HttpServletRequest> wsRoutes = req ->
        req.getRequestURI() != null && req.getRequestURI().startsWith("/ws/");

CaptureServletFilter filter = new CaptureServletFilter(
        logger,
        /* captureBody */ true,
        /* maxBodyBytes */ 16 * 1024,
        /* queryCharset */ Charset.forName("UTF-8"),
        /* upgradeRoutePredicate */ wsRoutes,        // allow ReadListener on these
        /* parsedBodyMethods */ Set.of("POST", "PUT"), // match container parseBodyMethods
        /* lenientParameterDecoding */ false);       // strict per Servlet 6.1
```

See the `CaptureServletFilter` javadoc for every knob's rationale.

---

## Integration recipes

### Wire format (JSONL)

Every event is one line; one line is one JSON object. The captured
JSONL is intended to be consumable by TrafficMorph's capture-import
flow without any post-processing.

```json
{"t":0.012,"method":"POST","url":"https://api.example.com/bid","headers":{"Content-Type":"application/json","X-Trace-Id":"trace-abc"},"body":"{\"id\":\"x-1\"}"}
```

Fields:

| Field | Type | Notes |
|---|---|---|
| `t` | number (seconds, non-negative finite) | Elapsed seconds since the logger started. Monotonically non-decreasing per logger. |
| `method` | string | HTTP method, passed through verbatim. The parser side upper-cases. |
| `url` | string | Full URL including query string. |
| `headers` | object (optional) | Name → value map. Multi-valued headers are pre-joined with `", "`. Absent / empty when no headers were logged. |
| `body` | string \| null (optional) | The body as captured, truncated to `maxBodyLength` chars. `null` when no body was logged. |

The parser is tolerant: extra fields are ignored, malformed lines are
counted-and-skipped (not fatal), empty lines are silently skipped.

### Recipe: custom sink (Kafka / syslog / async HTTP forwarder)

`EventSink` is a 3-method interface. Implement it to ship JSONL
anywhere — log aggregator, message bus, blob store, etc.

```java
import com.trafficmorph.capture.sink.EventSink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public final class KafkaSink implements EventSink {

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public KafkaSink(KafkaProducer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void write(String line) {
        // Called on the agent's single writer thread. Safe to do
        // blocking I/O here — the producer thread is already
        // back-pressured via the bounded ring; it's the consumer's
        // job to drain at whatever rate the sink supports.
        producer.send(new ProducerRecord<>(topic, line));
    }

    @Override
    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        producer.close();
    }
}
```

A misbehaving sink (one that throws, blocks too long, runs out of
memory) cannot break request flow — the producer's `log()` call has
already returned by the time the sink runs. The ring buffer drops
events under sustained back-pressure (per the configured
`OverflowPolicy`) rather than blocking producers.

### Recipe: per-route filter configuration

Different routes often need different capture settings. Body capture
for a JSON API route makes sense; for a streaming upload route or a
WebSocket endpoint, body capture is the wrong shape. Wire one filter
instance per URL pattern, sharing one logger:

```java
@Configuration
public class CaptureConfig {

    @Bean(destroyMethod = "close")
    public CaptureLogger captureLogger() throws IOException {
        return CaptureLogger.builder()
                .sink(FileSink.builder(Path.of("/var/log/tm-capture.jsonl"))
                        .maxAge(Duration.ofHours(1))
                        .build())
                .build();
    }

    // JSON API routes — body capture ON (small payloads, useful for replay).
    @Bean
    public FilterRegistrationBean<CaptureServletFilter> apiCaptureFilter(
            CaptureLogger logger) {
        FilterRegistrationBean<CaptureServletFilter> reg = new FilterRegistrationBean<>(
                new CaptureServletFilter(logger, /*captureBody*/ true, 16 * 1024));
        reg.addUrlPatterns("/api/*");
        return reg;
    }

    // Upload routes — body capture OFF (multipart auto-skips, but
    // setting captureBody=false makes the intent explicit and
    // avoids buffering even on fits-cap requests).
    @Bean
    public FilterRegistrationBean<CaptureServletFilter> uploadCaptureFilter(
            CaptureLogger logger) {
        FilterRegistrationBean<CaptureServletFilter> reg = new FilterRegistrationBean<>(
                new CaptureServletFilter(logger, /*captureBody*/ false, 0));
        reg.addUrlPatterns("/upload/*");
        return reg;
    }

    // WebSocket / upgrade routes — body capture OFF; for routes
    // that need ReadListener inside the wrapper, supply a
    // narrowing predicate via the five-arg constructor.
    @Bean
    public FilterRegistrationBean<CaptureServletFilter> wsCaptureFilter(
            CaptureLogger logger) {
        FilterRegistrationBean<CaptureServletFilter> reg = new FilterRegistrationBean<>(
                new CaptureServletFilter(logger));   // header + URL only
        reg.addUrlPatterns("/ws/*");
        return reg;
    }
}
```

Note both filters share `logger` — the agent is one logical sink for
the whole app, even when filter instances differ. Closing the logger
once at shutdown drains all of them.

### Recipe: plain Java / no DI container — shutdown hook

If you're not running inside Spring / Jakarta CDI / Guice / etc., wire
the logger's `close()` to a JVM shutdown hook so the writer drains
before exit:

```java
import com.trafficmorph.capture.CaptureLogger;
import com.trafficmorph.capture.sink.FileSink;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;

public final class CaptureBootstrap {

    public static CaptureLogger newProductionLogger() throws IOException {
        CaptureLogger logger = CaptureLogger.builder()
                .sink(FileSink.builder(Path.of("/var/log/tm-capture.jsonl"))
                        .maxAge(Duration.ofHours(1))
                        .maxBytes(64L * 1024 * 1024)
                        .build())
                .build();

        // Best-effort drain on JVM exit. The hook is bounded by
        // the agent's own close() timeout — close() returns even
        // if the writer can't fully drain (e.g. disk is full),
        // so the JVM never hangs on shutdown.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logger.close();
            } catch (Exception ignored) {
                // Shutdown-time failures are non-recoverable; the
                // JVM is going down anyway. Don't propagate.
            }
        }, "tm-capture-shutdown"));

        return logger;
    }
}
```

The same pattern applies to a custom sink: `logger.close()` calls
through to `sink.close()`, so a custom Kafka / HTTP / blob-store
sink's resources get released as part of the drain.

### Recipe: shipping captures into TrafficMorph's import flow

The JSONL produced here is the exact input format TrafficMorph's
import API expects. Typical flows:

- **Local / on-prem**: write to a file with `FileSink` (rolling on
  size + age), ship each rotated file to TrafficMorph via the import
  UI or the `POST /api/captures` endpoint.
- **Streaming**: implement an `EventSink` that POSTs each line (or a
  batch) directly to TrafficMorph's import endpoint. The main app's
  `CaptureParser` accepts JSONL streamed byte-by-byte.
- **Sidecar**: tail the rolling file with a log shipper (Vector,
  Fluent Bit, Filebeat, etc.) and ship to TrafficMorph from there.

The wire-format compatibility is guaranteed by the round-trip test
in the main app's test suite (`CaptureRoundTripTest`), which feeds
agent-emitted JSONL through `CaptureParser` on every CI run.

### Sizing guidance

Defaults are tuned for "moderate-traffic JSON / RTB" deployments.
Adjust when:

- **`queueCapacity` (default 16,384)** — bound on memory the agent
  holds on producer back-pressure. At default, ~16 K events × ~1 KiB
  per event ≈ 16 MiB worst case. Raise for higher sustained throughput
  spikes; lower if heap budget is tight.
- **`maxBodyLength` (default 16,384 chars)** — per-event body cap.
  Strictly bounded above by the filter's `maxBodyBytes` cap, but
  also enforced inside the logger for non-filter callers.
- **`OverflowPolicy.DROP_OLD` (default) vs `DROP_NEW`** — `DROP_OLD`
  evicts oldest queued events when the ring saturates; `DROP_NEW`
  refuses the newest. `DROP_OLD` is right for "most-recent-traffic-
  is-most-valuable" capture scenarios (the common case); `DROP_NEW`
  is right for "preserve historical order at all costs" archival.
- **Filter `maxBodyBytes`** — request body cap. Bodies larger than
  this skip body capture entirely (the filter logs method + URL +
  headers, never touches the input stream). Set this to the largest
  body size you actually care to capture; oversized bodies pass
  through with streaming preserved.

---

## Run the JMH benchmark suite

Latency and throughput benchmarks for the hot path live under
`src/test/java/com/trafficmorph/capture/bench/`.

```bash
# Run all benchmarks (~2 min wall time).
mvn -Pbench test-compile exec:exec

# Filter by lowercase regex — JMH's include() is case-sensitive and
# the methods use lowercase suffixes (..._latency, ..._throughput_*).
mvn -Pbench test-compile exec:exec -Dbench=latency
mvn -Pbench test-compile exec:exec -Dbench=throughput
mvn -Pbench test-compile exec:exec -Dbench=logCheap_latency
```

The profile uses `exec:exec` (NOT `exec:java`) so JMH's worker JVMs
inherit the test classpath at the OS process level. `exec:java` runs
in Maven's own JVM, where the forked workers can't see the classpath.

Reference numbers on a recent run (Apple Silicon, JDK 25):

| Benchmark | Result |
|---|---|
| `logCheap_latency` | ~160 ns/op |
| `logTypical_latency` (POST + 4 headers + small JSON body) | ~150 ns/op |
| `logFatBody_latency` (~4 KiB body) | ~110 ns/op |
| `logTypicalWithRedaction_latency` (default safelist redaction) | ~260 ns/op |
| `logTypical_throughput_1thread` | ~6.6 M ops/s |
| `logTypical_throughput_4threads` | ~6.1 M ops/s |
| `logTypical_throughput_8threads` | ~6.2 M ops/s |

Throughput saturates near 6 M ops/s in this setup because the
writer-thread drain (single consumer doing JSONL formatting) becomes
the bottleneck — the producer side is idle waiting on back-pressure.
For higher sustained rates, raise `queueCapacity` and / or use a
faster sink.

---

## Run the test suite

```bash
mvn test
```

Runs all JUnit tests (mock-based unit tests + one end-to-end Jetty
integration test). Benchmarks stay inert during `mvn test` — they're
not JUnit tests and aren't invoked by Surefire.

---

## Releases

Releases are cut from the monorepo with:

```bash
scripts/release-tm-capture-agent.sh X.Y.Z
```

The script bumps the `pom.xml` version, the README install snippets,
commits, tags `tm-capture-agent-vX.Y.Z` on the monorepo, mirrors the
`capture-agent/` subtree to the public repo at
`github.com/trafficmorph-gif/tm-capture-agent`, tags the mirror,
and the monorepo tag push fires
`.github/workflows/publish-tm-capture-agent.yml`, which runs the
`release` Maven profile (sources jar + javadoc jar + GPG signing
+ upload to Maven Central via the Sonatype Central Portal).

## License

Apache License 2.0 — see [LICENSE](LICENSE).
