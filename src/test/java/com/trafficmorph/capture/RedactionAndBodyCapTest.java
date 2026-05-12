package com.trafficmorph.capture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Step 3 tests — header redaction and body truncation. Both run on
 * the producer thread inside {@code log()}'s snapshot path, so the
 * stored event already carries the safe values by the time it
 * reaches the writer.
 */
class RedactionAndBodyCapTest {

    // ── Header redaction ───────────────────────────────────────────

    @Test
    void defaultPolicyRedactsAuthorizationCookieAndApiKey() throws Exception {
        ListSink sink = new ListSink();
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Authorization", "Bearer eyJhbGciOiJIUzI1NiJ9.secret.signature");
        headers.put("Cookie", "session=abc123; userId=42");
        headers.put("X-Api-Key", "ak-live-supersecret");
        headers.put("Content-Type", "application/json");
        try (CaptureLogger logger = CaptureLogger.builder().sink(sink).build()) {
            logger.log("POST", "https://x", headers, null);
        }
        String line = sink.lines().get(0);
        // Secrets replaced.
        assertTrue(line.contains("\"Authorization\":\"[REDACTED]\""),
                "Authorization value should be redacted: " + line);
        assertTrue(line.contains("\"Cookie\":\"[REDACTED]\""),
                "Cookie value should be redacted: " + line);
        assertTrue(line.contains("\"X-Api-Key\":\"[REDACTED]\""),
                "X-Api-Key value should be redacted: " + line);
        // Non-secret header passes through.
        assertTrue(line.contains("\"Content-Type\":\"application/json\""),
                "Content-Type should pass through: " + line);
        // Original secret values must NOT appear anywhere in the line.
        assertFalse(line.contains("eyJhbGciOiJIUzI1NiJ9"),
                "JWT body must not leak: " + line);
        assertFalse(line.contains("supersecret"),
                "API key must not leak: " + line);
    }

    @Test
    void defaultPolicyMatchesHeaderNamesCaseInsensitively() throws Exception {
        ListSink sink = new ListSink();
        Map<String, String> headers = new LinkedHashMap<>();
        // HTTP headers ARE case-insensitive per RFC. Whether the
        // caller used canonical / lowercase / yelling-snake doesn't
        // change which header is being passed.
        headers.put("authorization", "Bearer x");
        headers.put("COOKIE", "session=y");
        headers.put("x-aPi-KEY", "z");
        try (CaptureLogger logger = CaptureLogger.builder().sink(sink).build()) {
            logger.log("POST", "https://x", headers, null);
        }
        String line = sink.lines().get(0);
        assertTrue(line.contains("\"authorization\":\"[REDACTED]\""), line);
        assertTrue(line.contains("\"COOKIE\":\"[REDACTED]\""), line);
        assertTrue(line.contains("\"x-aPi-KEY\":\"[REDACTED]\""), line);
        // The header NAMES kept their original casing — only values
        // got replaced.
    }

    @Test
    void customPolicyCanDropHeadersEntirely() throws Exception {
        // Returning the DROP sentinel omits the header from the
        // captured line altogether — not just value-replacement.
        // Useful for stripping internal-only headers.
        RedactionPolicy stripInternal = (name, value) ->
                name.startsWith("X-Internal-") ? RedactionPolicy.DROP : value;
        ListSink sink = new ListSink();
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("X-Internal-Trace", "trace-abc");
        headers.put("X-Internal-Region", "us-east-1");
        headers.put("Content-Type", "application/json");
        try (CaptureLogger logger = CaptureLogger.builder()
                .sink(sink)
                .headerRedaction(stripInternal)
                .build()) {
            logger.log("POST", "https://x", headers, null);
        }
        String line = sink.lines().get(0);
        assertFalse(line.contains("X-Internal-Trace"),
                "dropped header must not appear: " + line);
        assertFalse(line.contains("X-Internal-Region"),
                "dropped header must not appear: " + line);
        assertTrue(line.contains("\"Content-Type\":\"application/json\""),
                "kept header must pass through: " + line);
    }

    @Test
    void allHeadersDroppedMeansNoHeadersFieldInOutput() throws Exception {
        // When the policy drops every header, the output line should
        // have no "headers":... field at all (matching the
        // no-headers-supplied shape) rather than an empty object.
        RedactionPolicy dropAll = (name, value) -> RedactionPolicy.DROP;
        ListSink sink = new ListSink();
        Map<String, String> headers = Map.of("X-Foo", "y");
        try (CaptureLogger logger = CaptureLogger.builder()
                .sink(sink)
                .headerRedaction(dropAll)
                .build()) {
            logger.log("POST", "https://x", headers, null);
        }
        String line = sink.lines().get(0);
        assertFalse(line.contains("\"headers\""),
                "headers field should be absent entirely: " + line);
    }

    @Test
    void nonePolicyKeepsEverythingVerbatim() throws Exception {
        ListSink sink = new ListSink();
        Map<String, String> headers = Map.of("Authorization", "Bearer abc");
        try (CaptureLogger logger = CaptureLogger.builder()
                .sink(sink)
                .headerRedaction(RedactionPolicy.none())
                .build()) {
            logger.log("POST", "https://x", headers, null);
        }
        String line = sink.lines().get(0);
        // The whole point: with RedactionPolicy.none(), the caller
        // is opting out and the value goes through as-is.
        assertTrue(line.contains("\"Authorization\":\"Bearer abc\""),
                "none() must keep the value verbatim: " + line);
    }

    @Test
    void nullValuedHeaderCanBePreservedThroughRedaction() throws Exception {
        // Regression: under the old "null means drop" contract, a
        // caller-supplied null header value (legal in Java's
        // Map<String,String>) couldn't be preserved — the policy
        // received null, returned null (meaning "keep verbatim"),
        // and the caller code interpreted that as drop.
        //
        // With the DROP sentinel, returning null is unambiguously
        // "keep, even if it's null". The formatter renders a null
        // value as empty string in the output JSON.
        ListSink sink = new ListSink();
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("X-Null-Header", null);
        headers.put("X-Other", "value");
        try (CaptureLogger logger = CaptureLogger.builder()
                .sink(sink)
                .headerRedaction(RedactionPolicy.none())
                .build()) {
            logger.log("POST", "https://x", headers, null);
        }
        String line = sink.lines().get(0);
        // The null-valued header is preserved (rendered as empty
        // string by the formatter's existing null-handling branch).
        assertTrue(line.contains("\"X-Null-Header\":\"\""),
                "null-valued header must be kept (as empty string): " + line);
        assertTrue(line.contains("\"X-Other\":\"value\""), line);
    }

    @Test
    void nullKeyedHeaderIsSkippedBeforeReachingThePolicy() throws Exception {
        // A null map key is legal in HashMap-style maps but
        // meaningless as an HTTP header. We skip null-keyed entries
        // in the snapshot pass so the RedactionPolicy contract
        // ("headerName is never null") holds.
        //
        // A policy that throws on a null name would otherwise route
        // the WHOLE event to invalidDropped — disproportionate
        // collateral damage for a single bad map entry.
        ListSink sink = new ListSink();
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(null, "should-be-skipped");
        headers.put("Content-Type", "application/json");

        // A policy that throws if it ever sees a null name. If the
        // null-key skip works, this policy never sees null and the
        // event flows through normally.
        RedactionPolicy explodes = (name, value) -> {
            if (name == null) throw new AssertionError("policy received null name");
            return value;
        };
        try (CaptureLogger logger = CaptureLogger.builder()
                .sink(sink)
                .headerRedaction(explodes)
                .build()) {
            logger.log("POST", "https://x", headers, null);
            // Event logged successfully (not routed to invalidDropped).
            CaptureLoggerStats s = logger.stats();
            assertEquals(1, s.logged());
            assertEquals(0, s.invalidDropped());
        }
        String line = sink.lines().get(0);
        // Only the non-null-keyed header made it.
        assertTrue(line.contains("\"Content-Type\":\"application/json\""), line);
        // And the dropped value text didn't leak under some other name.
        assertFalse(line.contains("should-be-skipped"), line);
    }

    @Test
    void dropSentinelIsIdentityComparedNotEqualityCompared() throws Exception {
        // Returning a string that happens to have the same content
        // as DROP must NOT trigger a drop — only the actual field
        // reference does. Guards against the situation where a
        // real header value coincidentally matches the sentinel's
        // content (vanishingly unlikely, but the test pins the
        // semantics).
        ListSink sink = new ListSink();
        RedactionPolicy lookalike = (name, value) ->
                new String(RedactionPolicy.DROP.toCharArray());
        Map<String, String> headers = Map.of("X-Foo", "bar");
        try (CaptureLogger logger = CaptureLogger.builder()
                .sink(sink)
                .headerRedaction(lookalike)
                .build()) {
            logger.log("POST", "https://x", headers, null);
        }
        String line = sink.lines().get(0);
        // Header is KEPT — the policy returned a string whose
        // content matches DROP but reference doesn't. The value
        // shows through in the output.
        assertTrue(line.contains("\"X-Foo\":\""),
                "lookalike string should not trigger drop: " + line);
    }

    // ── Body truncation ────────────────────────────────────────────

    @Test
    void bodyShorterThanCapStoredVerbatim() throws Exception {
        ListSink sink = new ListSink();
        // Small body — no truncation.
        String body = "{\"id\":1}";
        try (CaptureLogger logger = CaptureLogger.builder()
                .sink(sink)
                .maxBodyLength(16_384)
                .build()) {
            logger.log("POST", "https://x", null, body);
        }
        String line = sink.lines().get(0);
        assertTrue(line.contains("\"body\":\"{\\\"id\\\":1}\""),
                "small body kept verbatim: " + line);
        assertFalse(line.contains("truncated"),
                "no truncation marker on a small body: " + line);
    }

    @Test
    void bodyLongerThanCapIsTruncatedWithMarker() throws Exception {
        ListSink sink = new ListSink();
        // 100 chars of 'a' — way past the cap.
        StringBuilder huge = new StringBuilder();
        for (int i = 0; i < 100; i++) huge.append('a');
        try (CaptureLogger logger = CaptureLogger.builder()
                .sink(sink)
                .maxBodyLength(10)
                .build()) {
            logger.log("POST", "https://x", null, huge.toString());
        }
        String line = sink.lines().get(0);
        // Body field present, contains exactly 10 a's then the marker.
        assertTrue(line.contains("\"body\":\"aaaaaaaaaa...[truncated 90 chars]\""),
                "body truncated with marker reporting dropped chars: " + line);
    }

    @Test
    void zeroBodyCapDropsBodyEntirely() throws Exception {
        ListSink sink = new ListSink();
        try (CaptureLogger logger = CaptureLogger.builder()
                .sink(sink)
                .maxBodyLength(0)
                .build()) {
            logger.log("POST", "https://x", null, "anything");
        }
        String line = sink.lines().get(0);
        // No body field at all.
        assertFalse(line.contains("\"body\""),
                "body should be omitted when cap is 0: " + line);
    }

    @Test
    void zeroBodyCapAlsoDropsEmptyBody() throws Exception {
        // Regression: the old `body.length() > maxBodyLength` check
        // missed body=="" because length 0 isn't > 0. The contract
        // says "0 disables bodies entirely" — an empty body must
        // honour that just like a non-empty one.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = CaptureLogger.builder()
                .sink(sink)
                .maxBodyLength(0)
                .build()) {
            logger.log("POST", "https://x", null, "");
        }
        String line = sink.lines().get(0);
        assertFalse(line.contains("\"body\""),
                "empty body should also be dropped at cap 0: " + line);
    }

    @Test
    void negativeBodyCapIsRejectedAtBuildTime() {
        // A typo like maxBodyLength(-1) would otherwise silently
        // disable bodies (with my zero-cap-drops-body logic). Catch
        // it at startup with a clear message instead.
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> CaptureLogger.builder().maxBodyLength(-1).build());
        assertTrue(ex.getMessage().contains("maxBodyLength"),
                "error should name the field: " + ex.getMessage());
    }

    @Test
    void truncationDoesNotApplyToHeadersFieldOnlyToBody() throws Exception {
        // Sanity: the body cap doesn't accidentally clip header
        // values when one of them happens to be longer than the cap.
        ListSink sink = new ListSink();
        StringBuilder longHeader = new StringBuilder();
        for (int i = 0; i < 50; i++) longHeader.append('h');
        Map<String, String> headers = Map.of("X-Long", longHeader.toString());
        try (CaptureLogger logger = CaptureLogger.builder()
                .sink(sink)
                .maxBodyLength(10)
                .headerRedaction(RedactionPolicy.none())   // keep value
                .build()) {
            logger.log("POST", "https://x", headers, "small");
        }
        String line = sink.lines().get(0);
        assertTrue(line.contains("\"X-Long\":\"" + longHeader + "\""),
                "header value not affected by maxBodyLength: " + line);
        assertFalse(line.contains("truncated"),
                "small body shouldn't trigger truncation: " + line);
    }
}
