package com.trafficmorph.capture.servlet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.trafficmorph.capture.CaptureLogger;
import com.trafficmorph.capture.ListSink;
import com.trafficmorph.capture.RedactionPolicy;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Step 7 — turnkey servlet filter. Tests cover:
 *  - URL + headers + method captured for every HTTP request.
 *  - Body capture is opt-in and respects the size cap.
 *  - Streaming-shaped requests (no Content-Length) bypass body capture.
 *  - Downstream chain still reads the original request body bytes
 *    when the wrapper is in place.
 *  - Capture failures NEVER break the request.
 *  - Non-HTTP servlet requests pass through untouched.
 */
class CaptureServletFilterTest {

    private static ServletInputStream stream(byte[] body) {
        ByteArrayInputStream backing = new ByteArrayInputStream(body);
        return new ServletInputStream() {
            @Override public int read() { return backing.read(); }
            @Override public boolean isFinished() { return backing.available() == 0; }
            @Override public boolean isReady() { return true; }
            @Override public void setReadListener(jakarta.servlet.ReadListener l) {}
        };
    }

    private static HttpServletRequest httpRequest(String method, String url, String queryString,
                                                   List<String> headerNames,
                                                   java.util.Map<String, List<String>> headers,
                                                   byte[] body) throws IOException {
        HttpServletRequest http = mock(HttpServletRequest.class);
        when(http.getMethod()).thenReturn(method);
        when(http.getRequestURL()).thenReturn(new StringBuffer(url));
        when(http.getQueryString()).thenReturn(queryString);
        // thenAnswer (not thenReturn) so each invocation gets a
        // FRESH Enumeration — real HttpServletRequest does the
        // same, and any call site that iterates the enumeration
        // (collectHeaders, looksLikeUpgrade, etc.) leaves it
        // exhausted for subsequent callers. With thenReturn we'd
        // hand the same one-shot enumeration to every caller,
        // and the second consumer would see "no headers".
        when(http.getHeaderNames()).thenAnswer(inv -> Collections.enumeration(headerNames));
        for (String name : headerNames) {
            when(http.getHeaders(name))
                    .thenAnswer(inv -> Collections.enumeration(headers.get(name)));
            // getHeader (singular) returns the FIRST value or null.
            // No state — same value on every call, matches real impl.
            List<String> values = headers.get(name);
            when(http.getHeader(name)).thenReturn(
                    values == null || values.isEmpty() ? null : values.get(0));
        }
        when(http.getContentLengthLong()).thenReturn(body == null ? -1L : (long) body.length);
        when(http.getInputStream()).thenReturn(stream(body == null ? new byte[0] : body));
        when(http.getCharacterEncoding()).thenReturn("UTF-8");
        // Default Content-Type: derive from the headers map if
        // present, else null. The wrapper's parameter-handling
        // path branches on getContentType(); leaving it null
        // would silently skip the form-param interception logic
        // when tests want to exercise it.
        if (headers != null && headers.containsKey("Content-Type")) {
            List<String> ctValues = headers.get("Content-Type");
            when(http.getContentType()).thenReturn(
                    ctValues == null || ctValues.isEmpty() ? null : ctValues.get(0));
        } else {
            when(http.getContentType()).thenReturn(null);
        }
        // Default: NOT in async mode (matches the common case for
        // most servlet requests). ReadListener-using tests opt in
        // via markAsyncStarted().
        when(http.isAsyncStarted()).thenReturn(false);
        return http;
    }

    /**
     * Configure the mock to report that {@code startAsync()} has
     * been called on this request. Required before calling
     * {@link ServletInputStream#setReadListener} per the Servlet spec.
     */
    private static void markAsyncStarted(HttpServletRequest http) {
        when(http.isAsyncStarted()).thenReturn(true);
    }

    private static CaptureLogger loggerWithSink(ListSink sink) {
        return CaptureLogger.builder()
                .sink(sink)
                // RedactionPolicy.none() to keep test assertions
                // about specific header VALUES straightforward.
                .headerRedaction(RedactionPolicy.none())
                .build();
    }

    @Test
    void capturesMethodUrlAndHeaders() throws Exception {
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger);
            HttpServletRequest req = httpRequest(
                    "POST",
                    "https://api.example.com/api/bid",
                    "ssp=pub&deal=42",
                    List.of("Content-Type", "X-Trace-Id"),
                    java.util.Map.of(
                            "Content-Type", List.of("application/json"),
                            "X-Trace-Id", List.of("abc-123")),
                    null);
            HttpServletResponse resp = mock(HttpServletResponse.class);
            FilterChain chain = mock(FilterChain.class);

            filter.doFilter(req, resp, chain);

            // Chain WAS called — instrumentation didn't block the request.
            verify(chain, times(1)).doFilter(any(ServletRequest.class), any(ServletResponse.class));
        }
        // Logger drained on close → exactly one line in the sink.
        assertEquals(1, sink.lines().size());
        String line = sink.lines().get(0);
        assertTrue(line.contains("\"method\":\"POST\""), line);
        assertTrue(line.contains("\"url\":\"https://api.example.com/api/bid?ssp=pub&deal=42\""), line);
        assertTrue(line.contains("\"Content-Type\":\"application/json\""), line);
        assertTrue(line.contains("\"X-Trace-Id\":\"abc-123\""), line);
        // No body field — capture-body defaulted to false.
        assertFalse(line.contains("\"body\""), line);
    }

    @Test
    void multiValuedHeadersJoinedWithCommaSpace() throws Exception {
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger);
            HttpServletRequest req = httpRequest(
                    "GET",
                    "https://x/foo",
                    null,
                    List.of("Accept"),
                    java.util.Map.of("Accept", List.of("application/json", "text/plain")),
                    null);

            filter.doFilter(req, mock(HttpServletResponse.class), mock(FilterChain.class));
        }
        // Standard HTTP wire form: multi-valued header values joined
        // with ", ". The logger / parser already accepts either form;
        // this asserts our specific choice.
        assertTrue(sink.lines().get(0).contains("\"Accept\":\"application/json, text/plain\""),
                "expected comma-space join; got: " + sink.lines().get(0));
    }

    @Test
    void bodyCapturedWhenEnabledAndContentLengthFits() throws Exception {
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, /*captureBody*/ true, 16_384);
            byte[] body = "{\"id\":\"x-1\",\"ssp\":\"pubmatic\"}".getBytes();
            HttpServletRequest req = httpRequest(
                    "POST", "https://x/bid", null,
                    List.of("Content-Type"),
                    java.util.Map.of("Content-Type", List.of("application/json")),
                    body);

            filter.doFilter(req, mock(HttpServletResponse.class), mock(FilterChain.class));
        }
        String line = sink.lines().get(0);
        assertTrue(line.contains("\"body\":\"{\\\"id\\\":\\\"x-1\\\",\\\"ssp\\\":\\\"pubmatic\\\"}\""),
                "body should be JSON-escaped in the line: " + line);
    }

    @Test
    void bodyNotCapturedWhenContentLengthExceedsCap() throws Exception {
        // A body bigger than the cap → filter skips body capture
        // entirely (preserves streaming behaviour; doesn't even
        // touch the request's input stream).
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            // Tiny cap so a small body trips it.
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 10);
            byte[] big = new byte[1_000];
            HttpServletRequest req = httpRequest(
                    "POST", "https://x", null,
                    List.of(), java.util.Map.of(), big);

            filter.doFilter(req, mock(HttpServletResponse.class), mock(FilterChain.class));
            // Filter should NOT have touched the input stream — the
            // downstream chain still owns it. Verify with a spy.
            verify(req, never()).getInputStream();
        }
        // Line should have method + URL but NO body field.
        String line = sink.lines().get(0);
        assertFalse(line.contains("\"body\""),
                "oversized body should be skipped, no body field expected: " + line);
    }

    @Test
    void streamingRequestWithoutContentLengthSkipsBodyCapture() throws Exception {
        // Content-Length = -1 (unknown / chunked transfer) →
        // filter must NOT read the body. Streaming endpoints
        // depend on this.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            HttpServletRequest req = mock(HttpServletRequest.class);
            when(req.getMethod()).thenReturn("POST");
            when(req.getRequestURL()).thenReturn(new StringBuffer("https://x/stream"));
            when(req.getContentLengthLong()).thenReturn(-1L);
            when(req.getHeaderNames()).thenReturn(Collections.emptyEnumeration());

            filter.doFilter(req, mock(HttpServletResponse.class), mock(FilterChain.class));

            verify(req, never()).getInputStream();
        }
    }

    @Test
    void downstreamReadsTheOriginalBodyWhenWrapperIsActive() throws Exception {
        // Critical correctness test: when the filter pre-reads the
        // body for capture, downstream chain code must STILL see
        // the original bytes via getInputStream().
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            byte[] body = "hello world".getBytes();
            HttpServletRequest req = httpRequest(
                    "POST", "https://x", null,
                    List.of(), java.util.Map.of(), body);

            // Capture what the downstream chain reads via the filter's
            // wrapper. The filter passes a WRAPPED request to chain.doFilter;
            // we sniff the bytes the wrapper exposes.
            byte[][] downstreamRead = new byte[1][];
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                ServletInputStream in = ((HttpServletRequest) reqArg).getInputStream();
                java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
                byte[] buf = new byte[1024];
                int n;
                while ((n = in.read(buf)) != -1) out.write(buf, 0, n);
                downstreamRead[0] = out.toByteArray();
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);

            assertNotNull(downstreamRead[0], "downstream must have received some bytes");
            assertEquals("hello world", new String(downstreamRead[0]),
                    "downstream must read the EXACT original body bytes");
        }
    }

    @Test
    void hostileRequestThrowingDuringHeaderExtractionDoesNotPropagateToChain() throws Exception {
        // The filter's safelyLog() is the safety net against
        // anything inside the request inspection raising a
        // RuntimeException. CaptureLogger.log already catches its
        // own RuntimeExceptions internally, so the realistic
        // trigger is a hostile HttpServletRequest implementation —
        // e.g. one that throws from getHeaderNames(). The filter
        // must still pass control to the chain.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger);
            HttpServletRequest req = mock(HttpServletRequest.class);
            when(req.getMethod()).thenReturn("GET");
            when(req.getRequestURL()).thenReturn(new StringBuffer("https://x"));
            when(req.getHeaderNames()).thenThrow(new RuntimeException("header oops"));
            when(req.getContentLengthLong()).thenReturn(-1L);
            FilterChain chain = mock(FilterChain.class);

            // Must NOT throw — the filter swallows the
            // header-inspection failure.
            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            // Chain proceeded normally.
            verify(chain, times(1)).doFilter(any(), any());
        }
        // The line was NOT logged (header extraction failed before
        // log() could be called) — that's the right tradeoff: lose
        // capture for a hostile request rather than break it.
        assertEquals(0, sink.lines().size());
    }

    @Test
    void nonHttpServletRequestPassesThroughUntouched() throws Exception {
        // E.g. a websocket upgrade probe; we get a ServletRequest
        // that isn't HttpServletRequest. The filter must just pass
        // through, NOT try to log anything. We can't mock the final
        // CaptureLogger, so we observe "didn't log" via a sink that
        // records calls.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger);
            ServletRequest nonHttp = mock(ServletRequest.class);
            FilterChain chain = mock(FilterChain.class);

            filter.doFilter(nonHttp, mock(ServletResponse.class), chain);

            // All-matchers form: Mockito requires matchers across
            // every parameter or none. `eq(nonHttp)` checks the
            // exact reference, any() matches the response.
            verify(chain, times(1)).doFilter(eq(nonHttp), any(ServletResponse.class));
        }
        assertEquals(0, sink.lines().size(),
                "non-HTTP request must not produce a captured line");
    }

    @Test
    void setReadListenerWithoutAsyncRequiresContainerSpecCompliantThrow() throws Exception {
        // Spec: ReadListener can only be set during async processing
        // or after HTTP upgrade. A real container throws
        // IllegalStateException when neither is true; the replay
        // stream must do the same so a buggy sync handler doesn't
        // appear to work through capture instrumentation but break
        // in production.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            byte[] body = "sync handler".getBytes();
            HttpServletRequest req = httpRequest(
                    "POST", "https://x", null,
                    List.of(), java.util.Map.of(), body);
            // Deliberately DO NOT call markAsyncStarted(req) —
            // simulates a synchronous handler attempting to use
            // the non-blocking API by mistake.

            Throwable[] caught = new Throwable[]{null};
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                ServletInputStream in = ((HttpServletRequest) reqArg).getInputStream();
                try {
                    in.setReadListener(new jakarta.servlet.ReadListener() {
                        @Override public void onDataAvailable() {}
                        @Override public void onAllDataRead() {}
                        @Override public void onError(Throwable t) {}
                    });
                } catch (Throwable t) {
                    caught[0] = t;
                }
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertNotNull(caught[0],
                    "non-async setReadListener must throw — spec precondition violated");
            assertTrue(caught[0] instanceof IllegalStateException,
                    "expected IllegalStateException per spec; got " + caught[0]);
            assertTrue(caught[0].getMessage().toLowerCase().contains("async"),
                    "error message should mention async precondition; got: "
                            + caught[0].getMessage());
        }
    }

    @Test
    void setReadListenerAcceptedWhenUpgradeRoutePredicateMatchesTheRequest() throws Exception {
        // Strict default rejects setReadListener outside async to
        // catch sync-handler bugs and reject client-spoofed upgrade
        // headers. Genuine upgrade-mode routes opt out via a
        // deployer-supplied predicate that explicitly identifies
        // which requests should bypass the precondition.
        //
        // Verification: the SAME upgrade-shaped request the strict-
        // default test rejects must now succeed when the filter is
        // built with a predicate that matches it. The consumer
        // must also be able to drain the buffered body via the
        // registered listener.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            // Path-scoped predicate: only bypass for WebSocket routes
            // (URLs starting with wss://). The deployer's choice of
            // scoping criterion lives entirely in the predicate body.
            java.util.function.Predicate<HttpServletRequest> wsRoutes =
                    req -> req.getRequestURL() != null
                            && req.getRequestURL().toString().startsWith("wss://");
            CaptureServletFilter filter = new CaptureServletFilter(
                    logger,
                    /*captureBody*/ true,
                    /*maxBodyBytes*/ 16_384,
                    java.nio.charset.StandardCharsets.UTF_8,
                    wsRoutes);
            byte[] body = "upgrade handshake".getBytes();
            HttpServletRequest req = httpRequest(
                    "GET", "wss://x/socket", null,
                    List.of("Connection", "Upgrade"),
                    java.util.Map.of(
                            "Connection", List.of("Upgrade"),
                            "Upgrade", List.of("websocket")),
                    body);
            // Deliberately DO NOT mark async — the bypass is the
            // entire point of the predicate opt-in.

            Throwable[] caught = new Throwable[]{null};
            int[] dataAvailableCalls = new int[]{0};
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                ServletInputStream in = ((HttpServletRequest) reqArg).getInputStream();
                try {
                    in.setReadListener(new jakarta.servlet.ReadListener() {
                        @Override
                        public void onDataAvailable() throws IOException {
                            dataAvailableCalls[0]++;
                            byte[] buf = new byte[64];
                            int n;
                            while ((n = in.read(buf)) != -1) out.write(buf, 0, n);
                        }
                        @Override public void onAllDataRead() {}
                        @Override public void onError(Throwable t) {}
                    });
                } catch (Throwable t) {
                    caught[0] = t;
                }
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertNull(caught[0],
                    "predicate-matched upgrade request must permit setReadListener; got " + caught[0]);
            assertEquals(1, dataAvailableCalls[0],
                    "onDataAvailable should fire once after registration");
            assertEquals("upgrade handshake", out.toString(),
                    "consumer must receive the exact buffered body via the listener");
        }
    }

    @Test
    void setReadListenerStillRejectedWhenUpgradePredicateDoesNotMatchTheRequest() throws Exception {
        // Critical scoping property: a filter instance configured
        // with an upgrade-route predicate must NOT bypass the
        // precondition for requests the predicate REJECTS, even
        // when the same filter handles upgrade-route requests
        // elsewhere. A wide filter mapping (e.g. /*) that happens
        // to cover both upgrade and ordinary routes is the most
        // common deployment shape; the predicate is what keeps the
        // bypass narrow.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            // Same predicate as the previous test: only matches wss://.
            java.util.function.Predicate<HttpServletRequest> wsRoutes =
                    req -> req.getRequestURL() != null
                            && req.getRequestURL().toString().startsWith("wss://");
            CaptureServletFilter filter = new CaptureServletFilter(
                    logger,
                    /*captureBody*/ true,
                    /*maxBodyBytes*/ 16_384,
                    java.nio.charset.StandardCharsets.UTF_8,
                    wsRoutes);
            // Ordinary HTTP route — predicate returns false — even
            // though the request carries upgrade-shaped headers a
            // malicious client could have sent.
            byte[] body = "ordinary POST".getBytes();
            HttpServletRequest req = httpRequest(
                    "POST", "https://x/api", null,
                    List.of("Connection", "Upgrade"),
                    java.util.Map.of(
                            "Connection", List.of("Upgrade"),
                            "Upgrade", List.of("websocket")),
                    body);
            // No startAsync() — synchronous handler.

            Throwable[] caught = new Throwable[]{null};
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                ServletInputStream in = ((HttpServletRequest) reqArg).getInputStream();
                try {
                    in.setReadListener(new jakarta.servlet.ReadListener() {
                        @Override public void onDataAvailable() {}
                        @Override public void onAllDataRead() {}
                        @Override public void onError(Throwable t) {}
                    });
                } catch (Throwable t) {
                    caught[0] = t;
                }
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertNotNull(caught[0],
                    "request that the predicate REJECTS must still hit the strict "
                            + "precondition — bypass is per-request, not per-filter-instance");
            assertTrue(caught[0] instanceof IllegalStateException,
                    "expected IllegalStateException; got " + caught[0]);
        }
    }

    @Test
    void upgradeRoutePredicateThatThrowsFailsClosedNotOpen() throws Exception {
        // Defensive contract: if the deployer-supplied predicate
        // throws while evaluating a request, the wrapper MUST fall
        // through to the strict async check, not silently bypass.
        // A buggy predicate fails CLOSED (strict-mode rejection),
        // never OPEN (silent acceptance). This keeps the worst-case
        // failure mode "lose async on the upgrade route" rather
        // than "silently let synchronous handlers register listeners".
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            java.util.function.Predicate<HttpServletRequest> broken = req -> {
                throw new RuntimeException("deployer bug — typo in path matcher");
            };
            CaptureServletFilter filter = new CaptureServletFilter(
                    logger, true, 16_384,
                    java.nio.charset.StandardCharsets.UTF_8, broken);
            byte[] body = "x".getBytes();
            HttpServletRequest req = httpRequest(
                    "GET", "wss://x", null,
                    List.of(), java.util.Map.of(), body);
            // No async — the predicate would normally be consulted,
            // but it throws.

            Throwable[] caught = new Throwable[]{null};
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                ServletInputStream in = ((HttpServletRequest) reqArg).getInputStream();
                try {
                    in.setReadListener(new jakarta.servlet.ReadListener() {
                        @Override public void onDataAvailable() {}
                        @Override public void onAllDataRead() {}
                        @Override public void onError(Throwable t) {}
                    });
                } catch (Throwable t) {
                    caught[0] = t;
                }
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertNotNull(caught[0],
                    "a thrown predicate must NOT silently bypass — strict check should apply");
            assertTrue(caught[0] instanceof IllegalStateException,
                    "fail-closed surfaces as the spec-standard IllegalStateException; got "
                            + caught[0]);
            // Also: the request must still proceed (the predicate
            // failure must not propagate as a 500 from the filter
            // chain). doFilter completing without throwing is the
            // evidence — caught[0] holds the listener-registration
            // error inside the chain, not a filter-level failure.
        }
    }

    @Test
    void setReadListenerRejectedEvenWithConnectionUpgradeHeaderWhenAsyncNotStarted() throws Exception {
        // Client-supplied `Connection: Upgrade` (and `Upgrade:
        // websocket`) headers MUST NOT bypass the spec precondition
        // for setReadListener. The headers are client-controlled —
        // any synchronous handler could receive them — and acceptance
        // of the upgrade is a SERVER-side decision that lives outside
        // the request-header signal. A previous heuristic trusted
        // these headers and would have silently accepted
        // setReadListener in a synchronous handler that happened to
        // receive an upgrade-shaped request, hiding the bug from the
        // developer and breaking against a real container in
        // production.
        //
        // Net: strict async-only precondition under the DEFAULT
        // filter configuration. The five-arg constructor exposes
        // an `upgradeRoutePredicate` opt-in that lets the deployer
        // identify which specific requests should bypass the check —
        // see setReadListenerAcceptedWhenUpgradeRoutePredicateMatchesTheRequest
        // for the matched-request path and
        // setReadListenerStillRejectedWhenUpgradePredicateDoesNotMatchTheRequest
        // for the scoping property. This test pins the bulk-of-routes
        // default (no predicate supplied → strict for every request).
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            byte[] body = "upgrade-shaped request".getBytes();
            // Both common forms in one matrix: single-line
            // comma-separated AND split across multiple Connection
            // header lines. Neither should change the outcome —
            // the headers don't determine upgrade-mode.
            HttpServletRequest req = httpRequest(
                    "GET", "wss://x", null,
                    List.of("Connection", "Upgrade"),
                    java.util.Map.of(
                            "Connection", List.of("keep-alive", "Upgrade"),
                            "Upgrade", List.of("websocket")),
                    body);
            // Deliberately DO NOT call markAsyncStarted(req) — the
            // request shape mimics an upgrade attempt, but the
            // container hasn't promoted it via startAsync(). That's
            // exactly the bug-catching scenario.

            Throwable[] caught = new Throwable[]{null};
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                ServletInputStream in = ((HttpServletRequest) reqArg).getInputStream();
                try {
                    in.setReadListener(new jakarta.servlet.ReadListener() {
                        @Override public void onDataAvailable() {}
                        @Override public void onAllDataRead() {}
                        @Override public void onError(Throwable t) {}
                    });
                } catch (Throwable t) {
                    caught[0] = t;
                }
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertNotNull(caught[0],
                    "client-supplied Connection: Upgrade headers must NOT bypass "
                            + "the async precondition — setReadListener should still throw");
            assertTrue(caught[0] instanceof IllegalStateException,
                    "expected IllegalStateException per spec; got " + caught[0]);
            assertTrue(caught[0].getMessage().toLowerCase().contains("async"),
                    "error message should mention the async precondition; got: "
                            + caught[0].getMessage());
        }
    }

    @Test
    void asyncReadListenerWorksOnTheBodyWrapperReplayStream() throws Exception {
        // Async endpoints register a ReadListener and expect the
        // container to call back when data is ready. The previous
        // implementation threw UnsupportedOperationException here,
        // which broke async endpoints whenever captureBody=true.
        //
        // Verify the new behaviour: ReadListener fires synchronously
        // and the consumer reads the full original body via the
        // listener callbacks.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            byte[] body = "async hello".getBytes();
            HttpServletRequest req = httpRequest(
                    "POST", "https://x", null,
                    List.of(), java.util.Map.of(), body);
            // Required by the Servlet spec before setReadListener.
            markAsyncStarted(req);

            // Downstream chain simulates an async endpoint: register
            // a ReadListener, accumulate bytes, count callbacks.
            byte[][] downstreamRead = new byte[1][];
            int[] dataAvailableCalls = new int[]{0};
            int[] allReadCalls = new int[]{0};
            Throwable[] onErrorSurfaced = new Throwable[]{null};

            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                ServletInputStream in = ((HttpServletRequest) reqArg).getInputStream();
                java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
                in.setReadListener(new jakarta.servlet.ReadListener() {
                    @Override
                    public void onDataAvailable() throws IOException {
                        dataAvailableCalls[0]++;
                        // Drain everything synchronously — same shape
                        // as a typical async handler that does its
                        // reading inside the listener callback.
                        byte[] buf = new byte[1024];
                        int n;
                        while ((n = in.read(buf)) != -1) {
                            out.write(buf, 0, n);
                        }
                    }
                    @Override
                    public void onAllDataRead() {
                        allReadCalls[0]++;
                        downstreamRead[0] = out.toByteArray();
                    }
                    @Override
                    public void onError(Throwable t) {
                        onErrorSurfaced[0] = t;
                    }
                });
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);

            assertEquals(1, dataAvailableCalls[0],
                    "onDataAvailable should fire exactly once for the buffered body");
            assertEquals(1, allReadCalls[0],
                    "onAllDataRead should fire after the consumer drains the buffer");
            assertNull(onErrorSurfaced[0],
                    "no error should be surfaced on a happy-path async read");
            assertEquals("async hello", new String(downstreamRead[0]),
                    "async consumer must receive the exact original body bytes");
        }
    }

    @Test
    void replayStreamRejectsNullReadListener() throws Exception {
        // Spec compliance — passing null to setReadListener is a
        // contract violation, not something we should swallow.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            byte[] body = "x".getBytes();
            HttpServletRequest req = httpRequest(
                    "POST", "https://x", null,
                    List.of(), java.util.Map.of(), body);
            markAsyncStarted(req);

            Throwable[] caught = new Throwable[]{null};
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                ServletInputStream in = ((HttpServletRequest) reqArg).getInputStream();
                try {
                    in.setReadListener(null);
                } catch (Throwable t) {
                    caught[0] = t;
                }
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertNotNull(caught[0], "null listener must trigger an exception");
            assertTrue(caught[0] instanceof NullPointerException,
                    "expected NPE; got " + caught[0]);
        }
    }

    @Test
    void getReaderAfterGetInputStreamThrowsIllegalStateException() throws Exception {
        // Servlet spec: getInputStream() and getReader() are
        // mutually exclusive per request. Calling getReader() AFTER
        // getInputStream() must throw IllegalStateException. Real
        // containers enforce this; the previous wrapper let mixed
        // access slip through silently — hiding bugs that would
        // blow up in production.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            byte[] body = "exclusivity".getBytes();
            HttpServletRequest req = httpRequest(
                    "POST", "https://x", null,
                    List.of(), java.util.Map.of(), body);

            Throwable[] caught = new Throwable[]{null};
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                HttpServletRequest wrapped = (HttpServletRequest) reqArg;
                // First: getInputStream — locks the wrapper into
                // STREAM access mode.
                wrapped.getInputStream();
                // Then: getReader — must throw.
                try {
                    wrapped.getReader();
                } catch (Throwable t) {
                    caught[0] = t;
                }
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertNotNull(caught[0], "getReader after getInputStream must throw");
            assertTrue(caught[0] instanceof IllegalStateException,
                    "expected IllegalStateException; got " + caught[0]);
            assertTrue(caught[0].getMessage().contains("getInputStream"),
                    "error message should name the conflicting prior call; got: "
                            + caught[0].getMessage());
        }
    }

    @Test
    void getInputStreamAfterGetReaderThrowsIllegalStateException() throws Exception {
        // Symmetric direction: getReader first locks into READER
        // mode, getInputStream() must throw.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            byte[] body = "exclusivity".getBytes();
            HttpServletRequest req = httpRequest(
                    "POST", "https://x", null,
                    List.of(), java.util.Map.of(), body);

            Throwable[] caught = new Throwable[]{null};
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                HttpServletRequest wrapped = (HttpServletRequest) reqArg;
                wrapped.getReader();
                try {
                    wrapped.getInputStream();
                } catch (Throwable t) {
                    caught[0] = t;
                }
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertNotNull(caught[0], "getInputStream after getReader must throw");
            assertTrue(caught[0] instanceof IllegalStateException,
                    "expected IllegalStateException; got " + caught[0]);
            assertTrue(caught[0].getMessage().contains("getReader"),
                    "error message should name the conflicting prior call; got: "
                            + caught[0].getMessage());
        }
    }

    @Test
    void multipleGetReaderCallsReturnSameInstancePreservingConsumption() throws Exception {
        // Real containers return the same Reader across calls so
        // partial consumption is preserved. Our cached reader does
        // too — verify a second getReader() doesn't reset position
        // to byte 0.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            byte[] body = "abcdefghij".getBytes();
            HttpServletRequest req = httpRequest(
                    "POST", "https://x", null,
                    List.of(), java.util.Map.of(), body);

            int[] firstChars = new int[5];
            int[] secondChars = new int[5];
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                HttpServletRequest wrapped = (HttpServletRequest) reqArg;
                java.io.BufferedReader r1 = wrapped.getReader();
                // Read 5 chars from the first reader handle.
                for (int i = 0; i < 5; i++) firstChars[i] = r1.read();
                // Obtain a SECOND reader reference — must be the
                // same instance, continuing from char 5.
                java.io.BufferedReader r2 = wrapped.getReader();
                assertEquals(System.identityHashCode(r1), System.identityHashCode(r2),
                        "getReader() must return the same cached instance");
                for (int i = 0; i < 5; i++) secondChars[i] = r2.read();
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            // First reader read "abcde", second continued with "fghij".
            assertEquals('a', firstChars[0]);
            assertEquals('e', firstChars[4]);
            assertEquals('f', secondChars[0]);
            assertEquals('j', secondChars[4]);
        }
    }

    @Test
    void getInputStreamReturnsSameInstanceSoSetReadListenerIsRequestScoped() throws Exception {
        // Per the Servlet spec, setReadListener is a per-REQUEST
        // single-shot. The previous implementation returned a fresh
        // ReplayServletInputStream on each getInputStream() call —
        // each had its own listenerSet flag — so a consumer could
        // sidestep the constraint by registering listeners against
        // two different stream instances obtained from the same
        // request:
        //   req.getInputStream().setReadListener(a);   // would succeed
        //   req.getInputStream().setReadListener(b);   // ALSO succeeded — bug
        //
        // The fix: getInputStream() returns the SAME stream
        // instance for the lifetime of the wrapped request, so the
        // listenerSet flag genuinely tracks request-scoped state.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            byte[] body = "x".getBytes();
            HttpServletRequest req = httpRequest(
                    "POST", "https://x", null,
                    List.of(), java.util.Map.of(), body);
            markAsyncStarted(req);

            ServletInputStream[] firstStream = new ServletInputStream[1];
            ServletInputStream[] secondStream = new ServletInputStream[1];
            Throwable[] caughtOnSecondRegistration = new Throwable[]{null};

            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                HttpServletRequest wrapped = (HttpServletRequest) reqArg;
                // Two separate calls — must yield the same stream
                // instance, NOT a fresh one each time.
                firstStream[0] = wrapped.getInputStream();
                secondStream[0] = wrapped.getInputStream();

                // Register on the first stream — succeeds.
                firstStream[0].setReadListener(new jakarta.servlet.ReadListener() {
                    @Override public void onDataAvailable() {}
                    @Override public void onAllDataRead() {}
                    @Override public void onError(Throwable t) {}
                });
                // Try to register on the SECOND stream (also from
                // getInputStream()). Under the bug this would
                // succeed silently. With the wrapper-scoped cache,
                // both references point at the same stream — the
                // listenerSet flag is already true — and the
                // second registration throws IllegalStateException.
                try {
                    secondStream[0].setReadListener(new jakarta.servlet.ReadListener() {
                        @Override public void onDataAvailable() {}
                        @Override public void onAllDataRead() {}
                        @Override public void onError(Throwable t) {}
                    });
                } catch (Throwable t) {
                    caughtOnSecondRegistration[0] = t;
                }
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);

            // Identity check: same instance from both calls.
            assertEquals(System.identityHashCode(firstStream[0]),
                    System.identityHashCode(secondStream[0]),
                    "getInputStream() must return the SAME stream instance on repeated calls");
            assertNotNull(caughtOnSecondRegistration[0],
                    "second setReadListener (via second getInputStream()) must still be rejected");
            assertTrue(caughtOnSecondRegistration[0] instanceof IllegalStateException,
                    "expected IllegalStateException; got "
                            + caughtOnSecondRegistration[0]);
        }
    }

    @Test
    void replayStreamRejectsSecondReadListenerEvenWhenFirstDidNotDrain() throws Exception {
        // Regression for the spec-violation bug: the spec forbids
        // multiple ReadListener registrations on the same stream,
        // regardless of whether the first listener drained to EOF.
        // The previous "allReadFired"-based check only rejected
        // re-registration after full drain, so a first listener
        // that ignored / partial-read the body would let a second
        // listener slip through.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            byte[] body = "plenty of bytes here".getBytes();
            HttpServletRequest req = httpRequest(
                    "POST", "https://x", null,
                    List.of(), java.util.Map.of(), body);
            markAsyncStarted(req);

            Throwable[] caught = new Throwable[]{null};
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                ServletInputStream in = ((HttpServletRequest) reqArg).getInputStream();
                // First listener: deliberately reads NOTHING (the
                // exact failure mode the previous code missed).
                in.setReadListener(new jakarta.servlet.ReadListener() {
                    @Override public void onDataAvailable() { /* deliberate no-op */ }
                    @Override public void onAllDataRead() {}
                    @Override public void onError(Throwable t) {}
                });
                // Stream still has unread bytes — under the old
                // check, listenerSet's predecessor (allReadFired)
                // stayed false and this second registration was
                // wrongly accepted.
                try {
                    in.setReadListener(new jakarta.servlet.ReadListener() {
                        @Override public void onDataAvailable() {}
                        @Override public void onAllDataRead() {}
                        @Override public void onError(Throwable t) {}
                    });
                } catch (Throwable t) {
                    caught[0] = t;
                }
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertNotNull(caught[0],
                    "second setReadListener must throw even when first listener didn't drain");
            assertTrue(caught[0] instanceof IllegalStateException,
                    "expected IllegalStateException; got " + caught[0]);
        }
    }

    @Test
    void readListenerOnErrorSurfacesIOExceptionFromConsumer() throws Exception {
        // The consumer's onDataAvailable may itself throw — the spec
        // requires the stream to fire onError() with the exception.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            byte[] body = "x".getBytes();
            HttpServletRequest req = httpRequest(
                    "POST", "https://x", null,
                    List.of(), java.util.Map.of(), body);
            markAsyncStarted(req);

            Throwable[] errored = new Throwable[]{null};
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                ServletInputStream in = ((HttpServletRequest) reqArg).getInputStream();
                in.setReadListener(new jakarta.servlet.ReadListener() {
                    @Override
                    public void onDataAvailable() throws IOException {
                        throw new IOException("consumer-side oops");
                    }
                    @Override public void onAllDataRead() {}
                    @Override public void onError(Throwable t) { errored[0] = t; }
                });
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertNotNull(errored[0], "consumer IOException must surface via onError");
            assertTrue(errored[0].getMessage().contains("consumer-side oops"));
        }
    }

    @Test
    void constructorRejectsNullLoggerAndNegativeBodyCap() {
        assertThrows(NullPointerException.class,
                () -> new CaptureServletFilter(null));
        // For the negative-body-cap test we need SOMETHING for the
        // logger argument — a real CaptureLogger is cheap (build +
        // immediate close after the assertion verifies the throw).
        try (CaptureLogger logger = CaptureLogger.builder()
                .sink(new ListSink())
                .build()) {
            assertThrows(IllegalArgumentException.class,
                    () -> new CaptureServletFilter(logger, true, -1));
        }
    }

    @Test
    void wrapperDefaultCharsetIsIso88591ToMatchServletSpec() throws Exception {
        // Servlet 6 §3.13: when getCharacterEncoding() is null,
        // the container decodes request body / reader / form
        // parameters using ISO-8859-1. The wrapper used to default
        // to UTF-8, which mojibake'd any high-bit byte the container
        // would have handed back verbatim under ISO-8859-1.
        //
        // Verification: build a body with the byte 0xE9 (Latin-1
        // 'é'; NOT a valid lone UTF-8 byte). Decoded as UTF-8 it
        // becomes the replacement char U+FFFD; decoded as
        // ISO-8859-1 it becomes 'é'. Reading through getReader()
        // must produce 'é'.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            byte[] body = new byte[]{(byte) 0xE9};   // ISO-8859-1 'é'
            HttpServletRequest req = httpRequest(
                    "POST", "https://x", null,
                    List.of(), java.util.Map.of(), body);
            // Critical: NO Content-Type → getCharacterEncoding null.
            when(req.getCharacterEncoding()).thenReturn(null);

            char[] readChar = new char[1];
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                HttpServletRequest wrapped = (HttpServletRequest) reqArg;
                java.io.BufferedReader r = wrapped.getReader();
                readChar[0] = (char) r.read();
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertEquals('é', readChar[0],
                    "ISO-8859-1 default must decode 0xE9 as 'é', not the UTF-8 replacement char");
        }
    }

    @Test
    void formUrlencodedBodyParametersAreVisibleViaGetParameter() throws Exception {
        // The filter pre-reads the request stream for capture,
        // which would otherwise leave the container's lazy
        // form-parameter parser with no bytes to read. The
        // wrapper's getParameter*() overrides re-parse the cached
        // body so form POSTs still see their parameters.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            // Form-encoded body: name=alex&role=admin
            byte[] body = "name=alex&role=admin".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            HttpServletRequest req = httpRequest(
                    "POST", "https://x/api", null,
                    List.of("Content-Type"),
                    java.util.Map.of("Content-Type",
                            List.of("application/x-www-form-urlencoded")),
                    body);

            String[] gotName = new String[1];
            String[] gotRole = new String[1];
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                HttpServletRequest wrapped = (HttpServletRequest) reqArg;
                gotName[0] = wrapped.getParameter("name");
                gotRole[0] = wrapped.getParameter("role");
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertEquals("alex", gotName[0],
                    "form-body parameter 'name' should be visible after capture");
            assertEquals("admin", gotRole[0],
                    "form-body parameter 'role' should be visible after capture");
        }
    }

    @Test
    void formParamsMergeQueryStringAndBody() throws Exception {
        // Both query-string and form-body params must appear in
        // getParameterMap() — that's how containers compose them.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            byte[] body = "role=admin".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            HttpServletRequest req = httpRequest(
                    "POST", "https://x/api", "name=alex&debug=1",
                    List.of("Content-Type"),
                    java.util.Map.of("Content-Type",
                            List.of("application/x-www-form-urlencoded")),
                    body);

            java.util.Map<String, String[]>[] got = new java.util.Map[1];
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                HttpServletRequest wrapped = (HttpServletRequest) reqArg;
                got[0] = wrapped.getParameterMap();
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertEquals("alex", got[0].get("name")[0],
                    "query-string param 'name' must appear in the merged map");
            assertEquals("1", got[0].get("debug")[0],
                    "query-string param 'debug' must appear");
            assertEquals("admin", got[0].get("role")[0],
                    "form-body param 'role' must appear");
        }
    }

    @Test
    void queryStringDecodedAsUtf8EvenWhenBodyCharsetIsLatin1() throws Exception {
        // Regression: query strings are part of the URI and modern
        // containers decode them as UTF-8 (Tomcat 8+ default). The
        // previous code decoded query params with the BODY charset,
        // which now defaults to ISO-8859-1 — that would mojibake
        // UTF-8 query values (e.g. %C3%A9 → "Ã©" instead of "é").
        //
        // Setup: POST form, body charset null (→ ISO-8859-1 default),
        // query string contains UTF-8 percent-encoded "é". The
        // wrapper MUST still decode the query value correctly.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            byte[] body = "x=1".getBytes(java.nio.charset.StandardCharsets.ISO_8859_1);
            // Query: name=%C3%A9 — UTF-8 encoding of "é".
            HttpServletRequest req = httpRequest(
                    "POST", "https://x/api", "name=%C3%A9",
                    List.of("Content-Type"),
                    java.util.Map.of("Content-Type",
                            List.of("application/x-www-form-urlencoded")),
                    body);
            when(req.getCharacterEncoding()).thenReturn(null);  // → ISO-8859-1 body charset

            String[] got = new String[1];
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                HttpServletRequest wrapped = (HttpServletRequest) reqArg;
                got[0] = wrapped.getParameter("name");
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertEquals("é", got[0],
                    "query string must decode as UTF-8 regardless of body charset; got " + got[0]);
        }
    }

    @Test
    void queryCharsetIsConfigurableForNonUtf8ContainerDeployments() throws Exception {
        // The default queryCharset is UTF-8 (modern container norm),
        // but deployments running a container with a non-UTF-8
        // URIEncoding need the wrapper to match their container's
        // own decoding. Tomcat's legacy default was ISO-8859-1; the
        // four-arg constructor lets a consumer configure that.
        //
        // Setup: query string contains the byte 0xE9 percent-encoded
        // (%E9). Decoded as UTF-8 that's an invalid lone byte (→ U+FFFD
        // replacement char). Decoded as ISO-8859-1 it's 'é'. The
        // configured charset must drive the outcome.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(
                    logger, true, 16_384, java.nio.charset.StandardCharsets.ISO_8859_1);
            byte[] body = "x=1".getBytes(java.nio.charset.StandardCharsets.ISO_8859_1);
            HttpServletRequest req = httpRequest(
                    "POST", "https://x/api", "name=%E9",
                    List.of("Content-Type"),
                    java.util.Map.of("Content-Type",
                            List.of("application/x-www-form-urlencoded")),
                    body);

            String[] got = new String[1];
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                got[0] = ((HttpServletRequest) reqArg).getParameter("name");
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertEquals("é", got[0],
                    "ISO-8859-1 queryCharset override must decode %E9 as 'é'; got " + got[0]);
        }
    }

    @Test
    void constructorRejectsNullQueryCharset() {
        // queryCharset is a required argument on the four-arg form —
        // a null would NPE on first parameter decode, much later
        // than the constructor. Fail fast instead.
        try (CaptureLogger logger = CaptureLogger.builder()
                .sink(new ListSink())
                .build()) {
            assertThrows(NullPointerException.class,
                    () -> new CaptureServletFilter(logger, true, 16_384, null));
        }
    }

    @Test
    void malformedPercentEncodingInQueryFallsBackToRawWhenLenientModeOptedIn() throws Exception {
        // Lenient mode: URLDecoder failures fall back to the raw
        // encoded substring rather than throwing. Useful for
        // shadow-traffic capture where the filter must not perturb
        // request error semantics regardless of payload validity.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            // Seven-arg constructor: explicit opt-in to lenient mode.
            CaptureServletFilter filter = new CaptureServletFilter(
                    logger, true, 16_384,
                    java.nio.charset.StandardCharsets.UTF_8,
                    /*upgradeRoutePredicate*/ null,
                    CaptureServletFilter.DEFAULT_PARSED_BODY_METHODS,
                    /*lenientParameterDecoding*/ true);
            byte[] body = "valid=1".getBytes();
            HttpServletRequest req = httpRequest(
                    "POST", "https://x/api", "broken=%ZZ&ok=clean",
                    List.of("Content-Type"),
                    java.util.Map.of("Content-Type",
                            List.of("application/x-www-form-urlencoded")),
                    body);

            String[] gotBroken = new String[1];
            String[] gotOk = new String[1];
            String[] gotValid = new String[1];
            Throwable[] caught = new Throwable[]{null};
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                try {
                    HttpServletRequest wrapped = (HttpServletRequest) reqArg;
                    gotBroken[0] = wrapped.getParameter("broken");
                    gotOk[0] = wrapped.getParameter("ok");
                    gotValid[0] = wrapped.getParameter("valid");
                } catch (Throwable t) {
                    caught[0] = t;
                }
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertNull(caught[0],
                    "lenient mode must not throw on malformed percent-encoding; got " + caught[0]);
            // Malformed value is preserved in its raw encoded form
            // — downstream sees something rather than nothing, but
            // it's clearly un-decoded.
            assertEquals("%ZZ", gotBroken[0],
                    "lenient mode should fall back to raw encoded form; got " + gotBroken[0]);
            // Adjacent well-formed pairs still parse correctly —
            // one bad pair doesn't poison the rest.
            assertEquals("clean", gotOk[0]);
            assertEquals("1", gotValid[0]);
        }
    }

    @Test
    void malformedPercentEncodingInBodyFallsBackToRawWhenLenientModeOptedIn() throws Exception {
        // Symmetric coverage for the body parsing path. Lenient
        // opt-in lets capture proceed without altering downstream
        // error semantics on malformed input.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(
                    logger, true, 16_384,
                    java.nio.charset.StandardCharsets.UTF_8,
                    /*upgradeRoutePredicate*/ null,
                    CaptureServletFilter.DEFAULT_PARSED_BODY_METHODS,
                    /*lenientParameterDecoding*/ true);
            byte[] body = "broken=%XY&ok=clean".getBytes();
            HttpServletRequest req = httpRequest(
                    "POST", "https://x/api", null,
                    List.of("Content-Type"),
                    java.util.Map.of("Content-Type",
                            List.of("application/x-www-form-urlencoded")),
                    body);

            Throwable[] caught = new Throwable[]{null};
            String[] gotOk = new String[1];
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                try {
                    gotOk[0] = ((HttpServletRequest) reqArg).getParameter("ok");
                } catch (Throwable t) {
                    caught[0] = t;
                }
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertNull(caught[0],
                    "lenient mode must not throw on malformed body encoding; got " + caught[0]);
            assertEquals("clean", gotOk[0]);
        }
    }

    @Test
    void malformedPercentEncodingThrowsIllegalStateExceptionPerSpecInDefaultStrictMode() throws Exception {
        // Strict (default) mode: malformed percent-escapes surface
        // as IllegalStateException from getParameter*(), matching
        // Servlet 6.1's specified failure type for parameter-parse
        // errors. Downstream frameworks (Spring exception resolvers,
        // Jakarta REST mappers) key on this spec-standard type to
        // translate consistently to HTTP 400 — the same outcome a
        // real container's form parser would produce on the same
        // input. The underlying URLDecoder IAE is preserved as the
        // cause so debuggers / logs still see the original failure.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            byte[] body = "broken=%ZZ".getBytes();
            HttpServletRequest req = httpRequest(
                    "POST", "https://x/api", null,
                    List.of("Content-Type"),
                    java.util.Map.of("Content-Type",
                            List.of("application/x-www-form-urlencoded")),
                    body);

            Throwable[] caught = new Throwable[]{null};
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                try {
                    ((HttpServletRequest) reqArg).getParameter("broken");
                } catch (Throwable t) {
                    caught[0] = t;
                }
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertNotNull(caught[0],
                    "strict (default) mode must propagate malformed-encoding failures");
            assertTrue(caught[0] instanceof IllegalStateException,
                    "Servlet 6.1 specifies IllegalStateException from getParameter*() "
                            + "on parameter-parse failures; got " + caught[0]);
            // Underlying cause preserved — debug/logging chains can
            // still see the URLDecoder failure message.
            assertNotNull(caught[0].getCause(),
                    "wrapped ISE should carry the underlying decoder IAE as its cause");
            assertTrue(caught[0].getCause() instanceof IllegalArgumentException,
                    "cause should be the URLDecoder IllegalArgumentException; got "
                            + caught[0].getCause());
        }
    }

    @Test
    void putWithFormUrlencodedBodyIsParsedWhenParsedBodyMethodsIncludesPut() throws Exception {
        // Default parsedBodyMethods is {POST} — strict Servlet
        // semantics. But some deployments configure their container
        // to parse form bodies on additional methods (Tomcat's
        // parseBodyMethods=POST,PUT). In that case the wrapper
        // MUST also parse form bodies on PUT, otherwise it consumes
        // the stream during pre-read AND returns no params,
        // diverging from container behaviour. Verification: with
        // parsedBodyMethods={POST,PUT}, a form-encoded PUT body
        // is parsed exactly like a POST.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(
                    logger, true, 16_384,
                    java.nio.charset.StandardCharsets.UTF_8,
                    /*upgradeRoutePredicate*/ null,
                    java.util.Set.of("POST", "PUT"),
                    /*lenientParameterDecoding*/ false);
            byte[] body = "role=admin".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            HttpServletRequest req = httpRequest(
                    "PUT", "https://x/api", null,
                    List.of("Content-Type"),
                    java.util.Map.of("Content-Type",
                            List.of("application/x-www-form-urlencoded")),
                    body);

            String[] got = new String[1];
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                got[0] = ((HttpServletRequest) reqArg).getParameter("role");
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertEquals("admin", got[0],
                    "wrapper must parse PUT body when parsedBodyMethods includes PUT");
        }
    }

    @Test
    void parsedBodyMethodsMatchingIsCaseInsensitive() throws Exception {
        // HTTP methods are case-sensitive on the wire (the IETF
        // registry mandates uppercase), but clients send variations
        // in practice and the Servlet API returns the method as
        // sent. The wrapper normalises both the configured set and
        // the request method at match time so a lowercase "post"
        // request is treated identically to "POST" — matching what
        // real containers do.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            // Lower-case configuration on purpose — the wrapper
            // uppercases it at construction.
            CaptureServletFilter filter = new CaptureServletFilter(
                    logger, true, 16_384,
                    java.nio.charset.StandardCharsets.UTF_8,
                    null,
                    java.util.Set.of("post", "patch"),
                    false);
            byte[] body = "x=1".getBytes();
            // Lower-case method on the request side too.
            HttpServletRequest req = httpRequest(
                    "patch", "https://x/api", null,
                    List.of("Content-Type"),
                    java.util.Map.of("Content-Type",
                            List.of("application/x-www-form-urlencoded")),
                    body);

            String[] got = new String[1];
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                got[0] = ((HttpServletRequest) reqArg).getParameter("x");
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertEquals("1", got[0],
                    "method matching must be case-insensitive on both config and request sides");
        }
    }

    @Test
    void constructorRejectsNullParsedBodyMethods() {
        try (CaptureLogger logger = CaptureLogger.builder()
                .sink(new ListSink())
                .build()) {
            assertThrows(NullPointerException.class,
                    () -> new CaptureServletFilter(
                            logger, true, 16_384,
                            java.nio.charset.StandardCharsets.UTF_8,
                            null,
                            /*parsedBodyMethods*/ null,
                            false));
        }
    }

    @Test
    void constructorRejectsNullEntryInParsedBodyMethods() {
        try (CaptureLogger logger = CaptureLogger.builder()
                .sink(new ListSink())
                .build()) {
            // HashSet permits null elements; reject explicitly so
            // a null entry doesn't NPE later inside the wrapper's
            // per-request match loop.
            java.util.Set<String> withNull = new java.util.HashSet<>();
            withNull.add("POST");
            withNull.add(null);
            assertThrows(IllegalArgumentException.class,
                    () -> new CaptureServletFilter(
                            logger, true, 16_384,
                            java.nio.charset.StandardCharsets.UTF_8,
                            null, withNull, false));
        }
    }

    @Test
    void parsedBodyMethodEntriesAreTrimmedSoCommaSplitConfigStringsWork() throws Exception {
        // Real-world config flows pass comma-separated method
        // strings: "POST,PUT".split(",") yields ["POST", "PUT"]
        // (no whitespace). But "POST, PUT".split(",") yields
        // ["POST", " PUT"] — that leading space, left untouched,
        // would uppercase to " PUT" and never match request.getMethod()
        // returning "PUT". The wrapper trims at construction time
        // so both forms work identically.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            // Mimic split(",") output WITH ambient whitespace.
            java.util.Set<String> fromCommaSplit = java.util.Set.of("POST", " PUT", "PATCH ");
            CaptureServletFilter filter = new CaptureServletFilter(
                    logger, true, 16_384,
                    java.nio.charset.StandardCharsets.UTF_8,
                    null, fromCommaSplit, false);
            byte[] body = "role=admin".getBytes();
            HttpServletRequest req = httpRequest(
                    "PUT", "https://x/api", null,
                    List.of("Content-Type"),
                    java.util.Map.of("Content-Type",
                            List.of("application/x-www-form-urlencoded")),
                    body);

            String[] got = new String[1];
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                got[0] = ((HttpServletRequest) reqArg).getParameter("role");
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertEquals("admin", got[0],
                    "trimmed config entry ' PUT' should match request method 'PUT'");
        }
    }

    @Test
    void constructorRejectsEmptyParsedBodyMethods() {
        // An empty set + captureBody=true is the silent-failure
        // mode: body consumed by pre-read, never intercepted by
        // getParameter*(), every form param dropped. Fail loudly
        // at construction so the deployer catches this BEFORE
        // running traffic through a broken config. If the intent
        // is "no body capture", the dedicated knob is
        // captureBody=false (which also skips the body-buffering
        // allocation per request).
        try (CaptureLogger logger = CaptureLogger.builder()
                .sink(new ListSink())
                .build()) {
            assertThrows(IllegalArgumentException.class,
                    () -> new CaptureServletFilter(
                            logger, true, 16_384,
                            java.nio.charset.StandardCharsets.UTF_8,
                            null, java.util.Set.of(), false),
                    "empty parsedBodyMethods must be rejected — silently disabling "
                            + "every getParameter*() interception is a config bug");
        }
    }

    @Test
    void formUrlencodedContentTypeMatchedExactlyNotByPrefix() throws Exception {
        // Regression: previous implementation used
        //   lowercase(contentType).startsWith("application/x-www-form-urlencoded")
        // which falsely matched non-equivalent media types whose
        // names happen to share that prefix. The most plausible
        // real-world hit is "application/x-www-form-urlencoded-json"
        // (an extension some APIs use to mean "JSON body whose
        // shape mirrors form-encoded keys") — a different media
        // type the container would NOT feed to its form parser.
        //
        // Token-level equality on the media type fixes this. The
        // wrapper must NOT intercept getParameter*() for the
        // imposter type; super delegation should return null for
        // body keys (the container wouldn't parse them).
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            byte[] body = "role=admin".getBytes();
            HttpServletRequest req = httpRequest(
                    "POST", "https://x/api", null,
                    List.of("Content-Type"),
                    java.util.Map.of("Content-Type",
                            // Imposter: prefix matches form-urlencoded
                            // but the type is DIFFERENT (suffix -json).
                            List.of("application/x-www-form-urlencoded-json")),
                    body);
            // Container would NOT parse the body for this type, so
            // its getParameter would return null for body keys.
            when(req.getParameter("role")).thenReturn(null);

            String[] got = new String[1];
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                got[0] = ((HttpServletRequest) reqArg).getParameter("role");
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertNull(got[0],
                    "imposter media type sharing the form-urlencoded prefix must NOT "
                            + "trigger wrapper form-param parsing; got '" + got[0] + "'");
        }
    }

    @Test
    void formUrlencodedContentTypeWithCharsetParamStillMatches() throws Exception {
        // The fix that excludes "application/x-www-form-urlencoded-json"
        // must NOT also exclude the legitimate form Content-Type with
        // a charset parameter. Token-level matching strips ";" params
        // first, then compares the bare media type.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            byte[] body = "role=admin".getBytes();
            HttpServletRequest req = httpRequest(
                    "POST", "https://x/api", null,
                    List.of("Content-Type"),
                    java.util.Map.of("Content-Type",
                            List.of("application/x-www-form-urlencoded; charset=UTF-8")),
                    body);

            String[] got = new String[1];
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                got[0] = ((HttpServletRequest) reqArg).getParameter("role");
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertEquals("admin", got[0],
                    "form-urlencoded with charset param should still be intercepted");
        }
    }

    @Test
    void formUrlencodedContentTypeMatchedCaseInsensitively() throws Exception {
        // RFC 9110: media types are case-insensitive. Real clients
        // sometimes send uppercase or mixed-case. The wrapper's
        // exact-equality check must match case-insensitively to
        // keep parity with what the container would do.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            byte[] body = "k=v".getBytes();
            HttpServletRequest req = httpRequest(
                    "POST", "https://x/api", null,
                    List.of("Content-Type"),
                    java.util.Map.of("Content-Type",
                            List.of("Application/X-WWW-Form-Urlencoded")),
                    body);

            String[] got = new String[1];
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                got[0] = ((HttpServletRequest) reqArg).getParameter("k");
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertEquals("v", got[0],
                    "case-insensitive media-type match must accept mixed-case header");
        }
    }

    @Test
    void constructorRejectsBlankEntryInParsedBodyMethods() {
        // Empty-string and whitespace-only entries can sneak in
        // through buggy config parsing ("POST,,PUT" → ["POST", "",
        // "PUT"]; or "POST,  ,PUT" → ["POST", "  ", "PUT"]).
        // Fail loudly at construction so the deployer catches the
        // typo before deployment, rather than silently mismatching
        // every request later.
        try (CaptureLogger logger = CaptureLogger.builder()
                .sink(new ListSink())
                .build()) {
            assertThrows(IllegalArgumentException.class,
                    () -> new CaptureServletFilter(
                            logger, true, 16_384,
                            java.nio.charset.StandardCharsets.UTF_8,
                            null, java.util.Set.of("POST", ""), false),
                    "empty-string method entry must be rejected");
            assertThrows(IllegalArgumentException.class,
                    () -> new CaptureServletFilter(
                            logger, true, 16_384,
                            java.nio.charset.StandardCharsets.UTF_8,
                            null, java.util.Set.of("POST", "   "), false),
                    "whitespace-only method entry must be rejected");
        }
    }

    @Test
    void putWithFormUrlencodedBodyDoesNotTriggerFormParamInterception() throws Exception {
        // Servlet spec: form-body parameter parsing applies to POST
        // only — PUT/PATCH/DELETE/GET with form-encoded bodies do
        // NOT auto-populate getParameter*(). Previously the
        // wrapper's shouldIntercept() ignored method and would
        // surface phantom parameters on PUT requests that a real
        // container would never produce.
        //
        // Verification: PUT request with form-encoded body. Stub
        // super.getParameter(...) to return null for body keys
        // (container would NOT parse them on PUT). Confirm the
        // wrapper delegates rather than parsing the body itself.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            byte[] body = "role=admin".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            HttpServletRequest req = httpRequest(
                    "PUT", "https://x/api", null,
                    List.of("Content-Type"),
                    java.util.Map.of("Content-Type",
                            List.of("application/x-www-form-urlencoded")),
                    body);
            // Container behaviour: PUT body NOT parsed, so super
            // returns null for body keys.
            when(req.getParameter("role")).thenReturn(null);

            String[] got = new String[1];
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                HttpServletRequest wrapped = (HttpServletRequest) reqArg;
                got[0] = wrapped.getParameter("role");
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertNull(got[0],
                    "PUT request with form-encoded body must NOT have body params parsed; "
                            + "got '" + got[0] + "' — wrapper diverged from container semantics");
        }
    }

    @Test
    void jsonPostBodyDoesNotTriggerFormParamInterception() throws Exception {
        // For non-form content types the wrapper must NOT intercept
        // getParameter*() — it should delegate to super so the
        // container's own parsing (which only looks at the query
        // string for non-form bodies) returns the correct view.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            byte[] body = "{\"role\":\"admin\"}".getBytes();
            HttpServletRequest req = httpRequest(
                    "POST", "https://x/api", "name=alex",
                    List.of("Content-Type"),
                    java.util.Map.of("Content-Type", List.of("application/json")),
                    body);
            // Stub super.getParameter for the query-string case so
            // we can prove delegation occurred (rather than our
            // own parser running).
            when(req.getParameter("name")).thenReturn("alex");
            // And verify we DO NOT parse the JSON body as form data.
            when(req.getParameter("role")).thenReturn(null);

            String[] gotName = new String[1];
            String[] gotRole = new String[1];
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                HttpServletRequest wrapped = (HttpServletRequest) reqArg;
                gotName[0] = wrapped.getParameter("name");
                gotRole[0] = wrapped.getParameter("role");
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertEquals("alex", gotName[0],
                    "query-string 'name' must be visible via super-delegation");
            assertNull(gotRole[0],
                    "JSON body must NOT be parsed as form data; 'role' must not appear");
        }
    }

    @Test
    void bodyReadFailureDuringCaptureFallsBackToHeadersOnly() throws Exception {
        // The body is unreadable (corrupted upload, network reset
        // during chunked read). The filter must (a) not propagate
        // the failure and (b) still log headers + URL. CRUCIALLY:
        // the forwarded request must NOT be the original — its
        // input stream may have been advanced past whatever bytes
        // we did read before the failure, so handing the original
        // to downstream would expose a truncated/corrupted stream.
        // The wrapper stays in place and exposes (possibly empty)
        // cached bytes via its replay surface instead.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            HttpServletRequest req = mock(HttpServletRequest.class);
            when(req.getMethod()).thenReturn("POST");
            when(req.getRequestURL()).thenReturn(new StringBuffer("https://x"));
            when(req.getContentLengthLong()).thenReturn(50L);   // claims body
            when(req.getCharacterEncoding()).thenReturn("UTF-8");
            when(req.getHeaderNames()).thenReturn(Collections.enumeration(List.of("Content-Type")));
            when(req.getHeaders("Content-Type")).thenReturn(Collections.enumeration(List.of("application/json")));
            when(req.getContentType()).thenReturn("application/json");
            // ...but reading the body explodes. Hand-rolled rather
            // than mocked because Mockito can't mock the abstract
            // ServletInputStream (jakarta's Servlet 6 version has
            // package-private internals that Mockito's bytecode
            // instrumentation can't see).
            ServletInputStream broken = new ServletInputStream() {
                @Override public int read() throws IOException {
                    throw new IOException("network reset");
                }
                @Override public int read(byte[] b, int off, int len) throws IOException {
                    throw new IOException("network reset");
                }
                @Override public boolean isFinished() { return false; }
                @Override public boolean isReady() { return true; }
                @Override public void setReadListener(jakarta.servlet.ReadListener l) {}
            };
            when(req.getInputStream()).thenReturn(broken);

            // Spy the forwarded request so we can confirm the
            // wrapper-not-original property.
            ServletRequest[] forwarded = new ServletRequest[1];
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                forwarded[0] = reqArg;
            };
            filter.doFilter(req, mock(HttpServletResponse.class), chain);

            assertNotNull(forwarded[0],
                    "chain.doFilter must still be called after a body-read failure");
            assertNotEquals(System.identityHashCode(req),
                    System.identityHashCode(forwarded[0]),
                    "forwarded request must NOT be the original — its stream may "
                            + "have been advanced past the failure point; downstream "
                            + "must see the wrapper's cached replay instead");
        }
        // Logger should still have seen the request — minus body.
        assertEquals(1, sink.lines().size());
        String line = sink.lines().get(0);
        assertTrue(line.contains("\"Content-Type\":\"application/json\""),
                "headers should still be captured on body-read failure: " + line);
        assertFalse(line.contains("\"body\""),
                "no body field when body read fails: " + line);
    }

    /**
     * Build a request whose underlying input stream returns the
     * first {@code prefix.length} bytes successfully, then throws
     * {@link IOException} on the next read. Used by the failure-
     * surfacing tests to simulate a truncated upload / network
     * reset midway through the body.
     */
    private static HttpServletRequest httpRequestWithFlakeyStream(byte[] prefix, String contentType,
                                                                    String failureMessage)
            throws IOException {
        HttpServletRequest req = mock(HttpServletRequest.class);
        when(req.getMethod()).thenReturn("POST");
        when(req.getRequestURL()).thenReturn(new StringBuffer("https://x"));
        when(req.getContentLengthLong()).thenReturn(100L);
        when(req.getCharacterEncoding()).thenReturn("UTF-8");
        when(req.getHeaderNames()).thenReturn(Collections.emptyEnumeration());
        when(req.getContentType()).thenReturn(contentType);
        when(req.isAsyncStarted()).thenReturn(false);
        ServletInputStream flakey = new ServletInputStream() {
            int idx = 0;
            boolean done = false;
            @Override public int read() throws IOException {
                if (done) throw new IOException(failureMessage);
                if (idx >= prefix.length) {
                    done = true;
                    throw new IOException(failureMessage);
                }
                return prefix[idx++] & 0xff;
            }
            @Override public int read(byte[] b, int off, int len) throws IOException {
                if (done) throw new IOException(failureMessage);
                if (idx >= prefix.length) {
                    done = true;
                    throw new IOException(failureMessage);
                }
                int n = Math.min(len, prefix.length - idx);
                System.arraycopy(prefix, idx, b, off, n);
                idx += n;
                return n;
            }
            @Override public boolean isFinished() { return done; }
            @Override public boolean isReady() { return true; }
            @Override public void setReadListener(jakarta.servlet.ReadListener l) {}
        };
        when(req.getInputStream()).thenReturn(flakey);
        return req;
    }

    @Test
    void partialBodyReadDeliversPrefixThenSurfacesIOExceptionAtFailureBoundary() throws Exception {
        // CRITICAL safety property: a partial-then-failed pre-read
        // must NOT be masked as a clean EOF for downstream code.
        // If it were, a truncated JSON / form body would parse as
        // valid (e.g. `{"a":1}` extracted from a cut-off
        // `{"a":1,"b":2}` and processed as a complete request).
        // Real containers re-raise at the failure boundary; the
        // wrapper does the same: partial bytes delivered, THEN
        // IOException with the original failure as cause.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            HttpServletRequest req = httpRequestWithFlakeyStream(
                    "hello".getBytes(), "application/octet-stream", "network reset");

            byte[][] downstreamRead = new byte[1][];
            IOException[] surfaced = new IOException[]{null};
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                ServletInputStream in = ((HttpServletRequest) reqArg).getInputStream();
                java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
                byte[] buf = new byte[64];
                try {
                    int n;
                    while ((n = in.read(buf)) != -1) out.write(buf, 0, n);
                } catch (IOException ioe) {
                    surfaced[0] = ioe;
                }
                downstreamRead[0] = out.toByteArray();
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);

            assertEquals("hello", new String(downstreamRead[0]),
                    "partial bytes that arrived before the failure must be readable "
                            + "by downstream via the replay surface");
            assertNotNull(surfaced[0],
                    "I/O failure MUST surface to downstream as IOException — never "
                            + "be masked as a clean EOF. Without this, a truncated "
                            + "JSON body could parse as valid.");
            assertNotNull(surfaced[0].getCause(),
                    "surfaced IOException should chain the original failure as its cause");
            assertEquals("network reset", surfaced[0].getCause().getMessage(),
                    "cause should preserve the original failure message; got "
                            + surfaced[0].getCause().getMessage());
        }
        String line = sink.lines().get(0);
        assertFalse(line.contains("\"body\""),
                "no body field when body read fails partway: " + line);
    }

    @Test
    void partialBodyReadFailureAlsoSurfacesViaGetReaderPath() throws Exception {
        // Symmetric coverage: consumers using the character path
        // (BufferedReader) also need the failure signal, not a
        // false EOF. The reader is backed by the same failure-
        // surfacing InputStream, so reading past the prefix must
        // throw IOException from BufferedReader operations.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            HttpServletRequest req = httpRequestWithFlakeyStream(
                    "abc".getBytes(), "text/plain; charset=UTF-8", "stream truncated");

            char[] readChars = new char[3];
            IOException[] surfaced = new IOException[]{null};
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                java.io.BufferedReader r = ((HttpServletRequest) reqArg).getReader();
                try {
                    int total = 0;
                    while (total < 3) {
                        int n = r.read(readChars, total, 3 - total);
                        if (n == -1) break;
                        total += n;
                    }
                    // Force a read PAST the prefix — this should
                    // throw, NOT return -1.
                    r.read();
                } catch (IOException ioe) {
                    surfaced[0] = ioe;
                }
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);

            assertEquals("abc", new String(readChars),
                    "the 3-byte prefix must be readable through the reader");
            assertNotNull(surfaced[0],
                    "reader-path consumer must also see the I/O failure — not "
                            + "a clean -1 EOF from BufferedReader.read()");
        }
    }

    @Test
    void partialBodyReadFailureFiresOnErrorNotOnAllDataReadForAsyncListener() throws Exception {
        // Async-listener path equivalent: a consumer that drains
        // the partial bytes through onDataAvailable callbacks must
        // see onError at the end, NOT onAllDataRead. Without this
        // an async endpoint would treat a truncated body as fully-
        // received and proceed to parse it as valid.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            HttpServletRequest req = httpRequestWithFlakeyStream(
                    "hi".getBytes(), "application/octet-stream", "reset");
            markAsyncStarted(req);

            int[] dataAvailableCalls = new int[]{0};
            int[] allReadCalls = new int[]{0};
            Throwable[] errorSurfaced = new Throwable[]{null};
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                ServletInputStream in = ((HttpServletRequest) reqArg).getInputStream();
                in.setReadListener(new jakarta.servlet.ReadListener() {
                    @Override public void onDataAvailable() throws IOException {
                        dataAvailableCalls[0]++;
                        // Read just one byte per callback so we
                        // drain the partial WITHOUT hitting the
                        // boundary inside the listener — the
                        // post-drain branch must still surface
                        // the failure via onError.
                        in.read();
                    }
                    @Override public void onAllDataRead() { allReadCalls[0]++; }
                    @Override public void onError(Throwable t) { errorSurfaced[0] = t; }
                });
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);

            assertEquals(2, dataAvailableCalls[0],
                    "consumer should be re-invoked once per pending byte (2 bytes here)");
            assertEquals(0, allReadCalls[0],
                    "onAllDataRead must NOT fire on a failed pre-read; failure was "
                            + "silently treated as 'all data read' before the fix");
            assertNotNull(errorSurfaced[0],
                    "onError MUST fire to surface the I/O failure to the async consumer");
            assertTrue(errorSurfaced[0] instanceof IOException,
                    "onError should receive an IOException; got " + errorSurfaced[0]);
        }
    }

    @Test
    void getParameterAccessThrowsIllegalStateOnPartialBodyReadFailure() throws Exception {
        // Critical parity with the stream/reader paths: form-param
        // callers MUST also see the I/O failure, not silently
        // receive truncated params. Without this guard, a
        // pre-read failure during a form POST would let
        // getParameter() return a phantom last-key/value (or
        // miss it entirely) and the caller would have no way
        // to tell the body was truncated. Servlet 6.1 specifies
        // IllegalStateException for getParameter*() failures —
        // we use the same shape (cause-preserved) the strict
        // malformed-encoding path uses.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            HttpServletRequest req = httpRequestWithFlakeyStream(
                    // Partial form pair — would parse as
                    // {"name": "alex"} if we silently let it
                    // through, but the actual body was cut off
                    // mid-stream. Caller must observe the failure.
                    "name=alex".getBytes(),
                    "application/x-www-form-urlencoded",
                    "network reset");

            Throwable[] caught = new Throwable[]{null};
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                try {
                    ((HttpServletRequest) reqArg).getParameter("name");
                } catch (Throwable t) {
                    caught[0] = t;
                }
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertNotNull(caught[0],
                    "getParameter MUST surface the pre-read failure — not silently "
                            + "return a partial value");
            assertTrue(caught[0] instanceof IllegalStateException,
                    "Servlet 6.1 specifies IllegalStateException for getParameter*() "
                            + "failures; got " + caught[0]);
            assertNotNull(caught[0].getCause(),
                    "wrapped ISE should chain the original I/O failure as cause");
            assertTrue(caught[0].getCause() instanceof IOException,
                    "cause should be the original IOException; got " + caught[0].getCause());
            assertEquals("network reset", caught[0].getCause().getMessage(),
                    "underlying failure message preserved");
        }
    }

    @Test
    void getParameterMapAccessThrowsIllegalStateOnPartialBodyReadFailure() throws Exception {
        // Same guard applies to the map-of-arrays surface — a
        // caller using getParameterMap() must not receive a
        // partial map silently.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            HttpServletRequest req = httpRequestWithFlakeyStream(
                    "k=v".getBytes(),
                    "application/x-www-form-urlencoded",
                    "reset");

            Throwable[] caught = new Throwable[]{null};
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                try {
                    ((HttpServletRequest) reqArg).getParameterMap();
                } catch (Throwable t) {
                    caught[0] = t;
                }
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertNotNull(caught[0],
                    "getParameterMap MUST surface the pre-read failure");
            assertTrue(caught[0] instanceof IllegalStateException,
                    "expected ISE per Servlet 6.1; got " + caught[0]);
            assertTrue(caught[0].getCause() instanceof IOException,
                    "cause should be the original IOException; got " + caught[0].getCause());
        }
    }

    @Test
    void getParameterNamesAndValuesAlsoThrowOnPartialBodyReadFailure() throws Exception {
        // Coverage for the remaining two methods on the parameter
        // surface — getParameterNames and getParameterValues
        // funnel through the same parameters() builder, so the
        // failure-fast guard applies symmetrically.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            HttpServletRequest req = httpRequestWithFlakeyStream(
                    "a=1".getBytes(),
                    "application/x-www-form-urlencoded",
                    "reset");

            Throwable[] caughtNames = new Throwable[]{null};
            Throwable[] caughtValues = new Throwable[]{null};
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                HttpServletRequest wrapped = (HttpServletRequest) reqArg;
                try { wrapped.getParameterNames(); } catch (Throwable t) { caughtNames[0] = t; }
                try { wrapped.getParameterValues("a"); } catch (Throwable t) { caughtValues[0] = t; }
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertTrue(caughtNames[0] instanceof IllegalStateException,
                    "getParameterNames must throw ISE on pre-read failure; got " + caughtNames[0]);
            assertTrue(caughtValues[0] instanceof IllegalStateException,
                    "getParameterValues must throw ISE on pre-read failure; got " + caughtValues[0]);
        }
    }

    /**
     * Build a request mock whose {@code getInputStream()} call
     * itself throws — simulating a container-side runtime failure
     * (e.g. IllegalStateException because the request was
     * already processed). The wrapper must record this the same
     * way it records an IOException, so the failure-surfacing
     * machinery engages downstream instead of pretending the
     * body was an empty payload.
     */
    private static HttpServletRequest httpRequestWhereGetInputStreamThrows(String contentType,
                                                                            RuntimeException toThrow)
            throws IOException {
        HttpServletRequest req = mock(HttpServletRequest.class);
        when(req.getMethod()).thenReturn("POST");
        when(req.getRequestURL()).thenReturn(new StringBuffer("https://x"));
        when(req.getContentLengthLong()).thenReturn(50L);
        when(req.getCharacterEncoding()).thenReturn("UTF-8");
        when(req.getHeaderNames()).thenReturn(Collections.emptyEnumeration());
        when(req.getContentType()).thenReturn(contentType);
        when(req.isAsyncStarted()).thenReturn(false);
        when(req.getInputStream()).thenThrow(toThrow);
        return req;
    }

    @Test
    void runtimeFailureDuringPreReadSurfacesAsIOExceptionNotCleanEmptyBody() throws Exception {
        // The reviewer's exact scenario: super.getInputStream()
        // throws a RuntimeException (typically
        // IllegalStateException from a container that thinks the
        // request was already consumed, or other connector-state
        // failures). Before the fix, the wrapper had no
        // loadFailure recorded AND no cached bytes, so
        // getInputStream() downstream returned a clean empty body
        // (byte[0]). Application code parsing JSON / form bodies
        // would see "valid empty payload" and proceed — silently
        // masking the real container failure.
        //
        // After the fix, the runtime exception is wrapped into
        // loadFailure (IOException with the runtime as cause)
        // and cached is initialised to byte[0]. The failure-
        // surfacing replay delegate then throws on the first read.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            IllegalStateException containerBarf = new IllegalStateException(
                    "request input stream is no longer available");
            HttpServletRequest req = httpRequestWhereGetInputStreamThrows(
                    "application/json", containerBarf);

            int[] downstreamReadCount = new int[]{-1};
            IOException[] surfaced = new IOException[]{null};
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                ServletInputStream in = ((HttpServletRequest) reqArg).getInputStream();
                try {
                    byte[] buf = new byte[64];
                    downstreamReadCount[0] = in.read(buf);
                } catch (IOException ioe) {
                    surfaced[0] = ioe;
                }
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);

            assertEquals(-1, downstreamReadCount[0],
                    "downstream must NOT receive bytes — read should throw, not "
                            + "deliver a clean empty body");
            assertNotNull(surfaced[0],
                    "runtime failure during pre-read MUST surface as IOException; "
                            + "silently delivering an empty body would mask the failure");
            // Walk the cause chain — the original
            // IllegalStateException must be reachable so debug
            // tools / loggers can identify the real root cause.
            Throwable causeWalk = surfaced[0];
            boolean foundOriginalRuntime = false;
            while (causeWalk != null) {
                if (causeWalk == containerBarf) {
                    foundOriginalRuntime = true;
                    break;
                }
                causeWalk = causeWalk.getCause();
            }
            assertTrue(foundOriginalRuntime,
                    "the original IllegalStateException must be reachable via the "
                            + "getCause() chain on the surfaced IOException; chain was "
                            + surfaced[0]);
        }
    }

    @Test
    void runtimeFailureDuringPreReadSurfacesViaGetParameterToo() throws Exception {
        // Parity check on the form-param surface: a runtime
        // failure during pre-read must NOT let getParameter()
        // return null for every key (which would look like
        // "request had no params" and proceed). Same throw the
        // I/O failure path uses: IllegalStateException with the
        // original runtime exception reachable in the cause chain.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            IllegalStateException containerBarf = new IllegalStateException(
                    "input stream already consumed elsewhere");
            HttpServletRequest req = httpRequestWhereGetInputStreamThrows(
                    "application/x-www-form-urlencoded", containerBarf);

            Throwable[] caught = new Throwable[]{null};
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                try {
                    ((HttpServletRequest) reqArg).getParameter("anything");
                } catch (Throwable t) {
                    caught[0] = t;
                }
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);

            assertNotNull(caught[0],
                    "form-param caller must observe the pre-read runtime failure, not "
                            + "silently receive null for every key");
            assertTrue(caught[0] instanceof IllegalStateException,
                    "expected ISE per Servlet 6.1 getParameter*() contract; got " + caught[0]);
            // Original runtime exception reachable through cause chain.
            Throwable causeWalk = caught[0];
            boolean foundOriginalRuntime = false;
            while (causeWalk != null) {
                if (causeWalk == containerBarf) {
                    foundOriginalRuntime = true;
                    break;
                }
                causeWalk = causeWalk.getCause();
            }
            assertTrue(foundOriginalRuntime,
                    "original IllegalStateException must be reachable through cause chain; got "
                            + caught[0]);
        }
    }

    @Test
    void runtimeFailureDuringPreReadDoesNotPropagateToFilterChain() throws Exception {
        // Independent of the surfacing-on-read behaviour: the
        // filter itself MUST NOT propagate the runtime failure
        // up the chain. Capture instrumentation is non-blocking
        // by contract — a runtime failure during pre-read should
        // (a) be recorded for downstream surfacing and (b) let
        // the chain continue. Downstream still gets to make its
        // own decision about how to handle the read failure
        // when it reaches a consumer surface.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            HttpServletRequest req = httpRequestWhereGetInputStreamThrows(
                    "application/json",
                    new IllegalStateException("connector barf"));

            // Use a normal FilterChain mock that doesn't touch
            // the body — verifies the filter itself doesn't blow
            // up just because pre-read failed.
            FilterChain chain = mock(FilterChain.class);
            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            verify(chain, times(1)).doFilter(any(), any());
        }
        // Capture line still emitted — minus the body field.
        assertEquals(1, sink.lines().size());
        String line = sink.lines().get(0);
        assertFalse(line.contains("\"body\""),
                "no body field on a runtime-failure pre-read: " + line);
    }

    @Test
    void runtimeFailureWithEmptyCachedTriggersOnErrorWithoutFiringOnDataAvailable() throws Exception {
        // Spec contract: ReadListener.onDataAvailable() is for
        // "data IS available to read". When pre-read failed before
        // any byte was buffered (cached == byte[0], loadFailure
        // != null — the runtime-failure path), firing
        // onDataAvailable would be a contract violation: there
        // are zero readable bytes, only an error pending. Real
        // containers in this state skip directly to onError. The
        // wrapper must do the same.
        //
        // Pre-fix behaviour: do/while body fired onDataAvailable
        // unconditionally once even on an empty buffer, then
        // onError. Listeners that initialised lazy state in
        // onDataAvailable (assuming bytes were ready) would
        // execute that init for nothing, then immediately hit
        // the error path — sometimes double-charging a metric,
        // allocating a buffer they then have to discard, etc.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            HttpServletRequest req = httpRequestWhereGetInputStreamThrows(
                    "application/json",
                    new IllegalStateException("connector barf"));
            // Required for setReadListener registration under the
            // default strict precondition. (The bug isn't async-
            // specific — same fault would surface on an upgrade-
            // predicate-permitted listener too — but markAsyncStarted
            // is the cheapest way to reach the listener path.)
            markAsyncStarted(req);

            int[] dataAvailableCalls = new int[]{0};
            int[] allReadCalls = new int[]{0};
            Throwable[] errorSurfaced = new Throwable[]{null};
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                ServletInputStream in = ((HttpServletRequest) reqArg).getInputStream();
                in.setReadListener(new jakarta.servlet.ReadListener() {
                    @Override public void onDataAvailable() {
                        dataAvailableCalls[0]++;
                    }
                    @Override public void onAllDataRead() { allReadCalls[0]++; }
                    @Override public void onError(Throwable t) { errorSurfaced[0] = t; }
                });
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);

            assertEquals(0, dataAvailableCalls[0],
                    "onDataAvailable MUST NOT fire when there are zero readable bytes "
                            + "and a failure is pending — Servlet ReadListener contract "
                            + "is 'when data is available', not 'always at least once'");
            assertEquals(0, allReadCalls[0],
                    "onAllDataRead must NOT fire on a failed pre-read");
            assertNotNull(errorSurfaced[0],
                    "onError MUST fire to surface the pre-read failure");
            assertTrue(errorSurfaced[0] instanceof IOException,
                    "onError should receive an IOException; got " + errorSurfaced[0]);
        }
    }

    @Test
    void isFinishedReturnsFalseWhenLoadFailurePendingEvenAfterDrain() throws Exception {
        // A consumer that checks isFinished() before reading must
        // not be told "true" when there's still an I/O failure to
        // surface. isFinished should stay false until the failure
        // is observed via a read() throw, so polling loops
        // continue until they hit the IOException path rather
        // than exiting cleanly on a false-positive "all done".
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            HttpServletRequest req = httpRequestWithFlakeyStream(
                    "x".getBytes(), "application/octet-stream", "reset");

            boolean[] finishedAfterDrain = new boolean[]{true};
            IOException[] surfaced = new IOException[]{null};
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                ServletInputStream in = ((HttpServletRequest) reqArg).getInputStream();
                try {
                    in.read();   // drains the 1-byte prefix
                    finishedAfterDrain[0] = in.isFinished();
                    in.read();   // boundary — throws
                } catch (IOException ioe) {
                    surfaced[0] = ioe;
                }
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);

            assertFalse(finishedAfterDrain[0],
                    "isFinished must return false while a load failure is pending");
            assertNotNull(surfaced[0],
                    "subsequent read past the prefix must throw IOException");
        }
    }

    @Test
    void loadBodyDoesNotCloseUnderlyingRequestStream() throws Exception {
        // Servlet spec: the container owns the request input
        // stream's lifecycle. A filter closing it would break
        // any later code that expects to consume more (or any
        // fallback path inside the filter itself). loadBody must
        // drain WITHOUT closing.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            HttpServletRequest req = mock(HttpServletRequest.class);
            when(req.getMethod()).thenReturn("POST");
            when(req.getRequestURL()).thenReturn(new StringBuffer("https://x"));
            when(req.getContentLengthLong()).thenReturn(5L);
            when(req.getCharacterEncoding()).thenReturn("UTF-8");
            when(req.getHeaderNames()).thenReturn(Collections.emptyEnumeration());
            when(req.getContentType()).thenReturn("application/octet-stream");

            boolean[] closeCalled = new boolean[]{false};
            ServletInputStream watched = new ServletInputStream() {
                final byte[] data = "hello".getBytes();
                int idx = 0;
                @Override public int read() {
                    return idx >= data.length ? -1 : (data[idx++] & 0xff);
                }
                @Override public int read(byte[] b, int off, int len) {
                    if (idx >= data.length) return -1;
                    int n = Math.min(len, data.length - idx);
                    System.arraycopy(data, idx, b, off, n);
                    idx += n;
                    return n;
                }
                @Override public boolean isFinished() { return idx >= data.length; }
                @Override public boolean isReady() { return true; }
                @Override public void setReadListener(jakarta.servlet.ReadListener l) {}
                @Override public void close() {
                    closeCalled[0] = true;
                }
            };
            when(req.getInputStream()).thenReturn(watched);

            filter.doFilter(req, mock(HttpServletResponse.class), mock(FilterChain.class));

            assertFalse(closeCalled[0],
                    "loadBody must NOT close the underlying request stream — "
                            + "the container owns its lifecycle");
        }
    }

    @Test
    void getParameterValuesReturnsCloneSoCallerMutationsDoNotCorruptCachedMap() throws Exception {
        // Defensive-copy contract: getParameterValues() returns a
        // FRESH array, not the live internal one. A caller that
        // mutates the result must not see those mutations on the
        // next call against the same request.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            // Multi-value parameter: roles=admin&roles=user.
            byte[] body = "roles=admin&roles=user".getBytes();
            HttpServletRequest req = httpRequest(
                    "POST", "https://x/api", null,
                    List.of("Content-Type"),
                    java.util.Map.of("Content-Type",
                            List.of("application/x-www-form-urlencoded")),
                    body);

            String[][] firstRead = new String[1][];
            String[][] secondRead = new String[1][];
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                HttpServletRequest wrapped = (HttpServletRequest) reqArg;
                firstRead[0] = wrapped.getParameterValues("roles");
                // Caller mutates the result it received.
                firstRead[0][0] = "ATTACKER";
                // Subsequent call must see the ORIGINAL value, not
                // the mutation — proves defensive copy.
                secondRead[0] = wrapped.getParameterValues("roles");
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertEquals("ATTACKER", firstRead[0][0],
                    "caller's local mutation should land in their local array");
            assertEquals("admin", secondRead[0][0],
                    "subsequent read must return the ORIGINAL value — defensive "
                            + "copy on getParameterValues; got " + secondRead[0][0]);
            assertEquals("user", secondRead[0][1]);
        }
    }

    @Test
    void getParameterMapReturnsClonedArraysSoCallerMutationsDoNotCorruptCachedMap() throws Exception {
        // Same property as getParameterValues, applied to the
        // map-of-arrays surface. The outer map is unmodifiable
        // (no put/remove), but historically the value arrays were
        // live references that mutations could corrupt.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            byte[] body = "roles=admin&roles=user&team=red".getBytes();
            HttpServletRequest req = httpRequest(
                    "POST", "https://x/api", null,
                    List.of("Content-Type"),
                    java.util.Map.of("Content-Type",
                            List.of("application/x-www-form-urlencoded")),
                    body);

            java.util.Map<String, String[]>[] firstMap = new java.util.Map[1];
            String[][] readAfterMutation = new String[1][];
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                HttpServletRequest wrapped = (HttpServletRequest) reqArg;
                firstMap[0] = wrapped.getParameterMap();
                // Mutate the value array inside the returned map.
                firstMap[0].get("roles")[0] = "ATTACKER";
                firstMap[0].get("team")[0] = "blue";
                // Next read must NOT reflect those mutations.
                readAfterMutation[0] = wrapped.getParameterValues("roles");
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);
            assertEquals("admin", readAfterMutation[0][0],
                    "getParameterMap must deep-copy values — caller mutation "
                            + "of map[\"roles\"][0] must not leak to a follow-up "
                            + "getParameterValues call; got " + readAfterMutation[0][0]);
            assertEquals("user", readAfterMutation[0][1]);
        }
    }

    @Test
    void multipartRequestBypassesBodyWrapperToPreserveDownstreamPartParsing() throws Exception {
        // The wrapper exposes a replay InputStream/Reader but does
        // NOT synthesise a parsed getPart(s) view. If we wrapped
        // multipart requests, downstream parsing — req.getPart(s),
        // Spring's MultipartFile, Apache Commons FileUpload —
        // would see the already-exhausted original stream and
        // surface either no parts or a parse failure. The filter
        // must skip body capture for multipart and pass the
        // original request through.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            // Fits the cap on size; would normally be wrapped. The
            // content-type is what flips the bypass.
            byte[] body = ("--boundary\r\n"
                    + "Content-Disposition: form-data; name=\"file\"; filename=\"a.txt\"\r\n\r\n"
                    + "hello\r\n--boundary--\r\n").getBytes();
            HttpServletRequest req = httpRequest(
                    "POST", "https://x/upload", null,
                    List.of("Content-Type"),
                    java.util.Map.of("Content-Type",
                            List.of("multipart/form-data; boundary=boundary")),
                    body);

            // Capture the request the filter forwards downstream —
            // it must be the ORIGINAL (`req`), not a wrapper.
            ServletRequest[] forwarded = new ServletRequest[1];
            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                forwarded[0] = reqArg;
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);

            // Strong invariant: the filter must NOT have touched
            // the request's input stream. Touching it would consume
            // the bytes a downstream multipart parser depends on.
            verify(req, never()).getInputStream();
            assertEquals(System.identityHashCode(req),
                    System.identityHashCode(forwarded[0]),
                    "multipart request must pass through UNWRAPPED so downstream "
                            + "getPart(s) sees the original stream");
        }
        // Headers + URL still captured; no body field.
        assertEquals(1, sink.lines().size());
        String line = sink.lines().get(0);
        assertTrue(line.contains("\"Content-Type\":\"multipart/form-data; boundary=boundary\""),
                "multipart Content-Type should still be logged: " + line);
        assertFalse(line.contains("\"body\""),
                "multipart bodies must not be captured (no replay semantics): " + line);
    }

    @Test
    void multipartContentTypeMatchedCaseInsensitivelyForBypass() throws Exception {
        // Defensive matching: clients send Content-Type with any
        // case. "MULTIPART/FORM-DATA" must trigger the same bypass.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            byte[] body = "--b\r\n--b--\r\n".getBytes();
            HttpServletRequest req = httpRequest(
                    "POST", "https://x", null,
                    List.of("Content-Type"),
                    java.util.Map.of("Content-Type",
                            List.of("MULTIPART/FORM-DATA; boundary=b")),
                    body);

            filter.doFilter(req, mock(HttpServletResponse.class), mock(FilterChain.class));

            verify(req, never()).getInputStream();
        }
        assertFalse(sink.lines().get(0).contains("\"body\""),
                "uppercase multipart Content-Type must also skip capture");
    }

    @Test
    void readListenerReInvokedUntilConsumerDrainsForPartialReads() throws Exception {
        // Spec semantics: onDataAvailable signals "data is ready
        // NOW", not "drain everything synchronously". A listener
        // that reads a few bytes and returns is valid. Real
        // containers re-invoke onDataAvailable as long as data
        // remains ready. The previous implementation fired once
        // and called onAllDataRead only on full immediate drain,
        // which stalled partial-read listeners (they'd never see
        // the next callback or the final onAllDataRead).
        //
        // Verification: consumer reads exactly one byte per
        // onDataAvailable callback. The wrapper must keep firing
        // until the buffer drains, then fire onAllDataRead exactly
        // once.
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            // 5-byte body — expect 5 onDataAvailable + 1 onAllDataRead.
            byte[] body = "hello".getBytes();
            HttpServletRequest req = httpRequest(
                    "POST", "https://x", null,
                    List.of(), java.util.Map.of(), body);
            markAsyncStarted(req);

            int[] dataAvailableCalls = new int[]{0};
            int[] allReadCalls = new int[]{0};
            Throwable[] onErrorSurfaced = new Throwable[]{null};
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();

            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                ServletInputStream in = ((HttpServletRequest) reqArg).getInputStream();
                in.setReadListener(new jakarta.servlet.ReadListener() {
                    @Override
                    public void onDataAvailable() throws IOException {
                        dataAvailableCalls[0]++;
                        // Read exactly ONE byte per callback. The
                        // wrapper must keep firing until drained.
                        int b = in.read();
                        if (b != -1) out.write(b);
                    }
                    @Override
                    public void onAllDataRead() {
                        allReadCalls[0]++;
                    }
                    @Override
                    public void onError(Throwable t) {
                        onErrorSurfaced[0] = t;
                    }
                });
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);

            assertEquals(5, dataAvailableCalls[0],
                    "partial-read listener must be re-invoked once per pending-byte "
                            + "callback; expected 5, got " + dataAvailableCalls[0]);
            assertEquals(1, allReadCalls[0],
                    "onAllDataRead should fire exactly once after the drain");
            assertNull(onErrorSurfaced[0],
                    "no error expected on a happy-path partial-read drain");
            assertEquals("hello", out.toString(),
                    "every byte must be delivered across the multi-callback drain");
        }
    }

    @Test
    void readListenerRefusingConsumerBreaksLoopAndSkipsOnAllDataRead() throws Exception {
        // Defensive contract: a listener that reads ZERO bytes per
        // callback would loop the wrapper forever if we just kept
        // firing. Match real-container behaviour: stop firing once
        // the listener fails to make progress, and DO NOT fire
        // onAllDataRead (bytes are still pending). The consumer
        // stays stuck — same outcome as in a real container that
        // would only re-invoke when MORE data arrives (which can
        // never happen for an in-memory buffer).
        ListSink sink = new ListSink();
        try (CaptureLogger logger = loggerWithSink(sink)) {
            CaptureServletFilter filter = new CaptureServletFilter(logger, true, 16_384);
            byte[] body = "abc".getBytes();
            HttpServletRequest req = httpRequest(
                    "POST", "https://x", null,
                    List.of(), java.util.Map.of(), body);
            markAsyncStarted(req);

            int[] dataAvailableCalls = new int[]{0};
            int[] allReadCalls = new int[]{0};

            FilterChain chain = (ServletRequest reqArg, ServletResponse respArg) -> {
                ServletInputStream in = ((HttpServletRequest) reqArg).getInputStream();
                in.setReadListener(new jakarta.servlet.ReadListener() {
                    @Override
                    public void onDataAvailable() {
                        // Deliberately read nothing. Without a
                        // progress guard the wrapper would call
                        // this method until the heat death of the
                        // universe.
                        dataAvailableCalls[0]++;
                    }
                    @Override public void onAllDataRead() { allReadCalls[0]++; }
                    @Override public void onError(Throwable t) {}
                });
            };

            filter.doFilter(req, mock(HttpServletResponse.class), chain);

            assertEquals(1, dataAvailableCalls[0],
                    "progress guard should stop after exactly one no-op callback; got "
                            + dataAvailableCalls[0]);
            assertEquals(0, allReadCalls[0],
                    "onAllDataRead must NOT fire while data is still pending — "
                            + "the listener never drained the buffer");
        }
    }
}
