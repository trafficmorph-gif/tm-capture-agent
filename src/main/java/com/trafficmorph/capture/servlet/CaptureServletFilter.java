package com.trafficmorph.capture.servlet;

import com.trafficmorph.capture.CaptureLogger;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Turnkey {@link Filter} that feeds every inbound HTTP request into a
 * {@link CaptureLogger}. Drop it into any Servlet-5 / jakarta.servlet
 * container (Spring Boot 3, Tomcat 10+, Jetty 11+, etc.) and the
 * agent records traffic to whatever sink the logger is wired to.
 *
 * <h2>Wiring (Spring Boot 3 example)</h2>
 * <pre>
 *   {@literal @}Bean
 *   FilterRegistrationBean&lt;CaptureServletFilter&gt; captureFilter() {
 *       CaptureLogger logger = CaptureLogger.builder()
 *               .sink(FileSink.builder(Path.of("/var/log/tm-capture.jsonl"))
 *                       .maxAge(Duration.ofHours(1))
 *                       .build())
 *               .build();
 *       return new FilterRegistrationBean&lt;&gt;(
 *               new CaptureServletFilter(logger, true, 16_384));
 *   }
 * </pre>
 *
 * <h2>Body capture caveats</h2>
 * <p>With {@code captureBody=true} the filter buffers each request's
 * body into memory so it can both replay the bytes downstream AND
 * hand them to the logger. This is fine for small JSON / form POSTs
 * but is the wrong fit for:
 * <ul>
 *   <li>Streaming upload endpoints (multipart with large files,
 *       chunked uploads, video streams). The body buffer would
 *       hold the entire payload in heap.</li>
 *   <li>Requests larger than {@code maxBodyBytes}: bodies above
 *       this size skip capture entirely (the filter logs method +
 *       URL + headers, never reads the request stream itself, so
 *       streaming behaviour is preserved).</li>
 * </ul>
 *
 * <p>Even with capture enabled, the filter NEVER lets capture
 * instrumentation break a request: every interaction with
 * {@link CaptureLogger} and every body read is wrapped in
 * {@code try / catch (RuntimeException | IOException)} — failures
 * are swallowed and the request continues.
 *
 * <h2>Logger lifecycle</h2>
 * <p>The supplied {@link CaptureLogger} is NOT closed by this
 * filter's {@link #destroy()} — its lifecycle belongs to whoever
 * built it. Typically that's a Spring {@code @Bean} method with
 * {@code destroyMethod="close"} (or its equivalent in your DI
 * container).
 */
public final class CaptureServletFilter implements Filter {

    /**
     * Default body cap matches {@link CaptureLogger.Builder#maxBodyLength}:
     * 16 KiB is enough for typical JSON / RTB bid bodies, small
     * enough that buffering N concurrent requests under back-pressure
     * doesn't blow heap. Bodies above this are NOT buffered — the
     * filter logs headers/URL only for those requests.
     */
    public static final int DEFAULT_MAX_BODY_BYTES = 16 * 1024;

    /**
     * Default charset for decoding percent-escapes in the URL query
     * string. Modern Servlet containers default to UTF-8 for
     * {@code URIEncoding} (Tomcat 8+, Jetty 10+, Undertow), and the
     * WHATWG URL standard mandates UTF-8 for {@code
     * application/x-www-form-urlencoded} bodies. Deployments that
     * configure their container with a non-UTF-8 URI decoding need
     * to override via the four-arg constructor.
     */
    public static final Charset DEFAULT_QUERY_CHARSET = StandardCharsets.UTF_8;

    /**
     * Default set of HTTP methods whose request bodies the wrapper
     * intercepts for form-parameter parsing. Strict Servlet semantics:
     * the container's auto-parse-form-body rule applies to POST only;
     * PUT/PATCH/DELETE/GET expose only query-string params via
     * {@code getParameter*()}. Deployers running a container
     * configured to parse form bodies on other methods (e.g. Tomcat's
     * {@code parseBodyMethods=POST,PUT}) should pass an enlarged set
     * via the seven-arg constructor so wrapper parameters match what
     * the container produces. Stored as an immutable set; method
     * names are uppercase-normalised at construction time.
     */
    public static final Set<String> DEFAULT_PARSED_BODY_METHODS = Set.of("POST");

    private final CaptureLogger logger;
    private final boolean captureBody;
    private final int maxBodyBytes;
    private final Charset queryCharset;
    private final Predicate<HttpServletRequest> upgradeRoutePredicate;
    private final Set<String> parsedBodyMethods;
    private final boolean lenientParameterDecoding;

    /**
     * Convenience constructor: header + URL capture only. Use this
     * for high-volume request paths where body buffering would be
     * too expensive, or for endpoints that handle binary / streaming
     * payloads (uploads, websockets-over-HTTP-upgrade, video).
     */
    public CaptureServletFilter(CaptureLogger logger) {
        this(logger, false, DEFAULT_MAX_BODY_BYTES, DEFAULT_QUERY_CHARSET,
                null, DEFAULT_PARSED_BODY_METHODS, false);
    }

    /**
     * Three-arg constructor that defaults {@code queryCharset} to
     * UTF-8 (the modern container default). Use the four-arg form
     * if your container is configured with a non-UTF-8 URI encoding
     * and you need the wrapper's parameter parsing to match it.
     */
    public CaptureServletFilter(CaptureLogger logger, boolean captureBody, int maxBodyBytes) {
        this(logger, captureBody, maxBodyBytes, DEFAULT_QUERY_CHARSET,
                null, DEFAULT_PARSED_BODY_METHODS, false);
    }

    /**
     * Four-arg constructor that defaults {@code upgradeRoutePredicate}
     * to {@code null} — the strict spec-compliant default. Use the
     * five-arg form to nominate a deployer-supplied predicate that
     * narrowly identifies the upgrade-capable routes on which the
     * async precondition should be relaxed.
     */
    public CaptureServletFilter(CaptureLogger logger, boolean captureBody, int maxBodyBytes,
                                 Charset queryCharset) {
        this(logger, captureBody, maxBodyBytes, queryCharset,
                null, DEFAULT_PARSED_BODY_METHODS, false);
    }

    /**
     * Five-arg constructor that defaults {@code parsedBodyMethods}
     * to {@code {POST}} (strict Servlet semantics) and
     * {@code lenientParameterDecoding} to {@code false} (matches
     * container failure semantics on malformed percent-escapes).
     * Use the seven-arg form to widen the parsed-body method set or
     * opt into lenient decoding.
     */
    public CaptureServletFilter(CaptureLogger logger, boolean captureBody, int maxBodyBytes,
                                 Charset queryCharset,
                                 Predicate<HttpServletRequest> upgradeRoutePredicate) {
        this(logger, captureBody, maxBodyBytes, queryCharset, upgradeRoutePredicate,
                DEFAULT_PARSED_BODY_METHODS, false);
    }

    /**
     * Full constructor.
     *
     * @param logger        destination for captured events. Caller
     *                      owns the logger's lifecycle — closing
     *                      this filter does NOT close the logger.
     * @param captureBody   true to buffer the request body for
     *                      capture; false skips body reads entirely.
     * @param maxBodyBytes  upper bound on bytes buffered for capture.
     *                      Requests with {@code Content-Length} above
     *                      this are NOT buffered (the filter records
     *                      method + URL + headers only). The agent's
     *                      own {@code maxBodyLength} can truncate
     *                      further downstream.
     * @param queryCharset  charset used to decode percent-escapes in
     *                      the URL query string when the wrapper
     *                      re-parses parameters for form POSTs.
     *                      Default is UTF-8 (modern container norm);
     *                      override when running against a container
     *                      configured with a different URIEncoding
     *                      (Tomcat's legacy ISO-8859-1, EBCDIC mainframe
     *                      front-ends, etc.) so wrapper {@code
     *                      getParameter*()} matches the container's
     *                      own decoding. Body charset is independent:
     *                      it always follows the spec (Content-Type
     *                      charset, else ISO-8859-1 per Servlet §3.13).
     * @param upgradeRoutePredicate
     *                      deployer-supplied predicate that identifies
     *                      genuine upgrade-capable requests for which
     *                      {@link ServletInputStream#setReadListener}
     *                      should be allowed outside async processing.
     *                      {@code null} (default) enforces the strict
     *                      spec precondition for EVERY request flowing
     *                      through the filter — the safe default that
     *                      catches synchronous handlers misusing the
     *                      non-blocking API and rejects clients spoofing
     *                      {@code Connection: Upgrade}.
     *
     *                      <p>A non-null predicate is evaluated against
     *                      the source request when {@code setReadListener}
     *                      is called; when it returns {@code true}, the
     *                      async check is skipped. Typical predicate
     *                      shapes:
     *                      <ul>
     *                        <li>{@code req -> req.getRequestURI().startsWith("/ws/")}
     *                            — narrow by path pattern.</li>
     *                        <li>{@code req -> req.getRequestURI().equals("/socket")
     *                            && "websocket".equalsIgnoreCase(req.getHeader("Upgrade"))}
     *                            — path + upgrade-header narrowing for
     *                            extra defence-in-depth.</li>
     *                      </ul>
     *                      A predicate that returns true for everything
     *                      ({@code req -> true}) is equivalent to the
     *                      previous global flag — possible, but the
     *                      deployer must write it explicitly, so no
     *                      filter mapping can silently widen the bypass.
     *
     *                      <p>Predicate evaluation NEVER propagates an
     *                      exception: any {@link Throwable} thrown by
     *                      {@code predicate.test(req)} is treated as
     *                      {@code false} (i.e. strict-mode rejection).
     *                      A buggy predicate fails CLOSED, never OPEN.
     *
     *                      <p>The Servlet API has no portable server-side
     *                      getter for "upgrade has been accepted by the
     *                      server" (the {@code req.upgrade(...)} action
     *                      has no symmetric {@code isUpgraded()} readback
     *                      and request-header signals are client-controlled),
     *                      so this predicate is the honest opt-in point.
     * @param parsedBodyMethods
     *                      set of HTTP methods on which the wrapper
     *                      intercepts {@code getParameter*()} to
     *                      surface form-encoded body parameters from
     *                      its captured bytes. Defaults to
     *                      {@link #DEFAULT_PARSED_BODY_METHODS}
     *                      ({@code {POST}}) — strict Servlet
     *                      semantics, the container's own
     *                      form-body auto-parse rule. Pass an
     *                      enlarged set (e.g. {@code Set.of("POST",
     *                      "PUT")}) when running against a container
     *                      configured to parse form bodies on
     *                      additional methods (Tomcat's
     *                      {@code parseBodyMethods=POST,PUT}), so
     *                      wrapper {@code getParameter*()} matches
     *                      what the container would have produced.
     *                      Method names are uppercase-normalised at
     *                      construction time; matching is case-insensitive.
     * @param lenientParameterDecoding
     *                      controls how the wrapper handles malformed
     *                      percent-escapes ({@code %ZZ}, trailing
     *                      lone {@code %}, etc.) in form parameters.
     *                      When {@code false} (default, strict):
     *                      {@link java.net.URLDecoder#decode} failures
     *                      surface as {@link IllegalStateException}
     *                      from {@code getParameter*()}, matching the
     *                      Servlet 6.1 contract for parameter-parse
     *                      failures. The underlying
     *                      {@link IllegalArgumentException} from
     *                      {@code URLDecoder} is preserved as the
     *                      ISE's {@code cause} so debug chains keep
     *                      the original decoder message. Downstream
     *                      frameworks that key on the spec-standard
     *                      ISE (Spring's exception resolvers, Jakarta
     *                      REST mappers) translate consistently to
     *                      HTTP 400 — the same outcome as a real
     *                      container's form parser failing on the
     *                      same input. When {@code true} (lenient):
     *                      the wrapper falls back to the raw encoded
     *                      substring, never throws, and the request
     *                      proceeds. Lenient mode hides
     *                      malformed-request signal from downstream
     *                      code — use it only when capture must NEVER
     *                      perturb request error semantics regardless
     *                      of payload validity (e.g. shadow-traffic
     *                      capture against legacy endpoints).
     */
    public CaptureServletFilter(CaptureLogger logger, boolean captureBody, int maxBodyBytes,
                                 Charset queryCharset,
                                 Predicate<HttpServletRequest> upgradeRoutePredicate,
                                 Set<String> parsedBodyMethods,
                                 boolean lenientParameterDecoding) {
        this.logger = Objects.requireNonNull(logger, "logger");
        if (maxBodyBytes < 0) {
            throw new IllegalArgumentException(
                    "maxBodyBytes must be >= 0, was " + maxBodyBytes);
        }
        this.captureBody = captureBody;
        this.maxBodyBytes = maxBodyBytes;
        this.queryCharset = Objects.requireNonNull(queryCharset, "queryCharset");
        // Null is allowed and is the strict default — explicit "no
        // bypass possible". Non-null is the deployer opt-in.
        this.upgradeRoutePredicate = upgradeRoutePredicate;
        // Normalise methods to trimmed-uppercase, defensive-copy
        // into an immutable set. Matching at request time is then
        // a single contains() on an already-normalised method
        // name. Trim is essential because real config flows
        // ("POST,PUT".split(",")) emit leading/trailing whitespace
        // — without trim, " PUT" would never match the request's
        // "PUT" and form-param interception would silently disable
        // for that method (the exact bug pattern the reviewer
        // flagged: missing body params after a configured PUT
        // round-trips through the filter). Blank entries reject
        // early so a typo in config ("POST,,PUT") doesn't pass
        // validation only to mismatch every request.
        Objects.requireNonNull(parsedBodyMethods, "parsedBodyMethods");
        if (parsedBodyMethods.isEmpty()) {
            // Empty set + captureBody=true would consume form
            // bodies during pre-read AND never intercept
            // getParameter*() — silently dropping every form
            // parameter for every request. That's not a useful
            // mode; it's almost always a config bug. If "capture
            // headers + URL but skip body" is the intent, the
            // dedicated knob is captureBody=false (which also
            // saves the body-buffering allocation per request).
            throw new IllegalArgumentException(
                    "parsedBodyMethods must not be empty — an empty set would silently "
                            + "disable form-param replay for every request while still "
                            + "consuming the body during pre-read. Pass at least one "
                            + "method, or use captureBody=false if no body capture is "
                            + "wanted at all.");
        }
        Set<String> normalised = new HashSet<>(parsedBodyMethods.size());
        for (String m : parsedBodyMethods) {
            if (m == null) {
                throw new IllegalArgumentException(
                        "parsedBodyMethods must not contain null");
            }
            String trimmed = m.trim();
            if (trimmed.isEmpty()) {
                throw new IllegalArgumentException(
                        "parsedBodyMethods must not contain blank entries; got '" + m + "'");
            }
            normalised.add(trimmed.toUpperCase(Locale.ROOT));
        }
        this.parsedBodyMethods = Set.copyOf(normalised);
        this.lenientParameterDecoding = lenientParameterDecoding;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        // Non-HTTP requests (e.g. WebSocket upgrade prior to HTTP
        // promotion) pass through untouched. Pre-Servlet-3 wrappers
        // can also surface here; the instanceof check is the
        // standard pattern.
        if (!(request instanceof HttpServletRequest http)) {
            chain.doFilter(request, response);
            return;
        }

        // Decide BEFORE reading anything whether we can/will capture
        // the body — based on Content-Length and the cap. This lets
        // us preserve streaming for large requests (we won't touch
        // the input stream at all in that case).
        BodyCachingRequestWrapper wrapper = null;
        String capturedBody = null;
        if (captureBody && canBufferBody(http)) {
            wrapper = new BodyCachingRequestWrapper(
                    http, maxBodyBytes, queryCharset, upgradeRoutePredicate,
                    parsedBodyMethods, lenientParameterDecoding);
            try {
                wrapper.loadBody();
                capturedBody = wrapper.capturedBodyAsString();
            } catch (IOException | RuntimeException ex) {
                // Body read failed (truncated upload, network
                // reset, hostile client). The underlying request
                // stream may have been advanced past the bytes
                // we DID read — falling back to the original
                // request here would expose a truncated stream to
                // downstream filters / servlets. Instead, KEEP
                // the wrapper: loadBody's finally block published
                // whatever partial bytes survived, so the
                // wrapper's replay surface gives downstream
                // exactly the bytes that arrived and no more.
                // We drop the captured-body string only — the
                // capture line goes out with method + URL +
                // headers but no body field, matching the
                // documented body-failure behaviour.
                capturedBody = null;
            }
        }

        // Log at request START so the timestamp reflects when the
        // client's bytes arrived, not when the downstream chain
        // finished processing. Headers + body are already in hand
        // by this point; response data isn't captured (this is a
        // REQUEST replay tool, not a response trace).
        safelyLog(http, capturedBody);

        // Forward — using the wrapper if we buffered, so downstream
        // filters / servlets read the same bytes the original
        // client sent.
        chain.doFilter(wrapper != null ? wrapper : request, response);
    }

    /**
     * Whether the request body looks safe to buffer for capture.
     * Returns false in three cases:
     * <ul>
     *   <li>Unknown / zero Content-Length (chunked uploads,
     *       streaming). The filter MUST NOT touch the stream
     *       in that case — streaming endpoints depend on it.</li>
     *   <li>Body larger than {@code maxBodyBytes}. Bounds heap
     *       under back-pressure.</li>
     *   <li>{@code multipart/*} content types. The wrapper
     *       exposes a replay {@link ServletInputStream} and
     *       {@link BufferedReader} but does NOT synthesise a
     *       parsed {@code getPart(s)} view. Downstream multipart
     *       parsing (jakarta's {@code req.getPart()}, Spring's
     *       {@code MultipartFile}, Apache Commons FileUpload)
     *       would either re-read the already-exhausted source
     *       stream and see no parts, or — depending on the
     *       container — surface a parse failure. The honest
     *       behaviour is to skip body capture entirely for
     *       multipart and let the container's own parser run
     *       unimpeded; method + URL + headers are still logged.
     *       Consumers who need to capture multipart bodies
     *       must build that path themselves (multipart replay
     *       is intentionally out of scope for this filter — the
     *       agent is built for JSON / form / RTB request shapes,
     *       not file-upload archival).</li>
     * </ul>
     */
    private boolean canBufferBody(HttpServletRequest http) {
        long contentLength = http.getContentLengthLong();
        if (contentLength <= 0) return false;   // empty / unknown
        if (contentLength > maxBodyBytes) return false;
        String mediaType = mediaTypeOf(http.getContentType());
        if (mediaType != null) {
            // Multipart subtype family: form-data, mixed, related, etc.
            // The trailing slash anchors the prefix so unrelated types
            // ("multipart-extension/foo") don't slip through.
            if (mediaType.toLowerCase(Locale.ROOT).startsWith("multipart/")) return false;
        }
        return true;
    }

    /**
     * Extract the media-type token from a Content-Type header value
     * by stripping any parameters after the first {@code ;} and
     * trimming surrounding whitespace. Returns {@code null} for a
     * {@code null} input so callers can short-circuit on absent
     * Content-Type. Comparison should be case-insensitive on the
     * result — RFC 9110 says media types are case-insensitive.
     *
     * <p>The reason this helper exists: {@code startsWith} matching
     * on the raw header value misclassifies non-equivalent types
     * (e.g. {@code application/x-www-form-urlencoded-json} starts
     * with {@code application/x-www-form-urlencoded} but is a
     * different type the container would not feed to its form
     * parser). Token-level equality avoids the false positive.
     *
     * <p>Package-private: shared with {@link BodyCachingRequestWrapper#shouldIntercept()}.
     */
    static String mediaTypeOf(String contentType) {
        if (contentType == null) return null;
        int semi = contentType.indexOf(';');
        String t = (semi >= 0) ? contentType.substring(0, semi) : contentType;
        return t.trim();
    }

    /**
     * Build the inputs the logger expects and call {@code log()}.
     * Wrapped in try/catch so a misbehaving logger / sink can't
     * break the request the filter is instrumenting.
     */
    private void safelyLog(HttpServletRequest http, String body) {
        try {
            Map<String, String> headers = collectHeaders(http);
            String url = buildFullUrl(http);
            logger.log(http.getMethod(), url, headers, body);
        } catch (RuntimeException ignored) {
            // Capture instrumentation MUST NOT break the request.
            // The logger itself counts these via its own internal
            // try/catch + invalidDropped path; this is a
            // belt-and-braces safety net.
        }
    }

    /**
     * Snapshot the headers as a {@code LinkedHashMap} in
     * iteration order. Multi-valued headers are joined with
     * {@code ", "} per the canonical HTTP wire form. The map is
     * copied (not the request's live view) so the logger's own
     * defensive snapshot doesn't race with concurrent header
     * mutations from downstream filters.
     */
    private static Map<String, String> collectHeaders(HttpServletRequest http) {
        Map<String, String> out = new LinkedHashMap<>();
        Enumeration<String> names = http.getHeaderNames();
        if (names == null) return out;       // Servlet API allows null
        while (names.hasMoreElements()) {
            String name = names.nextElement();
            if (name == null) continue;
            Enumeration<String> values = http.getHeaders(name);
            if (values == null) {
                out.put(name, "");
                continue;
            }
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            while (values.hasMoreElements()) {
                if (!first) sb.append(", ");
                String v = values.nextElement();
                if (v != null) sb.append(v);
                first = false;
            }
            out.put(name, sb.toString());
        }
        return out;
    }

    /**
     * Reconstruct the full request URL including query string —
     * the Servlet API gives us URL (scheme/host/path) and
     * queryString separately, but the agent's parser expects them
     * concatenated.
     */
    private static String buildFullUrl(HttpServletRequest http) {
        StringBuffer url = http.getRequestURL();
        if (url == null) return "";
        String query = http.getQueryString();
        if (query != null && !query.isEmpty()) {
            url.append('?').append(query);
        }
        return url.toString();
    }

}
