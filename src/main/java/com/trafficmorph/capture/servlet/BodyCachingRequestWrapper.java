package com.trafficmorph.capture.servlet;

import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Pre-reads the request body into a byte[] up to the configured
 * cap and exposes a fresh {@link ServletInputStream} backed by
 * that buffer for every subsequent {@code getInputStream}/{@code getReader}
 * call. Downstream filters / servlets read the same bytes the
 * original client sent; the cached array also feeds the
 * captured-string representation handed to the logger.
 *
 * <p>Package-private: this class is an implementation detail of
 * {@link CaptureServletFilter} and only constructed from
 * {@code doFilter()}.
 */
final class BodyCachingRequestWrapper extends HttpServletRequestWrapper {

    /**
     * Per-request access mode for the body. The Servlet spec
     * mandates that {@code getInputStream()} and {@code getReader()}
     * are mutually exclusive per request — whichever the
     * consumer calls FIRST locks the request into that mode,
     * and a subsequent call to the other method throws
     * {@link IllegalStateException}. Real containers enforce
     * this; our wrapper must too, otherwise a consumer that
     * accidentally calls both will see partial / ambiguous body
     * reads through the wrapper but fail loudly against a real
     * container — exactly the kind of bug capture
     * instrumentation should NOT hide.
     */
    private enum AccessMode { UNUSED, STREAM, READER }

    private final int captureCap;
    private final Charset configuredQueryCharset;
    private final Predicate<HttpServletRequest> upgradeRoutePredicate;
    private final Set<String> parsedBodyMethods;
    private final boolean lenientParameterDecoding;
    private byte[] cached;
    private String cachedAsString;

    /**
     * If pre-read failed mid-stream, this holds the original
     * {@link IOException}. The replay surfaces it to downstream
     * readers AFTER the partial bytes drain — so application
     * code sees the same "stream went bad" signal it would have
     * seen from the underlying request stream, NOT a clean EOF
     * that would let it parse a truncated payload as valid.
     */
    private IOException loadFailure;

    private AccessMode accessMode = AccessMode.UNUSED;

    /**
     * Cached replay stream — created lazily on the first
     * {@link #getInputStream()} call and returned verbatim on
     * every subsequent call within this request's lifetime.
     * Critical for spec compliance: {@code setReadListener} is
     * a per-REQUEST single-shot per the Servlet spec, but the
     * "listener already registered" check lives on the stream
     * instance. If we returned a fresh stream every call,
     * a consumer could call {@code req.getInputStream().setReadListener(a)}
     * then {@code req.getInputStream().setReadListener(b)} and
     * both would succeed — non-deterministic callback behaviour
     * that diverges from real container streams.
     *
     * <p>Real containers also return the same stream instance
     * across multiple {@code getInputStream()} calls, with
     * read-state preserved between callers. We match that.
     */
    private ServletInputStream cachedStream;

    /**
     * Parsed parameter map (query-string + form-body merged),
     * populated lazily on first {@code getParameter*()} call.
     * Required because our {@link #loadBody} pre-reads the
     * request input stream and the underlying container's
     * {@code getParameter()} would parse the body lazily by
     * re-reading the stream — which is now empty. Without these
     * overrides any form-encoded POST loses its body params
     * once it passes through the filter.
     */
    private Map<String, String[]> cachedParameters;

    /**
     * Cached reader, same rationale as {@link #cachedStream} —
     * multiple {@code getReader()} calls within one request
     * must share consumption state, matching what real
     * containers do (a reader read halfway through the body
     * and then re-obtained via another {@code getReader()}
     * call resumes from where it left off, not from byte 0).
     */
    private BufferedReader cachedReader;

    BodyCachingRequestWrapper(HttpServletRequest delegate, int captureCap, Charset queryCharset,
                               Predicate<HttpServletRequest> upgradeRoutePredicate,
                               Set<String> parsedBodyMethods,
                               boolean lenientParameterDecoding) {
        super(delegate);
        this.captureCap = captureCap;
        this.configuredQueryCharset = queryCharset;
        this.upgradeRoutePredicate = upgradeRoutePredicate;
        this.parsedBodyMethods = parsedBodyMethods;
        this.lenientParameterDecoding = lenientParameterDecoding;
    }

    /**
     * Read the full body into {@code cached}. Called once at
     * filter entry; idempotent. May throw {@link IOException}
     * if the underlying read fails — the caller in
     * {@code doFilter} treats that as "skip body capture",
     * but KEEPS the wrapper in place so downstream readers
     * see the partial bytes captured here (via the cached
     * replay) rather than the advanced-and-corrupted
     * underlying request stream.
     *
     * <p>Two correctness properties beyond the obvious "drain
     * the stream":
     * <ul>
     *   <li><b>Do NOT close the underlying stream.</b> The
     *       container owns the request input stream's
     *       lifecycle. Closing it from a filter is wrong per
     *       the spec, and concretely breaks any fallback
     *       that wants to forward the original request
     *       (the stream would surface as closed downstream).
     *       We just drain.</li>
     *   <li><b>Publish partial-read bytes even on failure.</b>
     *       If the read loop throws halfway through (truncated
     *       upload, network reset), the underlying stream is
     *       already advanced past the bytes we DID read —
     *       there is no way for downstream code to recover
     *       them. Publishing them into {@code cached} via the
     *       {@code finally} block means the wrapper's replay
     *       surface gives downstream exactly the bytes that
     *       arrived, and no more. The caller still receives
     *       the {@code IOException} so it knows to skip the
     *       capture-line body field; the wrapper itself stays
     *       usable.</li>
     * </ul>
     */
    void loadBody() throws IOException {
        if (cached != null) return;
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try {
            // super.getInputStream() itself can throw — some
            // containers raise IllegalStateException when the
            // request has been processed already, or other
            // runtime failures depending on the connector's
            // state. That call MUST be inside the try block
            // so a runtime failure here records loadFailure
            // and lets the finally publish an empty cached
            // array. Without that, the wrapper would default
            // to byte[0] with no loadFailure, silently
            // delivering a clean empty body to downstream
            // and masking the real failure.
            byte[] tmp = new byte[4096];
            InputStream in = super.getInputStream();
            int read;
            while ((read = in.read(tmp)) != -1) {
                buf.write(tmp, 0, read);
            }
        } catch (IOException ioe) {
            // Stash the failure on the wrapper. The replay
            // path consults this in getInputStream / getReader
            // and surfaces it as an IOException at the
            // partial-bytes boundary — application code MUST
            // observe the I/O failure, not a clean EOF. A
            // clean EOF here would mean truncated JSON / form
            // payloads silently parse as valid.
            this.loadFailure = ioe;
            throw ioe;
        } catch (RuntimeException re) {
            // Runtime failure from the container side (the
            // most plausible shape is IllegalStateException
            // from super.getInputStream() when the request is
            // in an unexpected state, but any unchecked
            // exception lands here). Wrap as IOException so
            // the same downstream machinery — failure-
            // surfacing delegate, parameters() guard,
            // setReadListener onError branch — engages
            // uniformly with the IOException path. The
            // original runtime exception is preserved as the
            // chain's deepest cause.
            this.loadFailure = new IOException(
                    "request body pre-read failed with an unchecked "
                            + "exception: " + re.getClass().getSimpleName(),
                    re);
            throw re;
        } finally {
            // Publish whatever made it into the buffer. Empty
            // on a first-read failure (IOException or
            // RuntimeException); partial on a mid-read
            // failure; full on success. Either way, the
            // wrapper has a stable replay surface and the
            // underlying request stream is NOT closed.
            cached = buf.toByteArray();
        }
    }

    /**
     * Return the captured body as a String, truncated to
     * {@code captureCap} bytes. The logger's own
     * {@code maxBodyLength} may truncate further; the cap
     * here bounds the filter-side string allocation.
     */
    String capturedBodyAsString() {
        if (cached == null) return null;
        if (cachedAsString == null) {
            int len = Math.min(cached.length, captureCap);
            cachedAsString = new String(cached, 0, len, charset());
        }
        return cachedAsString;
    }

    private Charset charset() {
        String enc = getCharacterEncoding();
        // Spec (Jakarta Servlet 6, §3.13): when no charset has
        // been set by the client OR by setCharacterEncoding,
        // the container decodes request body / reader / form
        // parameters using ISO-8859-1. Defaulting to UTF-8
        // diverges from container semantics and would mojibake
        // non-ASCII bytes that the container would have handed
        // back as raw ISO-8859-1.
        //
        // Real-world bodies are usually UTF-8, but callers that
        // care must set the Content-Type charset explicitly OR
        // call setCharacterEncoding earlier in the filter chain
        // — which is the same constraint a real container
        // imposes. Capture instrumentation MATCHES the container,
        // it doesn't override.
        if (enc == null) return StandardCharsets.ISO_8859_1;
        try {
            return Charset.forName(enc);
        } catch (IllegalArgumentException ex) {
            return StandardCharsets.ISO_8859_1;
        }
    }

    @Override
    public ServletInputStream getInputStream() {
        // Spec: getInputStream() is illegal after getReader()
        // has been called on the same request. Real containers
        // throw IllegalStateException here.
        if (accessMode == AccessMode.READER) {
            throw new IllegalStateException(
                    "getInputStream() called after getReader() — Servlet spec "
                            + "requires choosing binary OR character access, not both");
        }
        accessMode = AccessMode.STREAM;
        if (cachedStream == null) {
            // `cached` is normally non-null at this point:
            // loadBody() always runs to completion (success
            // OR failure) before the wrapper is exposed to
            // downstream, and its finally block publishes
            // cached unconditionally — empty array on a
            // runtime failure that prevented any read, partial
            // bytes on a mid-stream IOException, or the full
            // body on success. The {@code cached != null
            // ? cached : new byte[0]} fallback is a defensive
            // net for the (currently unreachable) path where
            // some future refactor exposes the wrapper before
            // loadBody runs. Keeping the fallback makes the
            // failure mode "empty replay" rather than NPE.
            //
            // When loadFailure is non-null, the
            // failure-surfacing delegate (below) throws an
            // IOException at the partial-bytes boundary
            // instead of returning -1 — application code
            // sees the failure path, not a clean empty body.
            byte[] bytes = cached != null ? cached : new byte[0];
            // Pass the request (via `this`, the wrapper) so the
            // replay stream can verify spec preconditions on
            // setReadListener — namely, that the consumer is in
            // async or upgrade mode. The deployer-supplied
            // upgrade-route predicate flows through too, scoping
            // any precondition relaxation to genuine upgrade
            // routes (never globally).
            cachedStream = new ReplayServletInputStream(
                    failureSurfacingDelegate(bytes, loadFailure),
                    this, upgradeRoutePredicate, loadFailure);
        }
        return cachedStream;
    }

    @Override
    public BufferedReader getReader() {
        // Spec: getReader() is illegal after getInputStream()
        // has been called on the same request. Symmetric with
        // the check above.
        if (accessMode == AccessMode.STREAM) {
            throw new IllegalStateException(
                    "getReader() called after getInputStream() — Servlet spec "
                            + "requires choosing binary OR character access, not both");
        }
        accessMode = AccessMode.READER;
        if (cachedReader == null) {
            // Wrap an InputStreamReader over the same
            // failure-surfacing InputStream the binary path
            // uses, so reader-style consumers ALSO observe the
            // I/O failure (as an IOException from BufferedReader
            // operations) rather than seeing a truncated read
            // as a clean EOF.
            byte[] bytes = cached != null ? cached : new byte[0];
            cachedReader = new BufferedReader(
                    new InputStreamReader(
                            failureSurfacingDelegate(bytes, loadFailure),
                            charset()));
        }
        return cachedReader;
    }

    /**
     * Build the InputStream that backs both
     * {@link #getInputStream} and {@link #getReader}. When
     * {@code loadFailure} is {@code null}, returns a plain
     * {@link ByteArrayInputStream} over the captured bytes —
     * happy-path behaviour unchanged.
     *
     * <p>When {@code loadFailure} is non-null, returns an
     * adapter that delivers the partial bytes first and then,
     * on the read that would otherwise hit EOF, throws an
     * {@link IOException} chained to the original failure.
     * This is the entire point of recording the failure: a
     * clean EOF here would let downstream code parse a
     * truncated body as a complete request (e.g. JSON parser
     * happily consumes {@code {"a":1}} from a cut-off
     * {@code {"a":1,"b":2}}). Real containers re-raise the
     * I/O error at this exact boundary; the wrapper must too.
     */
    private static InputStream failureSurfacingDelegate(byte[] bytes, IOException loadFailure) {
        if (loadFailure == null) {
            return new ByteArrayInputStream(bytes);
        }
        ByteArrayInputStream raw = new ByteArrayInputStream(bytes);
        return new InputStream() {
            @Override
            public int read() throws IOException {
                int b = raw.read();
                if (b == -1) throw makeFailure();
                return b;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                // ByteArrayInputStream's read(...) returns -1
                // only when at EOF — so a -1 here is the
                // failure boundary, not a "would block".
                int n = raw.read(b, off, len);
                if (n == -1) throw makeFailure();
                return n;
            }

            @Override
            public int available() {
                return raw.available();
            }

            private IOException makeFailure() {
                return new IOException(
                        "request body pre-read failed; "
                                + bytes.length + " byte(s) were buffered "
                                + "before the upstream I/O failure",
                        loadFailure);
            }
        };
    }

    // ── Form-parameter overrides ───────────────────────────────
    //
    // Loading the body via super.getInputStream() consumes the
    // underlying stream. The container's own getParameter*()
    // parses form bodies lazily on first call, which would
    // come up empty because we drained the bytes ahead of it.
    // These overrides re-parse query-string + form-encoded body
    // from the cached bytes so downstream form-handling code
    // sees the same view it would have seen without the filter.
    //
    // For non-form requests (JSON POST, multipart, etc.) we
    // delegate to super — body bytes aren't form data and the
    // container's parameter parsing only knows about the query
    // string anyway, which super has access to via the original
    // request URI.

    @Override
    public String getParameter(String name) {
        if (shouldIntercept()) {
            String[] vs = parameters().get(name);
            // String is immutable, so vs[0] is safe to return
            // as-is — no array exposure.
            return vs == null || vs.length == 0 ? null : vs[0];
        }
        return super.getParameter(name);
    }

    @Override
    public Map<String, String[]> getParameterMap() {
        if (shouldIntercept()) {
            // Deep-copy: the outer map AND every value array
            // must be defensive copies. Returning the live
            // internal map (even wrapped unmodifiable) would
            // let a caller mutate the value arrays in-place
            // and corrupt subsequent reads from the same
            // request. Matches real container behaviour —
            // Tomcat and Jetty both clone array values on
            // getParameterMap.
            Map<String, String[]> internal = parameters();
            Map<String, String[]> snapshot = new LinkedHashMap<>(internal.size());
            for (Map.Entry<String, String[]> e : internal.entrySet()) {
                snapshot.put(e.getKey(), e.getValue().clone());
            }
            return Collections.unmodifiableMap(snapshot);
        }
        return super.getParameterMap();
    }

    @Override
    public Enumeration<String> getParameterNames() {
        if (shouldIntercept()) {
            return Collections.enumeration(parameters().keySet());
        }
        return super.getParameterNames();
    }

    @Override
    public String[] getParameterValues(String name) {
        if (shouldIntercept()) {
            String[] vs = parameters().get(name);
            // Clone before return so a caller mutating the
            // result doesn't corrupt the cached map for
            // subsequent getParameter*() calls within the
            // same request. Null pass-through (the contract
            // for "no such parameter").
            return vs == null ? null : vs.clone();
        }
        return super.getParameterValues(name);
    }

    /**
     * Intercept only when ALL of:
     * <ul>
     *   <li>The body has been cached (otherwise super still
     *       owns the stream).</li>
     *   <li>HTTP method is in the configured
     *       {@code parsedBodyMethods} set. Default is {@code POST}
     *       (strict Servlet auto-parse rule). Deployers running a
     *       container that parses form bodies on PUT/PATCH/etc.
     *       (Tomcat {@code parseBodyMethods=POST,PUT}) widen the
     *       set so wrapper parameters match what the container
     *       would have produced; otherwise we'd consume the
     *       stream during pre-read AND then return no params,
     *       diverging from container behaviour. Without this knob,
     *       wrapper {@code getParameter*()} on a configured
     *       container would silently drop body params.</li>
     *   <li>Content-Type is {@code application/x-www-form-urlencoded}.
     *       Other content types (JSON, multipart, none) don't
     *       feed the parameter parser at all; super handles
     *       those correctly because parameter parsing for them
     *       only looks at the query string.</li>
     * </ul>
     */
    private boolean shouldIntercept() {
        if (cached == null) return false;
        String method = getMethod();
        if (method == null) return false;
        // parsedBodyMethods is normalised to trimmed-uppercase at
        // construction time; per-request normalise the method.
        if (!parsedBodyMethods.contains(method.toUpperCase(Locale.ROOT))) return false;
        // Token-level equality on the media type, NOT
        // startsWith on the raw value: the latter would
        // misclassify e.g. application/x-www-form-urlencoded-json
        // as form-encoded (it's a different media type the
        // container would not auto-parse). Parameters like
        // "; charset=UTF-8" are stripped by mediaTypeOf().
        String mediaType = CaptureServletFilter.mediaTypeOf(getContentType());
        return "application/x-www-form-urlencoded".equalsIgnoreCase(mediaType);
    }

    /**
     * Lazily build and cache the merged query-string + form-body
     * parameter map. Query-string and body use SEPARATE charsets
     * (see {@link #queryCharset} and {@link #charset}) because
     * the HTTP specs govern them independently and real
     * containers decode them independently.
     *
     * <p>If a pre-read I/O failure was recorded, this method
     * throws {@link IllegalStateException} (Servlet 6.1's
     * specified failure mode for {@code getParameter*()})
     * instead of parsing partial bytes. Without that guard,
     * a truncated form body would silently surface phantom
     * params — the last key/value pair (or its tail) might be
     * missing entirely, but the caller would have no way to
     * tell. Stream/reader callers ALREADY see the failure via
     * the failure-surfacing replay delegate; this guard
     * extends the same protection to {@code getParameter*()}
     * callers, who would otherwise observe a silent
     * truncation instead of the I/O error.
     */
    private Map<String, String[]> parameters() {
        if (loadFailure != null) {
            throw new IllegalStateException(
                    "request body pre-read failed during capture; "
                            + cached.length + " byte(s) buffered before failure — "
                            + "form parameters cannot be safely derived from "
                            + "partial bytes",
                    loadFailure);
        }
        if (cachedParameters != null) return cachedParameters;
        Map<String, List<String>> work = new LinkedHashMap<>();
        // Query string: decoded with the URI-encoding charset
        // (typically UTF-8 on modern containers), NOT the
        // request body charset. Mixing the two would mojibake
        // non-ASCII query values whenever the body charset
        // defaulted to ISO-8859-1.
        String qs = getQueryString();
        if (qs != null && !qs.isEmpty()) parseFormInto(qs, queryCharset(), work);
        // Body: decoded with the request body charset
        // (Content-Type charset, else ISO-8859-1 per Servlet
        // spec §3.13). We already verified Content-Type is
        // form-urlencoded in shouldIntercept().
        if (cached.length > 0) {
            String bodyStr = new String(cached, charset());
            parseFormInto(bodyStr, charset(), work);
        }
        Map<String, String[]> out = new LinkedHashMap<>(work.size());
        for (Map.Entry<String, List<String>> e : work.entrySet()) {
            out.put(e.getKey(), e.getValue().toArray(new String[0]));
        }
        cachedParameters = out;
        return cachedParameters;
    }

    /**
     * Charset used to decode percent-escapes in the URL query
     * string. Defaults to UTF-8 (matches modern container norms
     * — Tomcat 8+, Jetty 10+, Undertow — and the WHATWG URL
     * standard for {@code application/x-www-form-urlencoded}).
     *
     * <p>Configurable via the four-arg
     * {@link CaptureServletFilter#CaptureServletFilter(com.trafficmorph.capture.CaptureLogger, boolean, int, Charset)}
     * constructor for deployments running a container with a
     * non-UTF-8 URIEncoding (Tomcat configured with
     * {@code <Connector URIEncoding="ISO-8859-1"/>}, mainframe
     * front-ends, etc.). Without override, wrapper
     * {@code getParameter*()} would diverge from the container's
     * own decoding on those deployments.
     *
     * <p>There's no portable Servlet API getter that surfaces the
     * container's configured URIEncoding, hence the explicit knob.
     */
    private Charset queryCharset() {
        return configuredQueryCharset;
    }

    /**
     * Parse a form-encoded string ({@code key1=val1&key2=val2})
     * into a multimap. Each key/value pair is URL-decoded with
     * the supplied charset, matching what the container does
     * for the corresponding source (URI for query, body
     * charset for form-encoded request body).
     *
     * <p>Malformed-encoding behaviour is governed by
     * {@link #lenientParameterDecoding}: in strict mode (default),
     * {@link java.net.URLDecoder#decode} failures propagate as
     * {@link IllegalStateException} (wrapping the underlying
     * {@link IllegalArgumentException}) — matching the Servlet
     * 6.1 contract for {@code getParameter*()} parse failures.
     * In lenient mode the malformed pair falls back to its raw
     * encoded substring and parsing continues over the remaining
     * pairs; useful for shadow-traffic capture where the filter
     * must not perturb request error semantics regardless of
     * payload validity.
     */
    private void parseFormInto(String form, Charset cs, Map<String, List<String>> out) {
        for (String pair : form.split("&")) {
            if (pair.isEmpty()) continue;
            int eq = pair.indexOf('=');
            String key, val;
            if (eq < 0) {
                key = decodeParam(pair, cs);
                val = "";
            } else {
                key = decodeParam(pair.substring(0, eq), cs);
                val = decodeParam(pair.substring(eq + 1), cs);
            }
            out.computeIfAbsent(key, k -> new ArrayList<>()).add(val);
        }
    }

    /**
     * URL-decode a single key or value, honouring the wrapper's
     * configured decode mode:
     * <ul>
     *   <li><b>Strict (default):</b>
     *       {@link java.net.URLDecoder#decode}'s
     *       {@link IllegalArgumentException} is wrapped as an
     *       {@link IllegalStateException} (cause preserved) and
     *       thrown. The Servlet 6.1 spec specifies
     *       {@code IllegalStateException} as the failure mode of
     *       {@code getParameter*()} on parameter-parse failures,
     *       so frameworks downstream (Spring's exception
     *       resolvers, Jakarta REST mappers) that key on the
     *       spec-standard exception type translate it
     *       consistently to HTTP 400 — same outcome as a real
     *       container's own form parser failing on the same input.
     *       Raw IAE would slip through such mappers and surface
     *       as a generic 500.</li>
     *   <li><b>Lenient:</b> on failure, the raw encoded
     *       substring is returned verbatim. Downstream code
     *       sees a partially-decoded view but the request
     *       proceeds. Hides malformed-input signal from
     *       downstream — intended for use cases where capture
     *       must absolutely not change request semantics.</li>
     * </ul>
     */
    private String decodeParam(String s, Charset cs) {
        try {
            return URLDecoder.decode(s, cs);
        } catch (IllegalArgumentException iae) {
            if (lenientParameterDecoding) return s;
            // Wrap as IllegalStateException to match the Servlet
            // 6.1 spec contract for getParameter*() failures.
            // Cause preserved so the underlying decoder message
            // ("URLDecoder: Illegal hex characters in escape (%)
            // pattern - ...") remains visible in logs and
            // exception chains.
            throw new IllegalStateException(
                    "form parameter could not be percent-decoded with charset "
                            + cs + "; encoded value was '" + s + "'", iae);
        }
    }
}
