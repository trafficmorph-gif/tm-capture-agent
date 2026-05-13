package com.trafficmorph.capture.servlet;

import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Predicate;

/**
 * {@link ServletInputStream} that replays bytes from an in-memory
 * buffer captured upstream by {@link BodyCachingRequestWrapper}.
 *
 * <p>Async-IO compatible: when an async endpoint registers a
 * {@link ReadListener}, the listener events fire synchronously
 * because the body is already buffered. The consumer sees the same
 * shape as a "data arrived in one chunk" event sequence —
 * {@code onDataAvailable()} → consumer reads → {@code onAllDataRead()}.
 * Any {@link IOException} the consumer throws surfaces via
 * {@code onError()} per the Servlet spec. Partial-bytes / pre-read
 * failures surface as {@code onError} too (see {@link #loadFailure}).
 *
 * <p>This is a minimal-correctness implementation, not a full
 * async-IO simulator: events fire on the caller's thread, not on
 * a container worker. For genuine non-blocking I/O benefits an
 * async endpoint should not pass through the body-capturing
 * wrapper — set {@code captureBody=false} for those routes.
 *
 * <p>Package-private: this class is an implementation detail of
 * {@link CaptureServletFilter} and only constructed by
 * {@link BodyCachingRequestWrapper#getInputStream()}.
 */
final class ReplayServletInputStream extends ServletInputStream {

    private final InputStream delegate;

    /**
     * Source request used solely to verify the async/upgrade
     * precondition on {@link #setReadListener}. Per the Servlet
     * spec, registering a {@link ReadListener} is only valid
     * once the request is in async processing
     * ({@code req.startAsync()} called) or after an HTTP upgrade.
     */
    private final HttpServletRequest source;

    /**
     * Non-null iff the original request body pre-read failed
     * mid-stream. Drives two things:
     * <ul>
     *   <li>{@link #isFinished()} returns {@code false} when
     *       a failure is pending — keeps polling consumers
     *       calling {@link #read} (which then throws via the
     *       failure-surfacing delegate). Without this, a
     *       consumer that checked {@code isFinished} before
     *       reading would see "yes, done" and skip the throw.</li>
     *   <li>{@link #setReadListener}'s drain branch fires
     *       {@code onError} instead of {@code onAllDataRead}
     *       — async consumers see the I/O failure path, not a
     *       false "all data read" signal.</li>
     * </ul>
     * The actual {@link IOException} that surfaces on read
     * comes from the failure-surfacing delegate, NOT this
     * field directly — the delegate has the partial-bytes
     * count baked into its message.
     */
    private final IOException loadFailure;

    /**
     * Deployer-supplied predicate that identifies genuine
     * upgrade-capable requests. When {@code null} (the strict
     * default), every request must pass {@code isAsyncStarted()}
     * before {@code setReadListener} succeeds. When non-null and
     * it returns {@code true} for {@link #source}, the async
     * precondition is skipped. Exceptions thrown by the predicate
     * are treated as {@code false} — fail-closed.
     */
    private final Predicate<HttpServletRequest> upgradeRoutePredicate;

    /**
     * Flipped {@code true} as soon as {@link #setReadListener}
     * accepts a first listener. Separate from any "data drained"
     * tracking on purpose — the Servlet spec forbids registering
     * more than one listener per stream regardless of whether
     * the first listener read everything, threw, or returned
     * with bytes still pending. Conflating the two states would
     * let a second listener slip through whenever the first
     * didn't drain to EOF.
     */
    private boolean listenerSet;

    ReplayServletInputStream(InputStream delegate,
                              HttpServletRequest source,
                              Predicate<HttpServletRequest> upgradeRoutePredicate,
                              IOException loadFailure) {
        this.delegate = delegate;
        this.source = source;
        this.upgradeRoutePredicate = upgradeRoutePredicate;
        this.loadFailure = loadFailure;
    }

    /**
     * Evaluate the deployer's upgrade-route predicate against the
     * source request, returning {@code true} only if it both
     * exists and returns {@code true}. Any exception from
     * {@code predicate.test()} is swallowed and treated as
     * {@code false} so that a buggy predicate fails CLOSED
     * (strict-mode rejection) rather than OPEN (silent bypass).
     */
    private boolean shouldBypassAsyncPrecondition() {
        if (upgradeRoutePredicate == null) return false;
        try {
            return upgradeRoutePredicate.test(source);
        } catch (Throwable ignored) {
            // Deployer's predicate threw — treat as "no opt-in
            // for this request". Strict check runs next.
            return false;
        }
    }

    @Override
    public int read() throws IOException {
        // The delegate is either a plain ByteArrayInputStream
        // (happy path) or a failure-surfacing wrapper (after a
        // pre-read I/O failure). In the latter case, the read
        // that would otherwise hit EOF throws — propagating
        // the I/O failure to downstream code instead of
        // masking it as a clean EOF.
        return delegate.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return delegate.read(b, off, len);
    }

    @Override
    public int available() throws IOException {
        return delegate.available();
    }

    @Override
    public boolean isFinished() {
        // When a pre-read failure is pending, the stream is NOT
        // "all data read" — there were upstream bytes we never
        // got. Returning false here keeps a consumer that
        // checks isFinished before reading from skipping the
        // throw. Once they call read(), the failure surfaces
        // via the delegate.
        if (loadFailure != null) return false;
        try {
            return delegate.available() == 0;
        } catch (IOException ioe) {
            // delegate.available throwing is unusual but
            // semantically equivalent to "not finished" —
            // there's a failure to surface on the next read.
            return false;
        }
    }

    @Override
    public boolean isReady() {
        // In-memory buffer is always non-blocking. After
        // exhaustion, isFinished() flips true and the
        // consumer's read() returns -1 — callers loop on
        // (isReady() && !isFinished()) and exit on EOF.
        return true;
    }

    @Override
    public void setReadListener(ReadListener listener) {
        if (listener == null) {
            throw new NullPointerException("listener");
        }
        // Spec precondition: ReadListener may only be set when
        // the request is in non-blocking processing — async
        // dispatch started (isAsyncStarted() == true) OR HTTP
        // upgrade in progress.
        //
        // We can verify async directly. Upgrade is harder:
        //
        //   - There is NO portable getter on HttpServletRequest
        //     for "upgrade has been accepted by the server".
        //     {@code req.upgrade(...)} is a server-side action
        //     with no symmetric isUpgraded() readback.
        //
        //   - The {@code Connection: Upgrade} / {@code Upgrade}
        //     request headers are CLIENT-controlled — accepting
        //     them as proof would let a synchronous handler dodge
        //     the precondition by receiving a spoofed header
        //     (defeating the entire bug-catching purpose).
        //
        // Resolution: the deployer supplies a route-scoping
        // predicate at filter construction time. When the
        // predicate matches this specific request, the async
        // check is skipped (the deployer asserts this is a
        // genuine upgrade route). When it doesn't match OR no
        // predicate was supplied, the strict check applies and
        // bug-catching value is preserved. Predicate exceptions
        // fail closed (see {@link #shouldBypassAsyncPrecondition}).
        //
        // The trust model is per-request, not per-filter-instance:
        // a single filter mapping covering both upgrade and
        // ordinary routes only bypasses on the requests the
        // predicate explicitly identifies. A misconfigured wide
        // mapping cannot silently widen the bypass.
        if (!shouldBypassAsyncPrecondition() && !source.isAsyncStarted()) {
            throw new IllegalStateException(
                    "ReadListener can only be set after ServletRequest.startAsync() "
                            + "— the Servlet spec restricts non-blocking I/O to "
                            + "async processing. For upgrade-mode endpoints, "
                            + "construct the filter with an upgradeRoutePredicate "
                            + "(see the five-arg constructor) that returns true for "
                            + "this request, OR set captureBody=false for that route.");
        }
        // Reject the second-and-later registrations unconditionally:
        // the spec allows exactly one listener per stream, even if
        // the first listener never consumed anything. Setting the
        // flag BEFORE invoking the listener also avoids a re-entrant
        // setReadListener call from inside onDataAvailable (rare
        // but legal under the spec) succeeding spuriously.
        if (listenerSet) {
            throw new IllegalStateException(
                    "ReadListener already set on this stream — the Servlet spec "
                            + "permits at most one registration per request");
        }
        listenerSet = true;
        // We have every byte buffered in memory, so the spec's
        // "data available" event is immediately satisfiable —
        // BUT only when there are actually bytes to deliver.
        // The Servlet ReadListener contract is "onDataAvailable
        // is invoked when data is available to read"; firing
        // it on an empty stream with only a pending error
        // would violate that. The branch on
        // delegate.available() == 0 at entry covers:
        //   - cached == byte[0] AND loadFailure != null
        //     (runtime failure before any byte read): go
        //     straight to onError — no fake "data available"
        //     callback.
        //   - cached == byte[0] AND loadFailure == null
        //     (degenerate empty body; unreachable today
        //     because canBufferBody rejects contentLength==0,
        //     but covered defensively): fire onAllDataRead
        //     directly.
        //
        // When there IS data to deliver, the loop fires
        // onDataAvailable repeatedly until the consumer
        // either drains the buffer or refuses to make
        // progress:
        //   1. onDataAvailable() — consumer reads what it wants.
        //   2. If bytes remain AND the consumer advanced the
        //      delegate, fire onDataAvailable() again. Real
        //      containers keep firing as long as data is ready
        //      and the listener returned without an explicit
        //      stop — matches the spec's intent that the
        //      callback signals readiness, not "all-data-now".
        //   3. Progress guard: if a callback returned with the
        //      same available() count as before, the listener
        //      is refusing to read. Break to avoid an infinite
        //      loop. The consumer stays stalled — exactly the
        //      same fate as in a real container that would
        //      only re-invoke onDataAvailable when MORE data
        //      arrives (which can never happen for an in-memory
        //      buffer). Skipping the terminal branch in that
        //      case preserves the spec invariant that
        //      onAllDataRead only fires after a complete drain.
        //   4. onAllDataRead() — once the buffer is empty AND
        //      no failure is pending.
        //   5. onError() — fired either via the catch blocks
        //      below (consumer threw inside onDataAvailable)
        //      OR via the post-loop terminal branch when
        //      loadFailure is non-null. A consumer that
        //      drained the partial bytes without calling
        //      read() past the boundary (and thus didn't
        //      surface the IOException synchronously) MUST
        //      still see the failure path, otherwise truncated
        //      payloads would land as "all data read".
        //
        // NOTE: synchronous firing on the caller's thread is a
        // documented simplification. A full container would
        // dispatch these on the servlet worker pool.
        try {
            while (delegate.available() > 0) {
                int previousAvailable = delegate.available();
                listener.onDataAvailable();
                int currentAvailable = delegate.available();
                // Listener returned without reading anything.
                // Refusing-consumer path — don't infinite-loop;
                // don't fire onAllDataRead (data still pending).
                if (currentAvailable > 0 && currentAvailable == previousAvailable) {
                    break;
                }
            }
            if (delegate.available() == 0) {
                if (loadFailure != null) {
                    try {
                        listener.onError(new IOException(
                                "request body pre-read failed; the failure-surfacing "
                                        + "replay delivered all available bytes before "
                                        + "this onError",
                                loadFailure));
                    } catch (Throwable suppressed) {
                        // Listener's onError threw — nothing
                        // more we can do here.
                    }
                } else {
                    listener.onAllDataRead();
                }
            }
        } catch (IOException ioe) {
            try {
                listener.onError(ioe);
            } catch (Throwable suppressed) {
                // Listener's own onError handler threw —
                // nothing more we can do; the request will
                // propagate naturally if the listener's
                // failure is fatal to the endpoint.
            }
        } catch (RuntimeException re) {
            try {
                listener.onError(re);
            } catch (Throwable suppressed) {
                // Same reasoning.
            }
        }
    }
}
