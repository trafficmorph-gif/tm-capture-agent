package com.trafficmorph.capture;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

/**
 * Per-header value transform applied during {@link CaptureLogger#log}'s
 * snapshot pass. Lets consumers strip credential-carrying headers
 * from captures before they're written to disk / shipped to a
 * collector — captures are designed for replay against staging
 * infrastructure, so leaking production auth tokens into them is a
 * real concern.
 *
 * <p>Functional interface — most policies can be written as a
 * one-line lambda:
 * <pre>
 *   logger = CaptureLogger.builder()
 *       .headerRedaction((name, value) ->
 *           name.equalsIgnoreCase("Authorization") ? "[redacted]" : value)
 *       .build();
 * </pre>
 *
 * <h2>Contract</h2>
 * <ul>
 *   <li>Return the original {@code value} (including {@code null})
 *       to keep the header verbatim. A null value renders as an
 *       empty string in the captured JSONL — same shape the
 *       formatter would have written without redaction.</li>
 *   <li>Return a different string to replace the value (the header
 *       name stays in the output, the value changes).</li>
 *   <li>Return {@link #DROP} to omit the header entirely (the name
 *       won't appear in the captured JSONL line at all). Must be
 *       returned via the {@code DROP} field reference — identity-
 *       compared so a same-content string from elsewhere doesn't
 *       trigger an accidental drop.</li>
 *   <li>Must NOT throw — anything thrown lands in the
 *       {@code invalidDropped} counter via the producer-side
 *       try/catch, and the event is lost.</li>
 *   <li>Must be thread-safe — called from arbitrary producer threads.</li>
 * </ul>
 *
 * <p>Null-key entries in the caller's headers map are skipped by
 * the snapshot pass BEFORE the policy is invoked — the policy's
 * {@code headerName} parameter is therefore guaranteed non-null.
 * Null-value entries DO reach the policy.
 *
 * <h2>Performance note</h2>
 * <p>Called once per header per logged event on the hot path. The
 * default {@link #defaultSafelist()} uses a case-insensitive
 * {@link java.util.TreeSet} lookup, no allocation per call.
 * Custom policies should aim for the same: avoid {@code toLowerCase()}
 * (allocates), prefer {@code equalsIgnoreCase} or pre-built
 * case-insensitive sets.
 */
@FunctionalInterface
public interface RedactionPolicy {

    /**
     * Sentinel return value: returning this from
     * {@link #redact(String, String)} omits the header entirely
     * from the captured JSONL line. Distinct from {@code null},
     * which is treated as a legitimate header value to preserve.
     *
     * <p>Identity-compared (not equality-compared) in the
     * caller, so policies MUST return this exact reference to
     * trigger a drop. A coincidentally-equal string from elsewhere
     * will NOT trigger a drop — protecting against accidental
     * drops if a real-world header value happened to match this
     * sentinel's content.
     *
     * <p>Wrapped in an explicit {@code new String(...)} so the
     * reference is unique across the JVM — the constant-pool
     * de-duping the compiler does for plain string literals would
     * otherwise let two distinct fields with the same content
     * share a reference. The literal content is plain printable
     * ASCII; source-scanning tools (git diff, IDE, grep, ...)
     * treat this file as text.
     */
    @SuppressWarnings("StringOperationCanBeSimplified")
    String DROP = new String("__TM_CAPTURE_DROP__");

    /**
     * Inspect one header and decide its fate.
     *
     * @param headerName  header name as provided by the caller —
     *                    guaranteed non-null by the caller-side
     *                    snapshot pass, which skips null-key
     *                    entries before invoking this method.
     * @param value       header value as provided by the caller.
     *                    May be {@code null}; treat defensively.
     * @return replacement value (possibly the original, possibly
     *         {@code null} to preserve a null entry), or
     *         {@link #DROP} to omit the header from the output.
     */
    String redact(String headerName, String value);

    /**
     * No-op policy — every header value passes through verbatim.
     * Useful for internal-network captures where headers are already
     * safe and the redaction overhead is unwanted.
     */
    static RedactionPolicy none() {
        return (name, value) -> value;
    }

    /**
     * Default safelist used when the caller doesn't supply a custom
     * policy. Replaces values of headers commonly known to carry
     * credentials / session state with the literal string
     * {@code "[REDACTED]"} — keeps the header name in the output
     * (so the capture's shape stays faithful) but drops the secret.
     *
     * <p>Matched header names (case-insensitive):
     * <ul>
     *   <li>{@code Authorization} / {@code Proxy-Authorization}</li>
     *   <li>{@code Cookie} / {@code Set-Cookie}</li>
     *   <li>{@code X-Api-Key}, {@code X-Auth-Token}</li>
     *   <li>{@code X-Csrf-Token}, {@code X-Amz-Security-Token}</li>
     * </ul>
     */
    static RedactionPolicy defaultSafelist() {
        return DefaultSafelist.INSTANCE;
    }

    /**
     * Hidden default-policy implementation. Pulled into its own
     * static nested class so the TreeSet is created exactly once
     * even if the public factory is called many times.
     */
    final class DefaultSafelist implements RedactionPolicy {
        static final DefaultSafelist INSTANCE = new DefaultSafelist();
        private static final String REDACTED = "[REDACTED]";

        private static final Set<String> NAMES;
        static {
            // String.CASE_INSENSITIVE_ORDER means contains() is
            // case-insensitive without any per-query allocation
            // (toLowerCase() would allocate on every header check).
            TreeSet<String> s = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            s.add("Authorization");
            s.add("Proxy-Authorization");
            s.add("Cookie");
            s.add("Set-Cookie");
            s.add("X-Api-Key");
            s.add("X-Auth-Token");
            s.add("X-Csrf-Token");
            s.add("X-Amz-Security-Token");
            NAMES = Collections.unmodifiableSet(s);
        }

        private DefaultSafelist() {}

        @Override
        public String redact(String headerName, String value) {
            if (headerName == null) return value;
            return NAMES.contains(headerName) ? REDACTED : value;
        }
    }
}
