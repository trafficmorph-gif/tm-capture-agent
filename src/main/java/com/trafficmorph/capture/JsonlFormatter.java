package com.trafficmorph.capture;

import java.util.Map;

/**
 * Formats {@link CaptureEvent} instances into TrafficMorph-compatible
 * JSONL lines. Hand-rolled, zero-dep, designed for the single writer
 * thread.
 *
 * <p>Each instance owns a {@link StringBuilder} that gets reset (not
 * reallocated) between events, so steady-state formatting is
 * allocation-free apart from the returned {@link String} itself.
 * Constructed lazily by the writer thread; never shared across
 * threads.
 *
 * <p>Output shape (one line, terminated by {@code \n}):
 * <pre>
 *   {"t":1.234,"method":"POST","url":"https://api.example.com/api/bid","headers":{"Content-Type":"application/json"},"body":"…"}
 * </pre>
 *
 * <p>Optional fields ({@code headers}, {@code body}) are omitted when
 * {@code null} — matching the parser's tolerance on the import side
 * and keeping lines minimal when callers used the cheap two-arg
 * {@code log(method, url)} overload.
 *
 * <p>Numbers: {@code t} renders with three decimal places (millisecond
 * precision), matching the bundled sample fixture and the resolution
 * of the import-side curve deriver.
 *
 * <p>String escaping: implements the JSON minimum — {@code "}, {@code \},
 * and ASCII control characters ({@code < 0x20}). High Unicode passes
 * through verbatim (the parser uses UTF-8). This matches what
 * Jackson's {@code JsonStringEncoder} produces for typical content
 * without paying for its setup cost.
 */
final class JsonlFormatter {

    // 256 bytes is enough for a typical line (method + URL + a few
    // headers + small body), avoiding the first internal grow. Larger
    // events will grow the buffer once and reuse it thereafter.
    private final StringBuilder sb = new StringBuilder(256);

    /**
     * Format one event into a JSONL line including the trailing {@code \n}.
     * Returns a fresh {@link String}; the internal builder is reset for
     * the next call.
     */
    String format(CaptureEvent e) {
        sb.setLength(0);
        sb.append("{\"t\":");
        appendSeconds(e.tSeconds());
        sb.append(",\"method\":\"");
        appendEscaped(e.method());
        sb.append("\",\"url\":\"");
        appendEscaped(e.url());
        sb.append('"');
        Map<String, String> headers = e.headers();
        if (headers != null && !headers.isEmpty()) {
            sb.append(",\"headers\":{");
            boolean first = true;
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                if (!first) sb.append(',');
                first = false;
                sb.append('"');
                appendEscaped(entry.getKey());
                sb.append("\":\"");
                appendEscaped(entry.getValue() == null ? "" : entry.getValue());
                sb.append('"');
            }
            sb.append('}');
        }
        String body = e.body();
        if (body != null) {
            sb.append(",\"body\":\"");
            appendEscaped(body);
            sb.append('"');
        }
        sb.append("}\n");
        return sb.toString();
    }

    /**
     * Render a non-negative seconds value with three decimal places.
     * Manual rather than {@code String.format("%.3f", ...)} because
     * the formatter call allocates and parses the format spec each
     * invocation — too heavy for the writer thread's per-event hot
     * path even though it's off the producer's hot path.
     */
    private void appendSeconds(double t) {
        if (Double.isNaN(t) || Double.isInfinite(t)) {
            // Defensive: shouldn't happen with monotonic time source,
            // but a corrupt event mustn't poison the line. Use 0
            // — the import-side parser would otherwise reject the
            // entire row.
            sb.append("0.000");
            return;
        }
        if (t < 0) t = 0;
        long whole = (long) t;
        long millis = Math.round((t - whole) * 1000);
        // Carry: 0.9995 rounds to whole=0, millis=1000 → fix.
        if (millis >= 1000) {
            whole += 1;
            millis -= 1000;
        }
        sb.append(whole);
        sb.append('.');
        if (millis < 100) sb.append('0');
        if (millis < 10) sb.append('0');
        sb.append(millis);
    }

    /**
     * Append {@code s}, JSON-escaping the characters that REQUIRE
     * escaping per RFC 8259: {@code "}, {@code \}, and any ASCII
     * control character ({@code < 0x20}). Everything else passes
     * through verbatim. This is what the parser on the import side
     * expects — Jackson's reader handles the same set.
     */
    private void appendEscaped(String s) {
        int n = s.length();
        for (int i = 0; i < n; i++) {
            char c = s.charAt(i);
            if (c == '"') {
                sb.append("\\\"");
            } else if (c == '\\') {
                sb.append("\\\\");
            } else if (c < 0x20) {
                switch (c) {
                    case '\n' -> sb.append("\\n");
                    case '\r' -> sb.append("\\r");
                    case '\t' -> sb.append("\\t");
                    case '\b' -> sb.append("\\b");
                    case '\f' -> sb.append("\\f");
                    default -> {
                        // For the rest of the ASCII control range
                        // emit the standard six-char JSON escape
                        // (backslash, lowercase u, four hex digits).
                        // The prefix is built in two pieces because
                        // the Java lexer pre-processes a literal
                        // backslash-u-XXXX-style sequence as a
                        // unicode escape even inside strings AND
                        // comments — a single-token literal would
                        // be misread.
                        sb.append('\\').append("u00");
                        sb.append(HEX[(c >>> 4) & 0xF]);
                        sb.append(HEX[c & 0xF]);
                    }
                }
            } else {
                sb.append(c);
            }
        }
    }

    private static final char[] HEX = "0123456789abcdef".toCharArray();
}
