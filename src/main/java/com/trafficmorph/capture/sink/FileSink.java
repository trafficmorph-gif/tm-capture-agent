package com.trafficmorph.capture.sink;

import java.io.BufferedWriter;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * File-backed {@link EventSink} with optional time- and size-based
 * rotation. Lines are written to a stable "active" path; when a
 * rotation trigger fires, the active file is renamed to a timestamped
 * sibling (atomically, so readers never see a partial file) and a
 * fresh active file is created.
 *
 * <h2>Rotation triggers</h2>
 * <ul>
 *   <li><b>{@code maxAge}</b> — duration since the active file was
 *       opened. Measured against {@link System#nanoTime()} so an
 *       NTP-driven wall-clock jump won't trigger spurious rolls or
 *       suppress legitimate ones.</li>
 *   <li><b>{@code maxBytes}</b> — bytes written to the active file
 *       (true bytes, not chars — UTF-8 encoded via a counting
 *       {@link OutputStream}).</li>
 * </ul>
 * Either trigger may be left unset. With both unset, the sink
 * appends to one file forever — useful for short-lived tests or
 * containerised workloads where an external log shipper handles
 * rotation.
 *
 * <h2>Rotated file naming</h2>
 * <p>Active path: {@code /var/log/tm/capture.jsonl}<br>
 * Rotated:        {@code /var/log/tm/capture-20260512T153045.123Z.jsonl}
 *
 * <p>UTC, millisecond precision. If two rotations land in the same
 * millisecond (rare even under aggressive rotation), a {@code -1},
 * {@code -2}, … suffix is appended to keep names unique.
 *
 * <h2>Atomic rename</h2>
 * <p>Rotation calls {@code Files.move(active, rolled, ATOMIC_MOVE)}.
 * Atomicity is guaranteed within a single filesystem on POSIX (and
 * Windows ≥ Vista with NTFS). The active path and rotated path live
 * in the same directory so this always applies — never cross
 * filesystems.
 *
 * <h2>Thread safety</h2>
 * <p>Per the {@link EventSink} contract, only the single writer
 * thread of {@link com.trafficmorph.capture.CaptureLogger} calls
 * {@code write/flush/close}. No internal synchronisation here —
 * adding any would just slow the hot path.
 */
public final class FileSink implements EventSink {

    private static final DateTimeFormatter STAMP =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'.'SSS'Z'").withZone(ZoneOffset.UTC);

    private final Path activePath;
    private final Duration maxAge;
    private final long maxBytes;

    private BufferedWriter writer;
    private CountingStream counter;
    /** Initial size of the active file at the most recent open — see {@link #currentBytes()}. */
    private long bytesAtOpen;
    /** {@link System#nanoTime()} reading from the most recent open. */
    private long openedAtNanos;

    /**
     * Set by an explicit {@link #close()} call. Writes after this
     * silently drop — the operator INTENDS the sink to stop.
     */
    private volatile boolean userClosed;

    /**
     * Set when rotation fails AND recovery also fails (or when
     * post-move {@code openNewFile()} fails after a successful
     * rename). Distinct from {@link #userClosed} because writes in
     * this state must KEEP THROWING — the writer thread's
     * {@code writeFailed} counter is the operator's only signal
     * that the sink is broken, and a silent-drop would hide ongoing
     * data loss behind a single initial alarm spike.
     */
    private volatile boolean failed;

    /**
     * Cached root-cause of the {@link #failed} state, attached as
     * the {@code cause} of every subsequent throw so the operator
     * can trace the broken sink back to the original rotation
     * failure without scanning logs.
     */
    private volatile IOException failureCause;

    private FileSink(Builder b) throws IOException {
        this.activePath = b.activePath;
        this.maxAge = b.maxAge;
        this.maxBytes = b.maxBytes;
        openNewFile();
    }

    /**
     * Start a builder for a rolling file sink writing to {@code activePath}.
     * Without further calls the sink is "plain append, no rotation"
     * — explicitly enable rotation triggers via
     * {@link Builder#maxAge}, {@link Builder#maxBytes}, or both.
     */
    public static Builder builder(Path activePath) {
        return new Builder(activePath);
    }

    @Override
    public void write(String line) throws IOException {
        if (userClosed) return;
        throwIfFailed();
        if (writer == null) return;        // defensive — shouldn't happen if neither flag is set
        if (shouldRotate()) rotate();
        // rotate() may have flipped `failed` (the recovery-also-
        // failed path). Re-check before touching the writer.
        if (userClosed) return;
        throwIfFailed();
        if (writer == null) return;
        writer.write(line);
    }

    @Override
    public void flush() throws IOException {
        if (userClosed) return;
        throwIfFailed();
        if (writer != null) writer.flush();
    }

    @Override
    public void close() throws IOException {
        if (userClosed) return;
        userClosed = true;
        // Capture the writer reference before nulling. Use the
        // same flushAndClose helper rotation uses so a flush /
        // close failure here also surfaces to the caller (matches
        // the contract that close() may throw).
        BufferedWriter w = writer;
        writer = null;
        IOException failure = flushAndClose(w);
        if (failure != null) throw failure;
    }

    /**
     * If the sink is in the {@code failed} state, throw a fresh
     * {@link IOException} whose cause is the cached root failure.
     * Each call allocates — but the {@code failed} state IS the
     * alarm state, off the producer hot path, and the writer thread
     * NEEDS continual signal here to keep {@code writeFailed}
     * climbing. Cheap signal in steady state is the wrong objective.
     */
    private void throwIfFailed() throws IOException {
        if (failed) {
            throw new IOException(
                    "FileSink is in a failed state from a prior rotation/recovery error",
                    failureCause);
        }
    }

    // ── Rotation ────────────────────────────────────────────────────

    private boolean shouldRotate() {
        if (maxAge != null) {
            long elapsedNanos = System.nanoTime() - openedAtNanos;
            if (elapsedNanos >= maxAge.toNanos()) return true;
        }
        if (maxBytes > 0 && currentBytes() >= maxBytes) {
            return true;
        }
        return false;
    }

    private long currentBytes() {
        // bytesAtOpen handles "active file already had content when
        // we opened it" (e.g. JVM restart resumed an existing log);
        // counter tracks new bytes written through this open. The
        // buffer is intentionally ignored — its content hasn't hit
        // disk yet, so it doesn't count against the rotation cap.
        // Worst case lag: one buffer's worth of bytes past the cap,
        // bounded by the BufferedWriter's 8 KB buffer.
        return bytesAtOpen + counter.getCount();
    }

    private void rotate() throws IOException {
        // Three-phase rotation with explicit failure handling so a
        // mid-rotation throw can't strand the sink with writer=null
        // (an earlier bug would have made every subsequent write()
        // NPE) AND can't silently swallow buffered-data loss from
        // the drain step (the EventSink contract requires surfacing
        // I/O failures, not hiding them).
        //
        // Sink state is tracked by two ORTHOGONAL flags:
        //   - userClosed: operator called close(). Writes silently
        //                 drop (intended; the operator wants the
        //                 sink to stop).
        //   - failed:     catastrophic rotation/recovery failure.
        //                 Writes KEEP THROWING so the writer-thread
        //                 pipeline's writeFailed counter keeps
        //                 climbing and the alarm stays loud.
        //
        // Invariants after rotate() returns or throws:
        //   - writer != null AND !userClosed AND !failed → sink usable
        //   - failed == true                              → sink in alarm
        //                                                   state, every
        //                                                   future write
        //                                                   throws with
        //                                                   failureCause
        //                                                   attached
        //   - userClosed == true                          → only reachable
        //                                                   via close(),
        //                                                   not via rotate()
        // The state "writer == null AND !userClosed AND !failed" is
        // unreachable: every path through rotate() either sets failed
        // or successfully reassigns writer.

        // Phase 1: drain + close the current writer. Capture any
        // flush/close failure so we can surface it on the way out —
        // a drain failure during rotation means buffered bytes
        // didn't make it to the rolled file, which the writer-thread
        // pipeline needs to see as writeFailed.
        IOException drainFailure = flushAndCloseCurrent();

        // Phase 2: rename active → rolled. If this fails, the
        // active file is still at activePath. Try to reopen it so
        // the sink keeps writing (over the rotation cap, but
        // functional); if reopen ALSO fails, mark failed=true with
        // the move failure as the cached cause so subsequent writes
        // THROW (carrying the cause) rather than NPE — keeps the
        // writer-thread's writeFailed counter climbing. The drain
        // failure (if any) is attached as suppressed.
        Path rolledPath = pickRolledPath();
        try {
            // ATOMIC_MOVE: a reader watching the directory either
            // sees the rotated name or doesn't; never a half-
            // written intermediate. Same-filesystem rename is
            // constant-time on POSIX so this is cheap.
            Files.move(activePath, rolledPath, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException moveFailure) {
            tryReopenForRecovery(moveFailure);
            if (drainFailure != null) moveFailure.addSuppressed(drainFailure);
            throw moveFailure;
        }

        // Phase 3: open a fresh active file. If this fails after a
        // successful move, the active path is gone and we can't
        // create a replacement — mark FAILED (not user-closed!) and
        // propagate. The "failed" state makes future writes throw
        // (with this open failure as the cause) so the writer
        // thread's writeFailed counter keeps incrementing.
        try {
            openNewFile();
        } catch (IOException openFailure) {
            failed = true;
            failureCause = openFailure;
            if (drainFailure != null) openFailure.addSuppressed(drainFailure);
            throw openFailure;
        }

        // Move + open both succeeded. If drain failed we still
        // need to tell the caller — buffered bytes were lost
        // during the rotation, which the contract says they must
        // be able to detect.
        if (drainFailure != null) throw drainFailure;
    }

    /**
     * Phase 1 of rotation: flush + close the existing writer, null
     * the field, and return the first I/O failure encountered (with
     * any later failure attached as a suppressed throwable). Returns
     * {@code null} when both flush and close succeeded or when there
     * was no writer to close.
     *
     * <p>The previous version of this method swallowed both
     * exceptions — making rotation report success even when buffered
     * bytes had been lost during flush. The EventSink contract says
     * implementations must surface I/O failures; this restores that
     * contract.
     */
    private IOException flushAndCloseCurrent() {
        BufferedWriter old = writer;
        if (old == null) return null;
        // Null the field BEFORE the IO so a partial close still
        // leaves the invariant intact: writer stays null until
        // either openNewFile reassigns it (success path) or
        // failed flips to true (catastrophic path).
        writer = null;
        return flushAndClose(old);
    }

    /**
     * Pure helper: flush + close in order, returning the first
     * {@link IOException} encountered (with the second as suppressed
     * if both fail). Package-private static so the
     * exception-aggregation logic can be unit-tested directly
     * without spinning up the whole rotation machinery.
     */
    static IOException flushAndClose(BufferedWriter old) {
        if (old == null) return null;
        IOException first = null;
        try {
            old.flush();
        } catch (IOException flushFailure) {
            first = flushFailure;
        }
        try {
            old.close();
        } catch (IOException closeFailure) {
            if (first == null) {
                first = closeFailure;
            } else {
                first.addSuppressed(closeFailure);
            }
        }
        return first;
    }

    /**
     * Phase 2 recovery: rename failed, active file (probably) still
     * exists at activePath. Try to reopen it so we can keep writing.
     * If the reopen itself fails, the sink is broken — set
     * {@code failed=true} with the primary move failure as the
     * cached root cause so subsequent writes throw a meaningful
     * {@link IOException} rather than silently dropping events.
     */
    private void tryReopenForRecovery(IOException primaryFailure) {
        try {
            openNewFile();
        } catch (IOException recoveryFailure) {
            failed = true;
            failureCause = primaryFailure;
            primaryFailure.addSuppressed(recoveryFailure);
        }
    }

    /**
     * Build {@code <base>-<stamp>.<ext>} alongside the active file.
     * Collision-checks against existing files in case two rotations
     * land in the same millisecond (rare but possible under aggressive
     * size-based rotation on fast writers).
     */
    private Path pickRolledPath() {
        String stamp = STAMP.format(Instant.now());
        Path candidate = activePath.resolveSibling(injectStamp(activePath.getFileName().toString(), stamp, 0));
        int n = 1;
        while (Files.exists(candidate)) {
            candidate = activePath.resolveSibling(
                    injectStamp(activePath.getFileName().toString(), stamp, n));
            n++;
        }
        return candidate;
    }

    /** {@code "capture.jsonl"} + {@code "20260512T..."} → {@code "capture-20260512T....jsonl"} */
    private static String injectStamp(String fileName, String stamp, int collisionSuffix) {
        String suffix = collisionSuffix == 0 ? "-" + stamp : "-" + stamp + "-" + collisionSuffix;
        int dot = fileName.lastIndexOf('.');
        if (dot > 0) {
            return fileName.substring(0, dot) + suffix + fileName.substring(dot);
        }
        return fileName + suffix;
    }

    private void openNewFile() throws IOException {
        Path parent = activePath.getParent();
        if (parent != null && !Files.exists(parent)) {
            Files.createDirectories(parent);
        }
        // bytesAtOpen seeds currentBytes() so a JVM restart against
        // an existing partial file doesn't reset the size budget.
        bytesAtOpen = Files.exists(activePath) ? Files.size(activePath) : 0L;

        OutputStream raw = Files.newOutputStream(activePath,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        counter = new CountingStream(raw);
        // 8 KB buffer matches StdoutSink's choice — same trade-off
        // (amortise syscall cost vs. bound the loss window on
        // crash before flush).
        writer = new BufferedWriter(new OutputStreamWriter(counter, StandardCharsets.UTF_8), 8192);
        openedAtNanos = System.nanoTime();
    }

    // ── Builder ─────────────────────────────────────────────────────

    public static final class Builder {
        private final Path activePath;
        private Duration maxAge;
        private long maxBytes;

        private Builder(Path activePath) {
            this.activePath = Objects.requireNonNull(activePath, "activePath");
        }

        /**
         * Rotate the active file after this much wall-time elapses
         * since it was last opened. {@code null} (the default)
         * disables time-based rotation.
         */
        public Builder maxAge(Duration duration) {
            if (duration != null && (duration.isNegative() || duration.isZero())) {
                throw new IllegalArgumentException(
                        "maxAge must be positive, was " + duration);
            }
            this.maxAge = duration;
            return this;
        }

        /**
         * Rotate the active file once it reaches this many bytes
         * (true UTF-8 bytes, not chars). {@code 0} (the default)
         * disables size-based rotation.
         */
        public Builder maxBytes(long bytes) {
            if (bytes < 0) {
                throw new IllegalArgumentException(
                        "maxBytes must be >= 0, was " + bytes);
            }
            this.maxBytes = bytes;
            return this;
        }

        public FileSink build() throws IOException {
            return new FileSink(this);
        }
    }

    // ── Counting stream ─────────────────────────────────────────────

    /**
     * Pass-through {@link FilterOutputStream} that tallies bytes
     * written. The writer thread is the only caller, so we don't
     * need atomicity on the counter.
     */
    private static final class CountingStream extends FilterOutputStream {
        private long count;

        CountingStream(OutputStream out) {
            super(out);
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            count++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            // Crucially override the (byte[], int, int) form too —
            // the default FilterOutputStream.write(byte[], int, int)
            // calls write(int) in a loop, defeating BufferedWriter's
            // bulk-write performance entirely.
            out.write(b, off, len);
            count += len;
        }

        long getCount() {
            return count;
        }
    }
}
