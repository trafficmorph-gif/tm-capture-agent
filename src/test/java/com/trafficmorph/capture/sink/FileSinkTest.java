package com.trafficmorph.capture.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Step 5 — rolling file sink.
 *
 * <p>All tests use {@link TempDir} so they're isolated from each
 * other and don't leave artefacts on the developer's disk.
 */
class FileSinkTest {

    private static String read(Path file) throws IOException {
        return Files.readString(file, StandardCharsets.UTF_8);
    }

    /** Snapshot of the files inside {@code dir} (NOT recursive). */
    private static List<Path> filesIn(Path dir) throws IOException {
        try (var s = Files.list(dir)) {
            return s.toList();
        }
    }

    @Test
    void plainModeWritesAllLinesToTheActiveFile(@TempDir Path tmp) throws Exception {
        Path active = tmp.resolve("capture.jsonl");
        try (FileSink sink = FileSink.builder(active).build()) {
            sink.write("{\"t\":0.0,\"method\":\"GET\",\"url\":\"https://x\"}\n");
            sink.write("{\"t\":1.0,\"method\":\"GET\",\"url\":\"https://x\"}\n");
        }
        assertTrue(Files.exists(active), "active file must exist after close");
        String content = read(active);
        assertEquals(2, content.split("\n").length, "two events written; content=\n" + content);
        // Only the active file — no rotated siblings should appear
        // when rotation triggers are unset.
        assertEquals(1, filesIn(tmp).size(), "no rotated siblings expected");
    }

    @Test
    void parentDirectoriesAreCreatedIfMissing(@TempDir Path tmp) throws Exception {
        // Active path is two levels deep under the temp dir, and
        // neither intermediate directory exists yet.
        Path active = tmp.resolve("a").resolve("b").resolve("capture.jsonl");
        try (FileSink sink = FileSink.builder(active).build()) {
            sink.write("{\"t\":0.0}\n");
        }
        assertTrue(Files.exists(active), "active file must exist after deep-path create");
    }

    @Test
    void sizeBasedRotationProducesStampedSibling(@TempDir Path tmp) throws Exception {
        Path active = tmp.resolve("capture.jsonl");
        // Tiny cap so a few short writes trigger rotation.
        try (FileSink sink = FileSink.builder(active).maxBytes(50).build()) {
            // Each line is ~30 bytes — first write fits, second
            // exceeds the 50-byte cap and triggers rotation BEFORE
            // its own bytes land in the new file.
            sink.write("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n");   // ~31 bytes
            sink.flush();
            sink.write("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\n");   // pushes over the cap
            sink.flush();
            sink.write("cccccccccccccccccccccccccccccc\n");
        }
        List<Path> all = filesIn(tmp);
        // Active + one rotated.
        assertEquals(2, all.size(), "expected one rotation; got: " + all);
        Path rolled = all.stream()
                .filter(p -> !p.equals(active))
                .findFirst().orElseThrow();
        String rolledName = rolled.getFileName().toString();
        // Stamped name shape: capture-YYYYMMDDTHHMMSS.SSSZ.jsonl
        assertTrue(rolledName.matches("capture-\\d{8}T\\d{6}\\.\\d{3}Z\\.jsonl"),
                "rolled name should be UTC-stamped: " + rolledName);
        // Rotation check fires BEFORE each write, so:
        //   - "a" (counter=0) → no rotate → into file, counter→31
        //   - "b" (counter=31 < 50) → no rotate → into file, counter→62
        //   - "c" (counter=62 >= 50) → ROTATE first → into NEW file
        // Result: rolled file holds "a" and "b"; new active holds "c".
        String rolledContent = read(rolled);
        String activeContent = read(active);
        assertTrue(rolledContent.contains("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                "first batch lives in the rolled file");
        assertTrue(rolledContent.contains("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                "second batch ALSO lives in the rolled file (cap not yet reached when it was written)");
        assertTrue(activeContent.contains("cccccccccccccccccccccccccccccc"),
                "write that finally tripped the cap rotates BEFORE itself, lands in the new active");
        assertFalse(activeContent.contains("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                "new active should not contain pre-rotation content");
    }

    @Test
    void timeBasedRotationFiresAfterMaxAgeElapses(@TempDir Path tmp) throws Exception {
        Path active = tmp.resolve("capture.jsonl");
        try (FileSink sink = FileSink.builder(active)
                .maxAge(Duration.ofMillis(100))
                .build()) {
            sink.write("line-1\n");
            // Wait past the rotation interval. The next write's
            // shouldRotate() check will fire.
            Thread.sleep(150);
            sink.write("line-2\n");
            sink.write("line-3\n");
        }
        List<Path> all = filesIn(tmp);
        assertEquals(2, all.size(), "expected one rotation; got: " + all);
        Path rolled = all.stream()
                .filter(p -> !p.equals(active))
                .findFirst().orElseThrow();
        // Rolled file has the pre-sleep line, active has post-sleep ones.
        assertEquals("line-1\n", read(rolled));
        assertEquals("line-2\nline-3\n", read(active));
    }

    @Test
    void rotationCollisionGetsSuffixedToPreserveUniqueness(@TempDir Path tmp) throws Exception {
        Path active = tmp.resolve("capture.jsonl");
        // Pre-create a file with the timestamped name FileSink would
        // otherwise produce on the next rotation. The sink should
        // pick a suffixed alternative instead of overwriting it.
        //
        // We can't predict the exact stamp, so seed every plausible
        // collision via a quick burst rotation pattern: force a
        // size-based rotation with a tiny cap.
        try (FileSink sink = FileSink.builder(active).maxBytes(5).build()) {
            sink.write("aaaaaa\n");   // triggers rotation eventually
            sink.flush();
            sink.write("bbbbbb\n");
            sink.flush();
            sink.write("cccccc\n");
            sink.flush();
            // Three rotations within a few ms — at least two of them
            // will land in the same millisecond on a fast machine.
        }
        List<Path> rolled = filesIn(tmp).stream()
                .filter(p -> !p.equals(active))
                .toList();
        // Every rolled file must have a unique name (assertion would
        // fail if FileSink overwrote a previous rotation).
        long distinctNames = rolled.stream().map(Path::getFileName).distinct().count();
        assertEquals(rolled.size(), distinctNames,
                "every rolled file must have a unique name; got " + rolled);
        assertTrue(rolled.size() >= 2,
                "expected multiple rotations from the burst; got " + rolled);
    }

    @Test
    void closeFlushesAndReleasesTheFile(@TempDir Path tmp) throws Exception {
        Path active = tmp.resolve("capture.jsonl");
        FileSink sink = FileSink.builder(active).build();
        sink.write("durable\n");
        // Before close, the line might still be in the BufferedWriter.
        // After close, it MUST be on disk.
        sink.close();
        assertEquals("durable\n", read(active),
                "close() must flush the buffer; file content must be durable");
        // close() is idempotent.
        sink.close();
    }

    @Test
    void resumingAgainstAnExistingFileAppendsAndAccountsExistingBytes(@TempDir Path tmp) throws Exception {
        Path active = tmp.resolve("capture.jsonl");
        // Pre-seed the file as if a previous JVM had been writing to it.
        Files.writeString(active, "previous-run-line\n");
        long preexistingSize = Files.size(active);

        // Open a sink with a maxBytes cap that's already exceeded by
        // the pre-existing content. The FIRST write should trigger
        // rotation immediately, because currentBytes() must include
        // bytesAtOpen.
        try (FileSink sink = FileSink.builder(active)
                .maxBytes(preexistingSize - 1)
                .build()) {
            sink.write("new-run-line\n");
            sink.flush();
        }
        List<Path> all = filesIn(tmp);
        assertEquals(2, all.size(),
                "rotation should fire on first write because bytesAtOpen >= cap; got " + all);
        Path rolled = all.stream()
                .filter(p -> !p.equals(active))
                .findFirst().orElseThrow();
        assertEquals("previous-run-line\n", read(rolled),
                "rolled file holds the pre-existing content");
        assertEquals("new-run-line\n", read(active),
                "new active file starts fresh with post-rotation writes");
    }

    @Test
    void builderRejectsZeroOrNegativeMaxAge(@TempDir Path tmp) {
        assertThrows(IllegalArgumentException.class,
                () -> FileSink.builder(tmp.resolve("x")).maxAge(Duration.ZERO));
        assertThrows(IllegalArgumentException.class,
                () -> FileSink.builder(tmp.resolve("x")).maxAge(Duration.ofMillis(-1)));
    }

    @Test
    void builderRejectsNegativeMaxBytes(@TempDir Path tmp) {
        assertThrows(IllegalArgumentException.class,
                () -> FileSink.builder(tmp.resolve("x")).maxBytes(-1));
    }

    @Test
    void writeAfterCloseIsSilentlyIgnored(@TempDir Path tmp) throws Exception {
        // Symmetric with StdoutSink: writes after close are dropped
        // silently rather than throwing. The owning CaptureLogger
        // routes them via writeFailed already if upstream care is
        // needed.
        Path active = tmp.resolve("capture.jsonl");
        FileSink sink = FileSink.builder(active).build();
        sink.write("before\n");
        sink.close();
        sink.write("after\n");    // no throw, no write
        assertEquals("before\n", read(active));
    }

    @Test
    void rotationFailureWithSuccessfulRecoveryKeepsSinkUsable(@TempDir Path tmp) throws Exception {
        // Files.move() throws mid-rotation (forced by deleting the
        // active file out from under the sink). Recovery reopens
        // the active path successfully. The sink stays usable —
        // no NPE on subsequent writes — and is NOT in the failed
        // state (so future writes can succeed normally).
        Path active = tmp.resolve("capture.jsonl");
        try (FileSink sink = FileSink.builder(active).maxBytes(5).build()) {
            sink.write("abcdef\n");      // 7 bytes -> over the 5 cap
            sink.flush();

            // External actor deletes the active file out from
            // under us; the next rotation's Files.move will fail.
            Files.delete(active);

            try {
                sink.write("ghijkl\n");
            } catch (IOException expected) {
                // Most platforms: move source missing → throws.
            }

            // CRITICAL: subsequent writes must NOT throw
            // NullPointerException. The recovery path reopened
            // the active file at the original path, so these
            // writes succeed.
            sink.write("mnopqr\n");
            sink.write("stuvwx\n");
            sink.flush();
        }
        // Got here without NPE → test passes.
    }

    @Test
    void catastrophicRotationFailureKeepsThrowingOnSubsequentWrites(@TempDir Path tmp) throws Exception {
        // The previous adjustment introduced `closed=true` after
        // catastrophic rotation-and-recovery failure. That caused
        // ongoing data loss to silently vanish — the writer-thread
        // pipeline's writeFailed counter only increments when
        // sink.write() throws, so a single failure spike was
        // followed by silent dropping forever.
        //
        // The fix: failed != userClosed. A failed sink must keep
        // throwing on every write so writeFailed keeps climbing
        // and the operator sees an ongoing alarm.
        //
        // Forcing the catastrophic state: put the active file
        // inside a subdirectory. Mid-test, replace the
        // subdirectory with a regular file. Then BOTH
        // Files.move (source gone) AND openNewFile (createDirectories
        // can't create a dir where a file is in the way) fail.
        Path subdir = tmp.resolve("sub");
        Files.createDirectory(subdir);
        Path active = subdir.resolve("capture.jsonl");

        FileSink sink = FileSink.builder(active).maxBytes(5).build();
        sink.write("abcdef\n");   // 7 bytes -> over the 5 cap
        sink.flush();

        // Replace the subdirectory with a regular file. Both
        // rotation phases will fail.
        Files.delete(active);
        Files.delete(subdir);
        Files.writeString(subdir, "not-a-directory");

        // First write triggers rotation. Move fails (source dir
        // gone) AND recovery's openNewFile fails (parent path is
        // a file, not a directory). Sink → failed state.
        IOException first = assertThrows(IOException.class,
                () -> sink.write("trigger\n"));
        // The throw should carry a meaningful cause for telemetry.
        assertNotNull(first, "first write after catastrophic failure must throw");

        // CRITICAL: subsequent writes ALSO throw. The old code
        // would silently drop them via closed=true, leaving the
        // operator blind to ongoing data loss.
        IOException second = assertThrows(IOException.class,
                () -> sink.write("again\n"));
        assertNotNull(second.getCause(),
                "subsequent throws should carry the cached root cause: " + second);
        assertTrue(second.getMessage().contains("failed state"),
                "subsequent throws should self-describe as a failed state: " + second.getMessage());

        // Yet another write — proves the keep-throwing contract
        // isn't a one-shot.
        IOException third = assertThrows(IOException.class,
                () -> sink.write("and again\n"));
        assertNotNull(third);

        // User close() must still work — it's the operator's way
        // to release the broken sink.
        sink.close();
        // After user close: writes silently drop (the userClosed
        // contract, NOT the failed contract). No exception.
        sink.write("after-close\n");
    }

    // ── flushAndClose helper ─────────────────────────────────────

    @Test
    void flushAndCloseReturnsNullWhenInputIsNull() {
        assertNull(FileSink.flushAndClose(null));
    }

    @Test
    void flushAndCloseReturnsNullWhenBothSucceed() throws IOException {
        BufferedWriter w = new BufferedWriter(new StringWriter());
        w.write("ok");
        assertNull(FileSink.flushAndClose(w),
                "no failure to report when flush/close both succeed");
    }

    @Test
    void flushAndCloseReturnsFlushFailureWithCloseAsSuppressed() {
        // Writer that throws on both flush and close. BufferedWriter
        // delegates flush() → out.flush() and close() → flushBuffer
        // + out.close(); both reach our throwing methods so we hit
        // the first-fails-with-second-suppressed branch of
        // flushAndClose.
        Writer thrower = new Writer() {
            @Override public void write(char[] cbuf, int off, int len) {}
            @Override public void flush() throws IOException { throw new IOException("flush fail"); }
            @Override public void close() throws IOException { throw new IOException("close fail"); }
        };
        BufferedWriter wrapped = new BufferedWriter(thrower);
        IOException result = FileSink.flushAndClose(wrapped);
        assertNotNull(result, "drain failure must be reported, not swallowed");
        assertTrue(result.getMessage().contains("flush"),
                "flush failure should be the primary (first encountered); got: " + result.getMessage());
        Throwable[] suppressed = result.getSuppressed();
        assertEquals(1, suppressed.length,
                "close failure should be attached as a single suppressed throwable");
        assertTrue(suppressed[0].getMessage().contains("close"),
                "suppressed throwable should be the close failure; got: " + suppressed[0].getMessage());
    }

    @Test
    void flushAndCloseReturnsCloseFailureWhenOnlyCloseFails() throws IOException {
        // Flush succeeds, close fails — the close failure becomes
        // the primary, no suppressed.
        Writer w = new Writer() {
            @Override public void write(char[] cbuf, int off, int len) {}
            @Override public void flush() {}
            @Override public void close() throws IOException { throw new IOException("close only"); }
        };
        BufferedWriter wrapped = new BufferedWriter(w);
        IOException result = FileSink.flushAndClose(wrapped);
        assertNotNull(result);
        assertTrue(result.getMessage().contains("close only"));
        assertEquals(0, result.getSuppressed().length);
    }

    @Test
    void counterTracksRealBytesNotChars(@TempDir Path tmp) throws Exception {
        // Multi-byte UTF-8 characters: a 4-char string of em-dashes
        // (each 3 bytes in UTF-8) writes 12 bytes, not 4. The
        // size-based rotation MUST react to bytes — a chars-based
        // check would let multi-byte content blow past the cap.
        Path active = tmp.resolve("capture.jsonl");
        try (FileSink sink = FileSink.builder(active).maxBytes(10).build()) {
            // 6 em-dashes + newline = 6 * 3 + 1 = 19 bytes — past the cap.
            sink.write("——————\n");
            sink.flush();
            sink.write("after\n");
            sink.flush();
        }
        // Rotation should have fired.
        assertEquals(2, filesIn(tmp).size(),
                "byte-based size accounting must see UTF-8 multi-byte chars exceed the cap");
    }
}
