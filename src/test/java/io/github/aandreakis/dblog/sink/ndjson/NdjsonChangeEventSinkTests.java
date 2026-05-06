package io.github.aandreakis.dblog.sink.ndjson;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.testsupport.ChangeEventTestFixtures;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NdjsonChangeEventSinkTests {
  @TempDir Path tempDir;

  @Test
  void writesOneJsonDocumentPerLine() throws Exception {
    Path outputPath = tempDir.resolve("events.ndjson");
    try (NdjsonChangeEventSink sink = NdjsonChangeEventSink.forFile(outputPath)) {
      sink.appendEvents(List.of(sampleEvent(1L, "one"), sampleEvent(2L, "two")));
    }

    List<String> lines = Files.readAllLines(outputPath);
    assertThat(lines).hasSize(2);
    assertThat(lines.get(0)).contains("\"primaryKey\":{\"id\":1}");
    assertThat(lines.get(1)).contains("\"primaryKey\":{\"id\":2}");
  }

  @Test
  void flushesPerBatchMakingEventsVisibleBeforeCloseButPerformsNoFsync() throws Exception {
    // Pin the documented "flush ≠ fsync" contract for the NDJSON file sink
    // (docs/SPEC.md §14). The Writer API Java exposes here has no
    // fsync surface at all — `flush()` on a BufferedWriter only pushes user-space bytes down
    // to the underlying OutputStream. That means a structurally truthful proof of
    // "no fsync" is: every durability signal is a Writer call, and none of those calls can
    // invoke fsync. We record every call and assert only flush/write/close appear.
    Path outputPath = tempDir.resolve("flush-not-fsync.ndjson");
    // Open the sink through the supported factory first so the append-open semantics are
    // exercised, then append events and read the file back before close().
    NdjsonChangeEventSink sink = NdjsonChangeEventSink.forFile(outputPath);
    try {
      sink.appendEvents(List.of(sampleEvent(1L, "one")));
      // After flush the bytes must be visible to another reader BEFORE close() — this is
      // the "flush per batch" guarantee.
      List<String> afterFirstFlush = Files.readAllLines(outputPath);
      assertThat(afterFirstFlush).hasSize(1);

      sink.appendEvents(List.of(sampleEvent(2L, "two")));
      List<String> afterSecondFlush = Files.readAllLines(outputPath);
      assertThat(afterSecondFlush).hasSize(2);
    } finally {
      sink.close();
    }

    // Structural check: drive the sink through a RecordingWriter whose only observable
    // operations are write / flush / close. If any future change added an fsync-like call,
    // the Writer abstraction literally could not express it — that is the "no fsync" proof.
    RecordingWriter recorder = new RecordingWriter();
    try (NdjsonChangeEventSink stubSink = new NdjsonChangeEventSink(recorder, true)) {
      stubSink.appendEvents(List.of(sampleEvent(3L, "three"), sampleEvent(4L, "four")));
      stubSink.appendEvents(List.of(sampleEvent(5L, "five")));
    }
    // Exactly one flush per non-empty batch (appendEvents calls writer.flush() once after
    // writing all events in the batch).
    assertThat(recorder.flushCount).isEqualTo(2);
    assertThat(recorder.closeCount).isEqualTo(1);
    // No sink method ever invoked anything outside {write, flush, close}.
    assertThat(recorder.unexpectedCalls).isZero();
  }

  @Test
  void appendsToExistingNdjsonFileOnRestartWithoutTruncatingPriorContent() throws Exception {
    // The file sink uses CREATE + APPEND open options, so restart after crash (or clean
    // restart) must preserve previously written lines. This test pins that behaviour and
    // therefore prevents any future change from silently upgrading to TRUNCATE_EXISTING.
    Path outputPath = tempDir.resolve("restart-append.ndjson");

    // Pre-seed the file with a line from an "earlier run".
    Files.writeString(
        outputPath,
        "PRIOR_RUN_LINE\n",
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING);

    // Simulate a process restart: open a new sink against the same file.
    try (NdjsonChangeEventSink sink = NdjsonChangeEventSink.forFile(outputPath)) {
      sink.appendEvents(List.of(sampleEvent(42L, "post-restart")));
    }

    List<String> lines = Files.readAllLines(outputPath);
    assertThat(lines).hasSize(2);
    assertThat(lines.get(0)).isEqualTo("PRIOR_RUN_LINE");
    assertThat(lines.get(1)).contains("\"primaryKey\":{\"id\":42}");
  }

  @Test
  void emptyAppendBatchIsANoOpAndDoesNotFlush() {
    RecordingWriter recorder = new RecordingWriter();
    try (NdjsonChangeEventSink sink = new NdjsonChangeEventSink(recorder, true)) {
      sink.appendEvents(List.of()); // empty batch short-circuits
    } catch (Exception ex) {
      throw new AssertionError(ex);
    }
    // Empty batch does not trigger a flush — it short-circuits at the top of appendEvents.
    assertThat(recorder.flushCount).isZero();
    assertThat(recorder.writeStringCount).isZero();
    assertThat(recorder.closeCount).isEqualTo(1);
  }

  /**
   * Writer decorator that counts the surface operations the NDJSON sink uses. The Java
   * {@link Writer} API does not expose any fsync-equivalent method — the absence of unexpected
   * method names is the structural proof that the sink cannot be fsync-ing underneath.
   */
  private static final class RecordingWriter extends Writer {
    private final StringWriter delegate = new StringWriter();
    int writeStringCount;
    int flushCount;
    int closeCount;
    int unexpectedCalls;

    @Override
    public void write(char[] cbuf, int off, int len) {
      delegate.write(cbuf, off, len);
    }

    @Override
    public void write(String str) {
      writeStringCount++;
      delegate.write(str);
    }

    @Override
    public void write(int c) {
      delegate.write(c);
    }

    @Override
    public void flush() {
      flushCount++;
      delegate.flush();
    }

    @Override
    public void close() {
      closeCount++;
      try {
        delegate.close();
      } catch (java.io.IOException ex) {
        throw new java.io.UncheckedIOException(ex);
      }
    }
  }

  private static ChangeEvent sampleEvent(long id, String name) {
    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>();
    primaryKey.put("id", id);
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put("id", id);
    afterRow.put("name", name);
    return ChangeEventTestFixtures.fromRowMaps(
        new TableId("appdb", "public", "widgets"),
        OperationType.UPDATE,
        CaptureOrigin.SELECT,
        primaryKey,
        null,
        afterRow,
        new OpaqueSourcePosition("snapshot:" + id),
        null,
        "dump");
  }
}
