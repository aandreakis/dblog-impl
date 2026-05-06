package io.github.aandreakis.dblog.tap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.model.WatermarkToken;
import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.request.DumpRequestState;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.sink.api.ChangeEventBatchContext;
import io.github.aandreakis.dblog.sink.api.ChangeEventSink;
import io.github.aandreakis.dblog.sink.api.ContextualChangeEventSink;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Locks the sink-failure contract of {@link TappingChangeEventSink}: when the delegate throws,
 * no {@code tap.onSinkEvent} call fires. The behaviour is correct by construction today (the
 * throw short-circuits the statement below the delegate call), but a future refactor that
 * reorders those statements — or wraps the delegate in a try/catch — must not silently start
 * emitting tap events for batches the sink never accepted.
 */
class TappingChangeEventSinkTests {

  private static final TableId TABLE = new TableId("db", "public", "orders");

  @Test
  void plainAppendSuccessEmitsOneTapEventPerChangeEvent() {
    RecordingTap tap = new RecordingTap();
    RecordingSink delegate = new RecordingSink();
    TappingChangeEventSink wrapper = new TappingChangeEventSink(delegate, "ndjson", tap);

    List<ChangeEvent> events = List.of(makeEvent("1", "binlog:10"), makeEvent("2", "binlog:20"));
    wrapper.appendEvents(events);

    assertThat(delegate.plainAppended).containsExactlyElementsOf(events);
    assertThat(tap.sinkEvents)
        .extracting(SinkEventRecord::event)
        .containsExactlyElementsOf(events);
    assertThat(tap.sinkEvents).allSatisfy(r -> assertThat(r.sinkName()).isEqualTo("ndjson"));
  }

  @Test
  void plainAppendDelegateFailureEmitsNoTapEvents() {
    RecordingTap tap = new RecordingTap();
    ChangeEventSink throwing =
        events -> {
          throw new RuntimeException("delegate failed");
        };
    TappingChangeEventSink wrapper = new TappingChangeEventSink(throwing, "ndjson", tap);

    List<ChangeEvent> events = List.of(makeEvent("1", "binlog:10"), makeEvent("2", "binlog:20"));

    assertThatThrownBy(() -> wrapper.appendEvents(events))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("delegate failed");
    assertThat(tap.sinkEvents).isEmpty();
  }

  @Test
  void contextualAppendSuccessEmitsOneTapEventPerChangeEvent() {
    RecordingTap tap = new RecordingTap();
    RecordingContextualSink delegate = new RecordingContextualSink();
    TappingChangeEventSink wrapper = new TappingChangeEventSink(delegate, "jdbc", tap);

    ChangeEventBatchContext context = new ChangeEventBatchContext("stage", Instant.EPOCH);
    List<ChangeEvent> events = List.of(makeEvent("3", "binlog:30"));
    wrapper.appendEvents(context, events);

    assertThat(delegate.contextualAppended).containsExactlyElementsOf(events);
    assertThat(delegate.plainAppended).isEmpty();
    assertThat(tap.sinkEvents).extracting(SinkEventRecord::event).containsExactlyElementsOf(events);
    assertThat(tap.sinkEvents).allSatisfy(r -> assertThat(r.sinkName()).isEqualTo("jdbc"));
  }

  @Test
  void contextualAppendDelegateFailureEmitsNoTapEvents() {
    RecordingTap tap = new RecordingTap();
    ContextualChangeEventSink throwing =
        new ContextualChangeEventSink() {
          @Override
          public void appendEvents(List<ChangeEvent> events) {
            throw new AssertionError("plain overload must not be called when contextual is used");
          }

          @Override
          public void appendEvents(ChangeEventBatchContext context, List<ChangeEvent> events) {
            throw new RuntimeException("contextual delegate failed");
          }
        };
    TappingChangeEventSink wrapper = new TappingChangeEventSink(throwing, "jdbc", tap);

    ChangeEventBatchContext context = new ChangeEventBatchContext("stage", Instant.EPOCH);
    List<ChangeEvent> events = List.of(makeEvent("4", "binlog:40"));

    assertThatThrownBy(() -> wrapper.appendEvents(context, events))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("contextual delegate failed");
    assertThat(tap.sinkEvents).isEmpty();
  }

  @Test
  void contextualAppendOnPlainDelegateFallsBackToPlainAppend() {
    // A plain (non-contextual) delegate wrapped by the tap must still deliver events on the
    // contextual overload — the decorator unwraps to delegate.appendEvents(events). The tap
    // still sees the sink events because the plain call succeeded.
    RecordingTap tap = new RecordingTap();
    RecordingSink delegate = new RecordingSink();
    TappingChangeEventSink wrapper = new TappingChangeEventSink(delegate, "ndjson", tap);

    ChangeEventBatchContext context = new ChangeEventBatchContext("stage", Instant.EPOCH);
    List<ChangeEvent> events = List.of(makeEvent("5", "binlog:50"));
    wrapper.appendEvents(context, events);

    assertThat(delegate.plainAppended).containsExactlyElementsOf(events);
    assertThat(tap.sinkEvents).hasSize(1);
  }

  // --- helpers --------------------------------------------------------------

  private static ChangeEvent makeEvent(String pk, String lsn) {
    SourcePosition position = new OpaqueSourcePosition(lsn);
    ImmutableRowImage pkImage = ImmutableRowImage.of(Map.of("id", pk));
    ImmutableRowImage rowImage = ImmutableRowImage.of(Map.of("id", pk, "amount", "10.00"));
    return new ChangeEvent(
        TABLE,
        OperationType.INSERT,
        CaptureOrigin.LOG,
        pkImage,
        null,
        rowImage,
        position,
        "tx-" + pk,
        null);
  }

  private record SinkEventRecord(ChangeEvent event, String sinkName) {}

  private static final class RecordingTap implements Tap {
    final List<SinkEventRecord> sinkEvents = new ArrayList<>();

    @Override
    public void onSinkBatchStart() {}

    @Override
    public void onSinkBatchCommit() {}

    @Override
    public void onSinkEvent(ChangeEvent event, String sinkName) {
      sinkEvents.add(new SinkEventRecord(event, sinkName));
    }

    @Override
    public void onCdcBatch(List<ChangeEvent> events) {}

    @Override
    public void onWatermarkWritten(WatermarkLevel level, WatermarkToken token) {}

    @Override
    public void onWatermarkReceived(WatermarkLevel level, WatermarkToken token, SourcePosition lsn) {}

    @Override
    public void onChunkSelected(String requestId, Chunk chunk) {}

    @Override
    public void onChunkCollision(TableSchema schema, ImmutableRowImage removedRow, ChangeEvent cause) {}

    @Override
    public void onChunkCompleted(
        String requestId, Chunk chunk, List<ChangeEvent> emittedEvents, SourcePosition hwLsn) {}

    @Override
    public void onCheckpointAdvanced(SourcePosition position, int bufferedEvents, String reason) {}

    @Override
    public void onRequestTransition(
        String requestId,
        DumpScope scope,
        TableId table,
        DumpRequestState previous,
        DumpRequestState current,
        String reason) {}

    @Override
    public void onError(String exceptionClass, String message, Map<String, Object> context) {}
  }

  private static final class RecordingSink implements ChangeEventSink {
    final List<ChangeEvent> plainAppended = new ArrayList<>();

    @Override
    public void appendEvents(List<ChangeEvent> events) {
      plainAppended.addAll(events);
    }
  }

  private static final class RecordingContextualSink implements ContextualChangeEventSink {
    final List<ChangeEvent> plainAppended = new ArrayList<>();
    final List<ChangeEvent> contextualAppended = new ArrayList<>();

    @Override
    public void appendEvents(List<ChangeEvent> events) {
      plainAppended.addAll(events);
    }

    @Override
    public void appendEvents(ChangeEventBatchContext context, List<ChangeEvent> events) {
      contextualAppended.addAll(events);
    }
  }
}
