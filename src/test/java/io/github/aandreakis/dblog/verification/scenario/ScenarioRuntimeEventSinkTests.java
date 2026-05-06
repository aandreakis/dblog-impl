package io.github.aandreakis.dblog.verification.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.testsupport.ChangeEventTestFixtures;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ScenarioRuntimeEventSinkTests {
  @Test
  void appendEventsFiltersWatermarkAndHeartbeatBeforeWritingToScenarioStore() {
    RecordingScenarioStore store = new RecordingScenarioStore();
    ScenarioRuntimeEventSink sink = new ScenarioRuntimeEventSink(store, "scenario-1");

    sink.appendEvents(
        List.of(
            event(OperationType.WATERMARK, CaptureOrigin.LOG, "wm"),
            event(OperationType.HEARTBEAT, CaptureOrigin.LOG, "hb"),
            event(OperationType.INSERT, CaptureOrigin.LOG, "1"),
            event(OperationType.UPDATE, CaptureOrigin.SELECT, "2")));

    assertThat(store.batches).hasSize(1);
    assertThat(store.batches.get(0))
        .extracting(ChangeEvent::operationType)
        .containsExactly(OperationType.INSERT, OperationType.UPDATE);
  }

  private static ChangeEvent event(
      OperationType operationType, CaptureOrigin captureOrigin, String primaryKeyLiteral) {
    return ChangeEventTestFixtures.fromRowMaps(
        new TableId("source", "appdb", "widgets"),
        operationType,
        captureOrigin,
        Map.of("id", primaryKeyLiteral),
        null,
        Map.of("id", primaryKeyLiteral),
        new OpaqueSourcePosition("pos:" + primaryKeyLiteral),
        "tx-" + primaryKeyLiteral,
        null);
  }

  private static final class RecordingScenarioStore implements ScenarioStore {
    private final List<List<ChangeEvent>> batches = new ArrayList<>();

    @Override
    public void appendEvents(String scenarioId, String stageLabel, List<ChangeEvent> events) {
      batches.add(List.copyOf(events));
    }

    @Override
    public void recordTelemetry(String scenarioId, String category, String message, String detail) {}

    @Override
    public List<ScenarioEventRecord> loadEvents(String scenarioId) {
      return List.of();
    }

    @Override
    public List<ScenarioRowState> loadCurrentRows(String scenarioId) {
      return List.of();
    }

    @Override
    public List<ScenarioTelemetryRecord> loadTelemetry(String scenarioId) {
      return List.of();
    }

    @Override
    public void close() {}
  }
}
