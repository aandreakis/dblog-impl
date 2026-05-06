package io.github.aandreakis.dblog.verification.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.sink.ndjson.NdjsonChangeEventSink;
import io.github.aandreakis.dblog.testsupport.ChangeEventTestFixtures;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.List;
import org.junit.jupiter.api.Test;

class TeeingScenarioStoreTests {
  @Test
  void mirrorsEventsToSecondarySinkWhileKeepingDelegateReadable() {
    RecordingScenarioStore delegate = new RecordingScenarioStore();
    StringWriter writer = new StringWriter();
    TeeingScenarioStore store = new TeeingScenarioStore(delegate, new NdjsonChangeEventSink(writer, false));

    ChangeEvent event = sampleEvent();
    store.appendEvents("scenario-1", "stage-1", List.of(event));

    assertThat(delegate.events).containsExactly(event);
    assertThat(writer.toString()).contains("\"tableName\":\"widgets\"");
  }

  @Test
  void closeSurfacesMirrorSinkFailure() {
    RecordingScenarioStore delegate = new RecordingScenarioStore();
    TeeingScenarioStore store =
        new TeeingScenarioStore(
            delegate,
            new io.github.aandreakis.dblog.sink.api.ChangeEventSink() {
              @Override
              public void appendEvents(List<ChangeEvent> events) {}

              @Override
              public void close() {
                throw new IllegalStateException("boom");
              }
            });

    assertThatThrownBy(store::close)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("scenario store tee");
  }

  private static ChangeEvent sampleEvent() {
    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>();
    primaryKey.put("id", 1L);
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put("id", 1L);
    afterRow.put("name", "widget-1");
    return ChangeEventTestFixtures.fromRowMaps(
        new TableId("appdb", "public", "widgets"),
        OperationType.INSERT,
        CaptureOrigin.LOG,
        primaryKey,
        null,
        afterRow,
        new OpaqueSourcePosition("mysql-bin.000001:4"),
        "tx-1",
        null);
  }

  private static final class RecordingScenarioStore implements ScenarioStore {
    private List<ChangeEvent> events = List.of();

    @Override
    public void appendEvents(String scenarioId, String stageLabel, List<ChangeEvent> events) {
      this.events = List.copyOf(events);
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
