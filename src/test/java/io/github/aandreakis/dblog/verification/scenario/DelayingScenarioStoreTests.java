package io.github.aandreakis.dblog.verification.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.testsupport.ChangeEventTestFixtures;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DelayingScenarioStoreTests {
  @Test
  void delaysAppendBeforeDelegating() {
    RecordingScenarioStore delegate = new RecordingScenarioStore();
    DelayingScenarioStore store =
        new DelayingScenarioStore(delegate, Duration.ofMillis(25));

    long startedAt = System.nanoTime();
    store.appendEvents("scenario-1", "stage-1", List.of(event("1", "alpha")));
    long elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000L;

    assertThat(elapsedMillis).isGreaterThanOrEqualTo(20L);
    assertThat(delegate.events).hasSize(1);
  }

  private static ChangeEvent event(String id, String name) {
    long numericId = Long.parseLong(id);
    return ChangeEventTestFixtures.fromRowMaps(
        new TableId("app", "public", "widgets"),
        OperationType.UPDATE,
        CaptureOrigin.LOG,
        Map.of("id", numericId),
        null,
        Map.of("id", numericId, "name", name),
        new OpaqueSourcePosition("pos:" + id),
        "tx-" + id,
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
