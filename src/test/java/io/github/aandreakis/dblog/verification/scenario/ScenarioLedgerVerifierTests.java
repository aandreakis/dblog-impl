package io.github.aandreakis.dblog.verification.scenario;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ScenarioLedgerVerifierTests {
  @Test
  void acceptsNonDecreasingPerKeyLogLedgerWithRepeatedSameMutation() {
    RecordingScenarioStore store =
        new RecordingScenarioStore(
            List.of(
                telemetry(
                    1L,
                    "scenario-1",
                    "source-mutation",
                    "mutation-1",
                    ScenarioLedgerVerifier.encodeSourceMutationDetail(
                        "UPDATE",
                        "source.widgets",
                        "Long:1",
                        "{id=Long:1,name=str:old}",
                        "{id=Long:1,name=str:new}"))),
            List.of(
                event(
                    1L,
                    "scenario-1",
                    "source.widgets",
                    "UPDATE",
                    "LOG",
                    "Long:1",
                    "{id=Long:1,name=str:old}",
                    "{id=Long:1,name=str:new}"),
                event(
                    2L,
                    "scenario-1",
                    "source.widgets",
                    "UPDATE",
                    "LOG",
                    "Long:1",
                    "{id=Long:1,name=str:old}",
                    "{id=Long:1,name=str:new}"),
                event(
                    3L,
                    "scenario-1",
                    "source.widgets",
                    "READ",
                    "SNAPSHOT",
                    "Long:1",
                    null,
                    "{id=Long:1,name=str:new}")));

    ScenarioLedgerVerifier.verifyObservedLedger(store, "scenario-1", Set.of("source.widgets"));
  }

  @Test
  void acceptsReplayOfAlreadySeenOlderMutationDuringAtLeastOnceRecovery() {
    RecordingScenarioStore store =
        new RecordingScenarioStore(
            List.of(
                telemetry(
                    1L,
                    "scenario-1",
                    "source-mutation",
                    "mutation-1",
                    ScenarioLedgerVerifier.encodeSourceMutationDetail(
                        "UPDATE",
                        "source.widgets",
                        "Long:1",
                        "{id=Long:1,name=str:seed}",
                        "{id=Long:1,name=str:v1}")),
                telemetry(
                    2L,
                    "scenario-1",
                    "source-mutation",
                    "mutation-2",
                    ScenarioLedgerVerifier.encodeSourceMutationDetail(
                        "UPDATE",
                        "source.widgets",
                        "Long:1",
                        "{id=Long:1,name=str:v1}",
                        "{id=Long:1,name=str:v2}"))),
            List.of(
                event(
                    1L,
                    "scenario-1",
                    "source.widgets",
                    "UPDATE",
                    "LOG",
                    "Long:1",
                    "{id=Long:1,name=str:seed}",
                    "{id=Long:1,name=str:v1}"),
                event(
                    2L,
                    "scenario-1",
                    "source.widgets",
                    "UPDATE",
                    "LOG",
                    "Long:1",
                    "{id=Long:1,name=str:v1}",
                    "{id=Long:1,name=str:v2}"),
                event(
                    3L,
                    "scenario-1",
                    "source.widgets",
                    "UPDATE",
                    "LOG",
                    "Long:1",
                    "{id=Long:1,name=str:seed}",
                    "{id=Long:1,name=str:v1}")));

    assertThatCode(
            () ->
                ScenarioLedgerVerifier.verifyObservedLedger(
                    store, "scenario-1", Set.of("source.widgets")))
        .doesNotThrowAnyException();
  }

  @Test
  void failsWhenLogLedgerSkipsAnEarlierMutation() {
    RecordingScenarioStore store =
        new RecordingScenarioStore(
            List.of(
                telemetry(
                    1L,
                    "scenario-1",
                    "source-mutation",
                    "mutation-1",
                    ScenarioLedgerVerifier.encodeSourceMutationDetail(
                        "UPDATE",
                        "source.widgets",
                        "Long:1",
                        "{id=Long:1,name=str:seed}",
                        "{id=Long:1,name=str:v1}")),
                telemetry(
                    2L,
                    "scenario-1",
                    "source-mutation",
                    "mutation-2",
                    ScenarioLedgerVerifier.encodeSourceMutationDetail(
                        "UPDATE",
                        "source.widgets",
                        "Long:1",
                        "{id=Long:1,name=str:v1}",
                        "{id=Long:1,name=str:v2}"))),
            List.of(
                event(
                    1L,
                    "scenario-1",
                    "source.widgets",
                    "UPDATE",
                    "LOG",
                    "Long:1",
                    "{id=Long:1,name=str:v1}",
                    "{id=Long:1,name=str:v2}")));

    assertThatThrownBy(
            () ->
                ScenarioLedgerVerifier.verifyObservedLedger(
                    store, "scenario-1", Set.of("source.widgets")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("skipped an earlier source mutation");
  }

  @Test
  void failsWhenSnapshotReadContradictsLatestPriorLogState() {
    RecordingScenarioStore store =
        new RecordingScenarioStore(
            List.of(
                telemetry(
                    1L,
                    "scenario-1",
                    "source-mutation",
                    "mutation-1",
                    ScenarioLedgerVerifier.encodeSourceMutationDetail(
                        "UPDATE",
                        "source.widgets",
                        "Long:1",
                        "{id=Long:1,name=str:old}",
                        "{id=Long:1,name=str:new}"))),
            List.of(
                event(
                    1L,
                    "scenario-1",
                    "source.widgets",
                    "UPDATE",
                    "LOG",
                    "Long:1",
                    "{id=Long:1,name=str:old}",
                    "{id=Long:1,name=str:new}"),
                event(
                    2L,
                    "scenario-1",
                    "source.widgets",
                    "UPDATE",
                    "SELECT",
                    "Long:1",
                    null,
                    "{id=Long:1,name=str:old}")));

    assertThatThrownBy(
            () ->
                ScenarioLedgerVerifier.verifyObservedLedger(
                    store, "scenario-1", Set.of("source.widgets")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("SELECT refresh UPDATE contradicted the latest prior LOG state");
  }

  private static ScenarioTelemetryRecord telemetry(
      long sequenceNumber, String scenarioId, String category, String message, String detail) {
    return new ScenarioTelemetryRecord(
        sequenceNumber, scenarioId, category, message, detail, Instant.now());
  }

  private static ScenarioEventRecord event(
      long sequenceNumber,
      String scenarioId,
      String tableDisplayName,
      String operationType,
      String captureOrigin,
      String primaryKeyLiteral,
      String beforePayload,
      String afterPayload) {
    return new ScenarioEventRecord(
        sequenceNumber,
        scenarioId,
        "stage",
        tableDisplayName,
        operationType,
        captureOrigin,
        primaryKeyLiteral,
        "pos",
        "tx",
        null,
        beforePayload,
        afterPayload,
        Instant.now());
  }

  private static final class RecordingScenarioStore implements ScenarioStore {
    private final List<ScenarioTelemetryRecord> telemetry;
    private final List<ScenarioEventRecord> events;

    private RecordingScenarioStore(
        List<ScenarioTelemetryRecord> telemetry, List<ScenarioEventRecord> events) {
      this.telemetry = telemetry;
      this.events = events;
    }

    @Override
    public void appendEvents(String scenarioId, String stageLabel, List<io.github.aandreakis.dblog.core.model.ChangeEvent> events) {}

    @Override
    public void recordTelemetry(String scenarioId, String category, String message, String detail) {}

    @Override
    public List<ScenarioEventRecord> loadEvents(String scenarioId) {
      return events;
    }

    @Override
    public List<ScenarioRowState> loadCurrentRows(String scenarioId) {
      return List.of();
    }

    @Override
    public List<ScenarioTelemetryRecord> loadTelemetry(String scenarioId) {
      return telemetry;
    }

    @Override
    public void close() {}
  }
}
