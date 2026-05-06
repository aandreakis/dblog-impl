package io.github.aandreakis.dblog.verification.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.core.request.DumpRequestState;
import io.github.aandreakis.dblog.core.request.DumpRequestStatus;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.request.DumpTableProgress;
import io.github.aandreakis.dblog.core.schema.FullDumpRequiredSignal;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import io.github.aandreakis.dblog.core.schema.SchemaUncertaintySignal;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.state.api.DumpProgressRepository;
import io.github.aandreakis.dblog.state.api.DumpRequestRepository;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import io.github.aandreakis.dblog.state.api.SchemaStateRepository;
import io.github.aandreakis.dblog.state.api.SourceOwnershipRepository;
import io.github.aandreakis.dblog.state.api.StoredDumpRequest;
import io.github.aandreakis.dblog.state.api.StreamPositionRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class FaultInjectingStateStoreTests {
  @Test
  void saveStatusRecordsTelemetryThroughFinalRuntimeStateApi() {
    InMemoryRuntimeStateStore delegate = new InMemoryRuntimeStateStore();
    RecordingScenarioStore scenarioStore = new RecordingScenarioStore();
    FaultInjectingStateStore stateStore =
        new FaultInjectingStateStore(delegate, scenarioStore, "scenario-1", ScenarioFaultPlan.none());

    DumpRequest request =
        new DumpRequest("req-1", DumpScope.TABLE, new TableId("appdb", "appdb", "widgets"), List.of());
    DumpRequestStatus status = DumpRequestStatus.active(request);

    stateStore.dumpRequests().saveStatus(status);

    assertThat(delegate.statuses).containsEntry(request.requestId(), status);
    assertThat(scenarioStore.telemetryDetails).contains(status.toString());
  }

  @Test
  void failsConfiguredFinalStateOperationBeforeDelegateWrite() {
    InMemoryRuntimeStateStore delegate = new InMemoryRuntimeStateStore();
    RecordingScenarioStore scenarioStore = new RecordingScenarioStore();
    FaultInjectingStateStore stateStore =
        new FaultInjectingStateStore(
            delegate,
            scenarioStore,
            "scenario-1",
            new ScenarioFaultPlan(
                Duration.ZERO,
                null,
                null,
                Duration.ZERO,
                null,
                Duration.ZERO,
                null,
                Duration.ZERO,
                null,
                Duration.ZERO,
                1,
                "saveStreamCheckpoint",
                Duration.ZERO,
                Duration.ZERO,
                Duration.ZERO,
                Duration.ZERO,
                Duration.ZERO,
                Duration.ZERO));

    assertThatThrownBy(
            () ->
                stateStore
                    .streamPositions()
                    .saveCheckpoint("source-1", new OpaqueSourcePosition("mysql-bin.000001:123")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("saveStreamCheckpoint");

    assertThat(delegate.checkpoints).isEmpty();
    assertThat(scenarioStore.telemetryDetails)
        .anyMatch(detail -> detail.contains("operation=saveStreamCheckpoint"));
  }

  private static final class RecordingScenarioStore implements ScenarioStore {
    private final List<String> telemetryDetails = new ArrayList<>();

    @Override
    public void appendEvents(String scenarioId, String stageLabel, List<ChangeEvent> events) {}

    @Override
    public void recordTelemetry(String scenarioId, String category, String message, String detail) {
      telemetryDetails.add(detail);
    }

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

  private static final class InMemoryRuntimeStateStore implements RuntimeStateStore {
    private final Map<String, SourcePosition> checkpoints = new LinkedHashMap<>();
    private final Map<String, SourcePosition> bootstrapPositions = new LinkedHashMap<>();
    private final Map<String, DumpRequest> requests = new LinkedHashMap<>();
    private final Map<String, DumpRequestStatus> statuses = new LinkedHashMap<>();
    private final StreamPositionRepository streamPositions =
        new StreamPositionRepository() {
          @Override
          public void saveCheckpoint(String sourceId, SourcePosition position) {
            checkpoints.put(sourceId, position);
          }

          @Override
          public Optional<SourcePosition> loadCheckpoint(String sourceId) {
            return Optional.ofNullable(checkpoints.get(sourceId));
          }

          @Override
          public void saveBootstrapPosition(String sourceId, SourcePosition position) {
            bootstrapPositions.put(sourceId, position);
          }

          @Override
          public Optional<SourcePosition> loadBootstrapPosition(String sourceId) {
            return Optional.ofNullable(bootstrapPositions.get(sourceId));
          }

          @Override
          public void clearBootstrapPosition(String sourceId) {
            bootstrapPositions.remove(sourceId);
          }
        };
    private final DumpRequestRepository dumpRequests =
        new DumpRequestRepository() {
          @Override
          public void upsert(DumpRequest request) {
            requests.put(request.requestId(), request);
          }

          @Override
          public boolean createIfAbsent(DumpRequest request) {
            return requests.putIfAbsent(request.requestId(), request) == null;
          }

          @Override
          public DumpRequest createGenerated(
              DumpScope scope, TableId tableId, List<PrimaryKeyTuple> primaryKeyTuples) {
            DumpRequest request =
                new DumpRequest(
                    "generated-" + (requests.size() + 1), scope, tableId, primaryKeyTuples);
            requests.put(request.requestId(), request);
            return request;
          }

          @Override
          public List<DumpRequest> loadPending() {
            return requests.values().stream()
                .filter(
                    request ->
                        statuses.getOrDefault(
                                    request.requestId(),
                                    DumpRequestStatus.active(request))
                                .state()
                            == DumpRequestState.ACTIVE)
                .toList();
          }

          @Override
          public List<DumpRequest> loadAll() {
            return List.copyOf(requests.values());
          }

          @Override
          public List<StoredDumpRequest> loadAllDetailed() {
            Instant now = Instant.parse("2026-04-13T00:00:00Z");
            return requests.values().stream()
                .map(request -> new StoredDumpRequest(request, now, now))
                .toList();
          }

          @Override
          public int countPending() {
            return loadPending().size();
          }

          @Override
          public Optional<DumpRequest> loadRequest(String requestId) {
            return Optional.ofNullable(requests.get(requestId));
          }

          @Override
          public Optional<StoredDumpRequest> loadRequestDetailed(String requestId) {
            return loadRequest(requestId)
                .map(
                    request ->
                        new StoredDumpRequest(
                            request,
                            Instant.parse("2026-04-13T00:00:00Z"),
                            Instant.parse("2026-04-13T00:00:00Z")));
          }

          @Override
          public void saveStatus(DumpRequestStatus status) {
            statuses.put(status.requestId(), status);
          }

          @Override
          public boolean saveStatusIfAbsentOrActive(DumpRequestStatus status) {
            DumpRequestStatus current = statuses.get(status.requestId());
            if (current != null && current.state() != DumpRequestState.ACTIVE) {
              return false;
            }
            statuses.put(status.requestId(), status);
            return true;
          }

          @Override
          public boolean saveStatusIfCurrentStateIn(
              DumpRequestStatus status,
              boolean allowMissing,
              DumpRequestState... allowedCurrentStates) {
            DumpRequestStatus current = statuses.get(status.requestId());
            if (current == null) {
              if (!allowMissing) {
                return false;
              }
            } else {
              boolean allowed = false;
              for (DumpRequestState allowedState : allowedCurrentStates) {
                if (current.state() == allowedState) {
                  allowed = true;
                  break;
                }
              }
              if (!allowed) {
                return false;
              }
            }
            statuses.put(status.requestId(), status);
            return true;
          }

          @Override
          public Optional<DumpRequestStatus> loadStatus(String requestId) {
            return Optional.ofNullable(statuses.get(requestId));
          }

          @Override
          public void failNonTerminalRequests(String reason) {}

          @Override
          public void pruneSupersededTerminalRequests(DumpRequest justCompletedRequest) {}
        };

    @Override
    public StreamPositionRepository streamPositions() {
      return streamPositions;
    }

    @Override
    public DumpRequestRepository dumpRequests() {
      return dumpRequests;
    }

    @Override
    public DumpProgressRepository dumpProgress() {
      return new DumpProgressRepository() {
        @Override
        public void save(DumpTableProgress progress) {}

        @Override
        public Optional<DumpTableProgress> load(String jobId, String tableName) {
          return Optional.empty();
        }

        @Override
        public void delete(String jobId, String tableName) {}

        @Override
        public void deleteAll() {}
      };
    }

    @Override
    public SchemaStateRepository schemas() {
      return new SchemaStateRepository() {
        @Override
        public void saveContractSchema(TableSchema schema) {}

        @Override
        public Optional<TableSchema> loadContractSchema(String tableDisplayName) {
          return Optional.empty();
        }

        @Override
        public List<TableSchema> loadAllContractSchemas() {
          return List.of();
        }

        @Override
        public void saveObservedSchema(TableSchema schema) {}

        @Override
        public Optional<TableSchema> loadObservedSchema(String tableDisplayName) {
          return Optional.empty();
        }

        @Override
        public List<TableSchema> loadAllObservedSchemas() {
          return List.of();
        }

        @Override
        public void saveFullDumpRequiredSignal(FullDumpRequiredSignal signal) {}

        @Override
        public List<FullDumpRequiredSignal> loadFullDumpRequiredSignals() {
          return List.of();
        }

        @Override
        public void saveSchemaUncertaintySignal(SchemaUncertaintySignal signal) {}

        @Override
        public void clearSchemaUncertaintySignal(String sourceId, String tableDisplayName) {}

        @Override
        public void deleteSchemaUncertaintySignals(String sourceId) {}

        @Override
        public List<SchemaUncertaintySignal> loadSchemaUncertaintySignals() {
          return List.of();
        }
      };
    }

    @Override
    public SourceOwnershipRepository ownership() {
      return sourceId -> {};
    }

    @Override
    public void invalidateRuntimeStateForPkChange(String sourceId, String reason) {}
  }
}
