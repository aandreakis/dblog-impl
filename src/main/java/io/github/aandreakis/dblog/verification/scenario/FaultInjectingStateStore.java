package io.github.aandreakis.dblog.verification.scenario;

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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Scenario-only {@link RuntimeStateStore} decorator for durable-state delay/failure injection.
 *
 * <p>This keeps fault injection on the final runtime-state surface so scenario-specific failure
 * behavior stays outside the production state-store implementation.
 */
public final class FaultInjectingStateStore implements RuntimeStateStore {
  private final RuntimeStateStore delegate;
  private final ScenarioStore scenarioStore;
  private final String scenarioId;
  private final ScenarioFaultPlan faultPlan;
  private final AtomicInteger operations = new AtomicInteger();
  private final StreamPositionRepository streamPositions = new FaultInjectingStreamPositions();
  private final DumpRequestRepository dumpRequests = new FaultInjectingDumpRequests();
  private final DumpProgressRepository dumpProgress = new FaultInjectingDumpProgress();
  private final SchemaStateRepository schemas = new FaultInjectingSchemas();
  private final SourceOwnershipRepository ownership = new FaultInjectingOwnership();

  public FaultInjectingStateStore(
      RuntimeStateStore delegate,
      ScenarioStore scenarioStore,
      String scenarioId,
      ScenarioFaultPlan faultPlan) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.scenarioStore = Objects.requireNonNull(scenarioStore, "scenarioStore");
    this.scenarioId = Objects.requireNonNull(scenarioId, "scenarioId");
    this.faultPlan = Objects.requireNonNull(faultPlan, "faultPlan");
  }

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
    return dumpProgress;
  }

  @Override
  public SchemaStateRepository schemas() {
    return schemas;
  }

  @Override
  public SourceOwnershipRepository ownership() {
    return ownership;
  }

  @Override
  public void invalidateRuntimeStateForPkChange(String sourceId, String reason) {
    before("invalidateRuntimeStateForPkChange", "sourceId=" + sourceId + " reason=" + reason);
    delegate.invalidateRuntimeStateForPkChange(sourceId, reason);
  }

  private void before(String operationName, String detail) {
    maybeDelay(operationName);
    int invocation = operations.incrementAndGet();
    if (faultPlan.shouldFailStateStoreOperation(operationName, invocation)) {
      ScenarioTelemetry.injectedFailure(
          scenarioStore,
          scenarioId,
          "failure-injection",
          "state-store failure",
          "operation=" + operationName + " invocation=" + invocation + " detail=" + detail);
      throw new IllegalStateException(
          "Injected scenario state-store failure at operation "
              + operationName
              + " invocation "
              + invocation);
    }
  }

  private void maybeDelay(String operationName) {
    Duration delay = faultPlan.stateStoreDelay();
    if (delay.isZero()) {
      return;
    }
    ScenarioTelemetry.delay(scenarioStore, scenarioId, "state-store", operationName, delay);
    ScenarioJdbcSupport.sleepQuietly(delay);
  }

  private final class FaultInjectingOwnership implements SourceOwnershipRepository {
    @Override
    public void claimSourceOwnership(String sourceId) {
      before("claimSourceOwnership", "sourceId=" + sourceId);
      delegate.ownership().claimSourceOwnership(sourceId);
    }
  }

  private final class FaultInjectingStreamPositions implements StreamPositionRepository {
    @Override
    public void saveCheckpoint(String sourceId, SourcePosition position) {
      before("saveStreamCheckpoint", "sourceId=" + sourceId + " position=" + position);
      delegate.streamPositions().saveCheckpoint(sourceId, position);
    }

    @Override
    public Optional<SourcePosition> loadCheckpoint(String sourceId) {
      before("loadStreamCheckpoint", "sourceId=" + sourceId);
      return delegate.streamPositions().loadCheckpoint(sourceId);
    }

    @Override
    public void saveBootstrapPosition(String sourceId, SourcePosition position) {
      before("saveBootstrapStreamPosition", "sourceId=" + sourceId + " position=" + position);
      delegate.streamPositions().saveBootstrapPosition(sourceId, position);
    }

    @Override
    public Optional<SourcePosition> loadBootstrapPosition(String sourceId) {
      before("loadBootstrapStreamPosition", "sourceId=" + sourceId);
      return delegate.streamPositions().loadBootstrapPosition(sourceId);
    }

    @Override
    public void clearBootstrapPosition(String sourceId) {
      before("clearBootstrapStreamPosition", "sourceId=" + sourceId);
      delegate.streamPositions().clearBootstrapPosition(sourceId);
    }
  }

  private final class FaultInjectingDumpRequests implements DumpRequestRepository {
    @Override
    public void upsert(DumpRequest request) {
      before("upsertDumpRequest", request.toString());
      delegate.dumpRequests().upsert(request);
    }

    @Override
    public boolean createIfAbsent(DumpRequest request) {
      before("createDumpRequestIfAbsent", request.toString());
      return delegate.dumpRequests().createIfAbsent(request);
    }

    @Override
    public DumpRequest createGenerated(
        DumpScope scope,
        TableId tableId,
        List<PrimaryKeyTuple> primaryKeyTuples) {
      before(
          "createDumpRequest",
          "scope=" + scope + " tableId=" + tableId + " primaryKeyTuples=" + primaryKeyTuples);
      return delegate.dumpRequests().createGenerated(scope, tableId, primaryKeyTuples);
    }

    @Override
    public List<DumpRequest> loadPending() {
      before("loadPendingDumpRequests", null);
      return delegate.dumpRequests().loadPending();
    }

    @Override
    public List<DumpRequest> loadAll() {
      before("loadAllDumpRequests", null);
      return delegate.dumpRequests().loadAll();
    }

    @Override
    public List<StoredDumpRequest> loadAllDetailed() {
      before("loadAllDetailedDumpRequests", null);
      return delegate.dumpRequests().loadAllDetailed();
    }

    @Override
    public int countPending() {
      before("countPendingDumpRequests", null);
      return delegate.dumpRequests().countPending();
    }

    @Override
    public Optional<DumpRequest> loadRequest(String requestId) {
      before("loadDumpRequest", "requestId=" + requestId);
      return delegate.dumpRequests().loadRequest(requestId);
    }

    @Override
    public Optional<StoredDumpRequest> loadRequestDetailed(String requestId) {
      before("loadDetailedDumpRequest", "requestId=" + requestId);
      return delegate.dumpRequests().loadRequestDetailed(requestId);
    }

    @Override
    public void saveStatus(DumpRequestStatus status) {
      before("saveDumpRequestStatus", status.toString());
      delegate.dumpRequests().saveStatus(status);
      ScenarioTelemetry.requestStatus(scenarioStore, scenarioId, "state-store", status);
    }

    @Override
    public boolean saveStatusIfAbsentOrActive(DumpRequestStatus status) {
      before("saveDumpRequestStatusIfAbsentOrActive", status.toString());
      boolean saved = delegate.dumpRequests().saveStatusIfAbsentOrActive(status);
      if (saved) {
        ScenarioTelemetry.requestStatus(scenarioStore, scenarioId, "state-store", status);
      }
      return saved;
    }

    @Override
    public boolean saveStatusIfCurrentStateIn(
        DumpRequestStatus status, boolean allowMissing, DumpRequestState... allowedCurrentStates) {
      before("saveDumpRequestStatusIfCurrentStateIn", status.toString());
      boolean saved =
          delegate
              .dumpRequests()
              .saveStatusIfCurrentStateIn(status, allowMissing, allowedCurrentStates);
      if (saved) {
        ScenarioTelemetry.requestStatus(scenarioStore, scenarioId, "state-store", status);
      }
      return saved;
    }

    @Override
    public Optional<DumpRequestStatus> loadStatus(String requestId) {
      before("loadDumpRequestStatus", "requestId=" + requestId);
      return delegate.dumpRequests().loadStatus(requestId);
    }

    @Override
    public void failNonTerminalRequests(String reason) {
      before("failNonTerminalDumpRequests", "reason=" + reason);
      delegate.dumpRequests().failNonTerminalRequests(reason);
    }

    @Override
    public void pruneSupersededTerminalRequests(DumpRequest justCompletedRequest) {
      before(
          "pruneSupersededTerminalDumpRequests",
          "requestId=" + justCompletedRequest.requestId());
      delegate.dumpRequests().pruneSupersededTerminalRequests(justCompletedRequest);
    }
  }

  private final class FaultInjectingDumpProgress implements DumpProgressRepository {
    @Override
    public void save(DumpTableProgress progress) {
      before("saveDumpTableProgress", progress.toString());
      delegate.dumpProgress().save(progress);
    }

    @Override
    public Optional<DumpTableProgress> load(String jobId, String tableName) {
      before("loadDumpTableProgress", "jobId=" + jobId + " table=" + tableName);
      return delegate.dumpProgress().load(jobId, tableName);
    }

    @Override
    public void delete(String jobId, String tableName) {
      before("deleteDumpTableProgress", "jobId=" + jobId + " table=" + tableName);
      delegate.dumpProgress().delete(jobId, tableName);
    }

    @Override
    public void deleteAll() {
      before("deleteAllDumpTableProgress", null);
      delegate.dumpProgress().deleteAll();
    }
  }

  private final class FaultInjectingSchemas implements SchemaStateRepository {
    @Override
    public void saveContractSchema(TableSchema schema) {
      before("saveTableSchema", schema.toString());
      delegate.schemas().saveContractSchema(schema);
    }

    @Override
    public Optional<TableSchema> loadContractSchema(String tableDisplayName) {
      before("loadTableSchema", "tableDisplayName=" + tableDisplayName);
      return delegate.schemas().loadContractSchema(tableDisplayName);
    }

    @Override
    public List<TableSchema> loadAllContractSchemas() {
      before("loadAllTableSchemas", null);
      return delegate.schemas().loadAllContractSchemas();
    }

    @Override
    public void saveObservedSchema(TableSchema schema) {
      before("saveObservedTableSchema", schema.toString());
      delegate.schemas().saveObservedSchema(schema);
    }

    @Override
    public Optional<TableSchema> loadObservedSchema(String tableDisplayName) {
      before("loadObservedTableSchema", "tableDisplayName=" + tableDisplayName);
      return delegate.schemas().loadObservedSchema(tableDisplayName);
    }

    @Override
    public List<TableSchema> loadAllObservedSchemas() {
      before("loadAllObservedTableSchemas", null);
      return delegate.schemas().loadAllObservedSchemas();
    }

    @Override
    public void saveFullDumpRequiredSignal(FullDumpRequiredSignal signal) {
      before("saveFullDumpRequiredSignal", signal.toString());
      delegate.schemas().saveFullDumpRequiredSignal(signal);
    }

    @Override
    public List<FullDumpRequiredSignal> loadFullDumpRequiredSignals() {
      before("loadFullDumpRequiredSignals", null);
      return delegate.schemas().loadFullDumpRequiredSignals();
    }

    @Override
    public void saveSchemaUncertaintySignal(SchemaUncertaintySignal signal) {
      before("saveSchemaUncertaintySignal", signal.toString());
      delegate.schemas().saveSchemaUncertaintySignal(signal);
    }

    @Override
    public void clearSchemaUncertaintySignal(String sourceId, String tableDisplayName) {
      before(
          "clearSchemaUncertaintySignal",
          "sourceId=" + sourceId + " tableDisplayName=" + tableDisplayName);
      delegate.schemas().clearSchemaUncertaintySignal(sourceId, tableDisplayName);
    }

    @Override
    public void deleteSchemaUncertaintySignals(String sourceId) {
      before("deleteSchemaUncertaintySignals", "sourceId=" + sourceId);
      delegate.schemas().deleteSchemaUncertaintySignals(sourceId);
    }

    @Override
    public List<SchemaUncertaintySignal> loadSchemaUncertaintySignals() {
      before("loadSchemaUncertaintySignals", null);
      return delegate.schemas().loadSchemaUncertaintySignals();
    }
  }
}
