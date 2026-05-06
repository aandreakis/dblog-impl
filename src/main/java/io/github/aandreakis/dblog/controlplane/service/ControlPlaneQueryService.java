package io.github.aandreakis.dblog.controlplane.service;

import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.request.DumpRequestStatus;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.FullDumpRequiredSignal;
import io.github.aandreakis.dblog.core.schema.SchemaUncertaintySignal;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.runtime.sql.SourceFlowControlSnapshot;
import io.github.aandreakis.dblog.runtime.telemetry.RuntimeMeasurementMetricsSnapshot;
import io.github.aandreakis.dblog.state.api.DumpRequestNotFoundException;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import io.github.aandreakis.dblog.state.api.StoredDumpRequest;
import io.github.aandreakis.dblog.state.api.StoredDumpRequestDetail;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class ControlPlaneQueryService {
  private static final List<String> VALID_REQUEST_STATES =
      List.of("QUEUED", "ACTIVE", "COMPLETED", "FAILED");

  private final RuntimeStateStore stateStore;
  private final RuntimeStatusProvider runtimeStatusProvider;
  private final ControlPlaneEventStore eventStore;
  private final MeterRegistry meterRegistry;
  private final String measurementAdapterTag;

  public ControlPlaneQueryService(
      RuntimeStateStore stateStore, RuntimeStatusProvider runtimeStatusProvider) {
    this(stateStore, runtimeStatusProvider, ControlPlaneEventStore.noop(), null, null);
  }

  public ControlPlaneQueryService(
      RuntimeStateStore stateStore,
      RuntimeStatusProvider runtimeStatusProvider,
      ControlPlaneEventStore eventStore) {
    this(stateStore, runtimeStatusProvider, eventStore, null, null);
  }

  public ControlPlaneQueryService(
      RuntimeStateStore stateStore,
      RuntimeStatusProvider runtimeStatusProvider,
      ControlPlaneEventStore eventStore,
      MeterRegistry meterRegistry,
      String measurementAdapterTag) {
    this.stateStore = Objects.requireNonNull(stateStore, "stateStore");
    this.runtimeStatusProvider = Objects.requireNonNull(runtimeStatusProvider, "runtimeStatusProvider");
    this.eventStore = Objects.requireNonNull(eventStore, "eventStore");
    this.meterRegistry = meterRegistry;
    this.measurementAdapterTag = measurementAdapterTag;
  }

  public RuntimeStateStore stateStore() {
    return stateStore;
  }

  public Map<String, Object> runtimePayload() {
    RuntimeStatusProvider.RuntimeStatusSnapshot runtime = runtimeStatusProvider.snapshot();
    List<RequestView> requests = loadAllRequestViews();
    long pendingRequests =
        requests.stream()
            .filter(request -> request.state().equals("QUEUED") || request.state().equals("ACTIVE"))
            .count();
    LinkedHashMap<String, Object> queue = new LinkedHashMap<>();
    queue.put("pendingRequests", pendingRequests);
    queue.put("submissionAvailable", runtime.requestSubmissionAvailable());
    queue.put("submissionMessage", runtime.requestSubmissionMessage());

    LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
    payload.put("mode", runtime.mode());
    payload.put("adapter", runtime.adapter());
    payload.put("health", runtime.healthStatus());
    LinkedHashMap<String, Object> sourceRuntime = new LinkedHashMap<>();
    sourceRuntime.put("sourceId", runtime.sourceId());
    sourceRuntime.put("lastAcknowledgedCheckpoint", runtime.lastAcknowledgedCheckpoint());
    sourceRuntime.put("pendingTransactions", runtime.pendingTransactions());
    sourceRuntime.put("capturedTableCount", runtime.capturedTableCount());
    if (runtime.sourceFlowControl() != null) {
      sourceRuntime.put("sourceFlowControl", sourceFlowControlPayload(runtime.sourceFlowControl()));
    }
    payload.put("sourceRuntime", sourceRuntime);
    payload.put("queue", queue);
    return payload;
  }

  public Map<String, Object> healthPayload() {
    RuntimeStatusProvider.RuntimeStatusSnapshot runtime = runtimeStatusProvider.snapshot();
    LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
    payload.put("status", runtime.healthStatus());
    payload.put("mode", runtime.mode());
    payload.put("adapter", runtime.adapter());
    payload.put("sourceId", runtime.sourceId());
    payload.put("lastAcknowledgedCheckpoint", runtime.lastAcknowledgedCheckpoint());
    return payload;
  }

  public Map<String, Object> metricsPayload() {
    LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
    payload.put("runtimeMeasurement", runtimeMeasurementPayload());
    payload.put("eventCapture", eventCapturePayload());
    return payload;
  }

  public Map<String, Object> runtimeStatusPayload() {
    RuntimeStatusProvider.RuntimeStatusSnapshot runtime = runtimeStatusProvider.snapshot();
    List<RequestView> requests = loadAllRequestViews();
    long queuedCount =
        requests.stream().filter(request -> request.state().equals("QUEUED")).count();
    long activeCount =
        requests.stream().filter(request -> request.state().equals("ACTIVE")).count();
    RequestView activeRequest =
        requests.stream().filter(request -> request.state().equals("ACTIVE")).findFirst().orElse(null);

    LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
    payload.put("mode", runtime.mode());
    payload.put("adapter", runtime.adapter());
    payload.put("health", healthPayload());
    LinkedHashMap<String, Object> sourceRuntime = new LinkedHashMap<>();
    sourceRuntime.put("sourceId", runtime.sourceId());
    sourceRuntime.put("lastAcknowledgedCheckpoint", runtime.lastAcknowledgedCheckpoint());
    sourceRuntime.put("pendingTransactions", runtime.pendingTransactions());
    sourceRuntime.put("capturedTableCount", runtime.capturedTableCount());
    if (runtime.sourceFlowControl() != null) {
      sourceRuntime.put("sourceFlowControl", sourceFlowControlPayload(runtime.sourceFlowControl()));
    }
    payload.put("sourceRuntime", sourceRuntime);
    LinkedHashMap<String, Object> requestsPayload = new LinkedHashMap<>();
    requestsPayload.put("queuedCount", queuedCount);
    requestsPayload.put("activeCount", activeCount);
    requestsPayload.put("pendingCount", queuedCount + activeCount);
    requestsPayload.put("activeRequest", activeRequest == null ? null : requestPayload(activeRequest));
    requestsPayload.put(
        "activeRequestWindow",
        activeRequest == null ? null : activeRequest.activeWindow());
    payload.put("requests", requestsPayload);
    LinkedHashMap<String, Object> eventsPayload = new LinkedHashMap<>();
    eventsPayload.put("cumulativeEventsObserved", eventStore.cumulativeEventsObserved());
    eventsPayload.put("recentEventWindowSize", eventStore.recentEventWindowSize());
    eventsPayload.put(
        "tables", eventStore.tableSummaries().stream().map(this::tableSummaryPayload).toList());
    payload.put("events", eventsPayload);
    return payload;
  }

  public Map<String, Object> requestsPayload(String stateFilter, Integer limit) {
    List<RequestView> allRequests = loadAllRequestViews();
    List<RequestView> requests = filterRequestViews(allRequests, stateFilter, limit);
    LinkedHashMap<String, Integer> countsByState = new LinkedHashMap<>();
    for (RequestView request : allRequests) {
      countsByState.merge(request.state(), 1, Integer::sum);
    }
    return Map.of(
        "stateStoreConfigured", true,
        "requests", requests.stream().map(this::requestPayload).toList(),
        "countsByState", countsByState);
  }

  public Map<String, Object> requestPayload(String requestId) {
    RequestView request =
        stateStore.dumpRequests().loadRequestDetailedWithStatus(requestId)
            .map(this::toRequestView)
            .orElseThrow(() -> new DumpRequestNotFoundException(requestId));
    return requestPayload(request);
  }

  public Map<String, Object> runtimeSchemasPayload() {
    List<FullDumpRequiredSignal> fullDumpSignals = stateStore.schemas().loadFullDumpRequiredSignals();
    List<SchemaUncertaintySignal> uncertaintySignals = stateStore.schemas().loadSchemaUncertaintySignals();
    Map<String, FullDumpRequiredSignal> fullDumpByTable = indexFullDumpSignals(fullDumpSignals);
    Map<String, SchemaUncertaintySignal> uncertaintyByTable = indexSchemaUncertaintySignals(uncertaintySignals);

    List<TableSchema> contractSchemas = stateStore.schemas().loadAllContractSchemas();

    return Map.of(
        "stateStoreConfigured", true,
        "count", contractSchemas.size(),
        "schemas",
        contractSchemas.stream()
            .map(schema -> schemaViewPayload(schema, fullDumpByTable, uncertaintyByTable))
            .toList());
  }

  public Map<String, Object> runtimeSchemaIssuesPayload() {
    return Map.of(
        "fullDumpRequiredSignals",
        stateStore.schemas().loadFullDumpRequiredSignals().stream()
            .map(this::fullDumpSignalPayload)
            .toList(),
        "schemaUncertaintySignals",
        stateStore.schemas().loadSchemaUncertaintySignals().stream()
            .map(this::schemaUncertaintySignalPayload)
            .toList());
  }

  public Map<String, Object> recentEventsPayload(int limit) {
    LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
    payload.put("cumulativeEventsObserved", eventStore.cumulativeEventsObserved());
    payload.put("recentEventWindowSize", eventStore.recentEventWindowSize());
    payload.put(
        "events",
        eventStore.recentEvents(limit).stream().map(this::recordedEventPayload).toList());
    return payload;
  }

  public Map<String, Object> eventSummaryPayload() {
    LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
    payload.put("cumulativeEventsObserved", eventStore.cumulativeEventsObserved());
    payload.put("recentEventWindowSize", eventStore.recentEventWindowSize());
    payload.put(
        "tables", eventStore.tableSummaries().stream().map(this::tableSummaryPayload).toList());
    return payload;
  }

  public Map<String, Object> tableEventsPayload(String tableDisplayName, int limit) {
    Objects.requireNonNull(tableDisplayName, "tableDisplayName");
    return Map.of(
        "tableDisplayName", tableDisplayName,
        "recentEventCount", eventStore.recentEventsForTable(tableDisplayName, limit).size(),
        "events",
        eventStore.recentEventsForTable(tableDisplayName, limit).stream()
            .map(this::recordedEventPayload)
            .toList());
  }

  private List<RequestView> loadAllRequestViews() {
    return stateStore.dumpRequests().loadAllDetailedWithStatus().stream()
        .map(this::toRequestView)
        .toList();
  }

  private List<RequestView> filterRequestViews(
      List<RequestView> views, String stateFilter, Integer limit) {
    String normalizedFilter =
        stateFilter == null || stateFilter.isBlank()
            ? null
            : stateFilter.trim().toUpperCase(java.util.Locale.ROOT);
    if (normalizedFilter != null && !VALID_REQUEST_STATES.contains(normalizedFilter)) {
      throw new IllegalArgumentException(
          "state must be one of " + String.join(", ", VALID_REQUEST_STATES));
    }
    List<RequestView> filteredViews =
        views.stream()
            .filter(view -> normalizedFilter == null || view.state().equals(normalizedFilter))
            .toList();
    if (limit == null || limit >= filteredViews.size()) {
      return filteredViews;
    }
    return filteredViews.subList(0, limit);
  }

  private RequestView toRequestView(StoredDumpRequestDetail storedRequestDetail) {
    StoredDumpRequest storedRequest = storedRequestDetail.storedRequest();
    var request = storedRequest.request();
    DumpRequestStatus status = storedRequestDetail.status();
    String state = status == null ? "QUEUED" : status.state().name();
    Map<String, Object> table = request.tableId() == null ? null : tablePayload(request.tableId());
    return new RequestView(
        request.requestId(),
        request.scope().name(),
        table,
        state,
        request.primaryKeyLiterals(),
        status == null ? List.of() : status.missingPrimaryKeyLiterals(),
        null,
        status == null ? null : status.failureReason(),
        storedRequest.createdAt(),
        storedRequest.updatedAt());
  }

  private Map<String, Object> requestPayload(RequestView request) {
    LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
    payload.put("requestId", request.requestId());
    payload.put("scope", request.scope());
    payload.put("table", request.table());
    payload.put("state", request.state());
    payload.put("primaryKeyLiterals", request.primaryKeyLiterals());
    payload.put("missingPrimaryKeyLiterals", request.missingPrimaryKeyLiterals());
    payload.put("activeWindow", request.activeWindow());
    payload.put("failureReason", request.failureReason());
    payload.put("createdAt", request.createdAt().toString());
    payload.put("updatedAt", request.updatedAt().toString());
    return payload;
  }

  private Map<String, Object> schemaViewPayload(
      TableSchema contractSchema,
      Map<String, FullDumpRequiredSignal> fullDumpByTable,
      Map<String, SchemaUncertaintySignal> uncertaintyByTable) {
    String tableDisplayName = contractSchema.tableId().displayName();
    TableSchema observedSchema = stateStore.schemas().loadObservedSchema(tableDisplayName).orElse(null);
    FullDumpRequiredSignal fullDumpSignal = fullDumpByTable.get(tableDisplayName);
    SchemaUncertaintySignal uncertaintySignal = uncertaintyByTable.get(tableDisplayName);
    String schemaStatus =
        fullDumpSignal != null
            ? "FULL_DUMP_REQUIRED"
            : uncertaintySignal != null ? "SCHEMA_UNCERTAIN" : "OK";
    LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
    payload.put("tableDisplayName", tableDisplayName);
    payload.put("databaseName", contractSchema.tableId().databaseName());
    payload.put("schemaName", contractSchema.tableId().schemaName());
    payload.put("tableName", contractSchema.tableId().tableName());
    payload.put("contractSchema", schemaDetailPayload(contractSchema));
    payload.put("observedSchema", observedSchema == null ? null : schemaDetailPayload(observedSchema));
    payload.put("schemaStatus", schemaStatus);
    payload.put("fullDumpRequired", fullDumpSignal != null);
    payload.put("fullDumpReason", fullDumpSignal == null ? null : fullDumpSignal.reason());
    payload.put("schemaUncertain", uncertaintySignal != null);
    payload.put(
        "schemaUncertaintyReason", uncertaintySignal == null ? null : uncertaintySignal.reason());
    return payload;
  }

  private Map<String, Object> schemaDetailPayload(TableSchema schema) {
    return Map.of(
        "fingerprint", schema.fingerprint(),
        "refreshedAt", schema.refreshedAt().toString(),
        "primaryKeyColumns", schema.primaryKeyColumns(),
        "ignoredColumns", schema.ignoredColumns(),
        "columns", schema.columns().stream().map(this::columnPayload).toList());
  }

  private Map<String, Object> columnPayload(ColumnDefinition column) {
    return Map.of(
        "name", column.name(),
        "sourceType", column.sourceType(),
        "neutralType", column.neutralType().name(),
        "primaryKey", column.primaryKey(),
        "primaryKeyOrdinal", column.primaryKeyOrdinal(),
        "nullable", column.nullable(),
        "supported", column.supported());
  }

  private Map<String, Object> fullDumpSignalPayload(FullDumpRequiredSignal signal) {
    LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
    payload.put("sourceId", signal.sourceId());
    payload.put("table", signal.tableId() == null ? null : tablePayload(signal.tableId()));
    payload.put("reason", signal.reason());
    payload.put("detectedAt", signal.detectedAt().toString());
    return payload;
  }

  private Map<String, Object> schemaUncertaintySignalPayload(SchemaUncertaintySignal signal) {
    LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
    payload.put("sourceId", signal.sourceId());
    payload.put("table", signal.tableId() == null ? null : tablePayload(signal.tableId()));
    payload.put("reason", signal.reason());
    payload.put("firstDetectedAt", signal.firstDetectedAt().toString());
    payload.put("lastDetectedAt", signal.lastDetectedAt().toString());
    payload.put("occurrenceCount", signal.occurrenceCount());
    return payload;
  }

  private Map<String, Object> tablePayload(TableId tableId) {
    return Map.of(
        "databaseName", tableId.databaseName(),
        "schemaName", tableId.schemaName(),
        "tableName", tableId.tableName(),
        "displayName", tableId.displayName());
  }

  private Map<String, Object> recordedEventPayload(ControlPlaneEventStore.RecordedEvent recordedEvent) {
    LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
    payload.put("stageLabel", recordedEvent.stageLabel());
    payload.put("recordedAt", recordedEvent.recordedAt().toString());
    payload.put("table", tablePayload(recordedEvent.event().tableId()));
    payload.put("operationType", recordedEvent.event().operationType().name());
    payload.put("captureOrigin", recordedEvent.event().captureOrigin().name());
    payload.put("primaryKey", recordedEvent.event().primaryKey());
    payload.put("sourcePosition", recordedEvent.event().sourcePosition().displayValue());
    payload.put("dumpId", recordedEvent.event().dumpId());
    return payload;
  }

  private Map<String, Object> tableSummaryPayload(ControlPlaneEventStore.TableEventSummary summary) {
    return Map.of(
        "tableDisplayName", summary.tableDisplayName(),
        "recentEventCountInWindow", summary.recentEventCountInWindow());
  }

  private Map<String, Object> eventCapturePayload() {
    LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
    payload.put("cumulativeEventsObserved", eventStore.cumulativeEventsObserved());
    payload.put("recentEventWindowSize", eventStore.recentEventWindowSize());
    return payload;
  }

  private Map<String, Object> sourceFlowControlPayload(
      SourceFlowControlSnapshot snapshot) {
    LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
    payload.put("mode", snapshot.mode().name());
    payload.put("queueCapacity", snapshot.queueCapacity());
    payload.put("queueDepth", snapshot.queueDepth());
    payload.put("maxQueueDepth", snapshot.maxQueueDepth());
    payload.put("queueFull", snapshot.queueFull());
    payload.put("sourceFetchPaused", snapshot.sourceFetchPaused());
    payload.put("pausedSince", snapshot.pausedSince());
    payload.put("pauseAgeSeconds", snapshot.pauseAgeSeconds());
    payload.put("totalPausedSeconds", snapshot.totalPausedSeconds());
    payload.put("totalEnqueued", snapshot.totalEnqueued());
    payload.put("totalDequeued", snapshot.totalDequeued());
    payload.put("totalPauseCount", snapshot.totalPauseCount());
    payload.put("enqueueRatePerSecond", snapshot.enqueueRatePerSecond());
    payload.put("dequeueRatePerSecond", snapshot.dequeueRatePerSecond());
    return payload;
  }

  private Map<String, Object> runtimeMeasurementPayload() {
    if (meterRegistry == null || measurementAdapterTag == null || measurementAdapterTag.isBlank()) {
      return Map.of();
    }
    RuntimeMeasurementMetricsSnapshot snapshot =
        RuntimeMeasurementMetricsSnapshot.capture(meterRegistry, measurementAdapterTag);
    LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
    payload.put("sourceCapture", sourceCaptureMeasurementPayload(snapshot));
    payload.put("chunking", chunkingMeasurementPayload(snapshot));
    payload.put("requestCoordinator", requestCoordinatorMeasurementPayload(snapshot));
    return payload;
  }

  private Map<String, Object> sourceCaptureMeasurementPayload(
      RuntimeMeasurementMetricsSnapshot snapshot) {
    LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
    payload.put("polls", snapshot.sourceCapturePolls());
    payload.put("emptyPolls", snapshot.sourceCaptureEmptyPolls());
    payload.put("transactions", snapshot.sourceCaptureTransactions());
    payload.put("events", snapshot.sourceCaptureEvents());
    payload.put("pollFailures", snapshot.sourceCapturePollFailures());
    payload.put(
        "transactionDecodeDurationMillisTotal",
        snapshot.sourceCaptureTransactionDecodeDurationMillisTotal());
    payload.put(
        "transactionDecodeDurationMillisMax",
        snapshot.sourceCaptureTransactionDecodeDurationMillisMax());
    payload.put("acknowledgeCalls", snapshot.sourceCaptureAcknowledgeCalls());
    payload.put("acknowledgeFailures", snapshot.sourceCaptureAcknowledgeFailures());
    payload.put(
        "acknowledgeDurationMillisTotal", snapshot.sourceCaptureAcknowledgeDurationMillisTotal());
    payload.put(
        "acknowledgeDurationMillisMax", snapshot.sourceCaptureAcknowledgeDurationMillisMax());
    payload.put("heartbeatsWritten", snapshot.sourceCaptureHeartbeatsWritten());
    payload.put("heartbeatsSkipped", snapshot.sourceCaptureHeartbeatsSkipped());
    payload.put("heartbeatFailures", snapshot.sourceCaptureHeartbeatFailures());
    payload.put("heartbeatDurationMillisTotal", snapshot.sourceCaptureHeartbeatDurationMillisTotal());
    payload.put("heartbeatDurationMillisMax", snapshot.sourceCaptureHeartbeatDurationMillisMax());
    return payload;
  }

  private Map<String, Object> chunkingMeasurementPayload(
      RuntimeMeasurementMetricsSnapshot snapshot) {
    LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
    payload.put("upperBoundReads", snapshot.chunkUpperBoundReads());
    payload.put("upperBoundReadMisses", snapshot.chunkUpperBoundReadMisses());
    payload.put("upperBoundReadFailures", snapshot.chunkUpperBoundReadFailures());
    payload.put("upperBoundReadDurationMillisTotal", snapshot.chunkUpperBoundReadDurationMillisTotal());
    payload.put("upperBoundReadDurationMillisMax", snapshot.chunkUpperBoundReadDurationMillisMax());
    payload.put("nextTableChunkCalls", snapshot.nextTableChunkCalls());
    payload.put("nextTableChunkEmpty", snapshot.nextTableChunkEmpty());
    payload.put("nextTableChunkRows", snapshot.nextTableChunkRows());
    payload.put("nextTableChunkFinal", snapshot.nextTableChunkFinal());
    payload.put("nextTableChunkFailures", snapshot.nextTableChunkFailures());
    payload.put("nextTableChunkDurationMillisTotal", snapshot.nextTableChunkDurationMillisTotal());
    payload.put("nextTableChunkDurationMillisMax", snapshot.nextTableChunkDurationMillisMax());
    payload.put("targetedPrimaryKeyChunkCalls", snapshot.targetedPrimaryKeyChunkCalls());
    payload.put("targetedPrimaryKeyChunkEmpty", snapshot.targetedPrimaryKeyChunkEmpty());
    payload.put("targetedPrimaryKeyRequestedKeys", snapshot.targetedPrimaryKeyRequestedKeys());
    payload.put("targetedPrimaryKeyRows", snapshot.targetedPrimaryKeyRows());
    payload.put("targetedPrimaryKeyFailures", snapshot.targetedPrimaryKeyFailures());
    payload.put(
        "targetedPrimaryKeyDurationMillisTotal",
        snapshot.targetedPrimaryKeyDurationMillisTotal());
    payload.put(
        "targetedPrimaryKeyDurationMillisMax",
        snapshot.targetedPrimaryKeyDurationMillisMax());
    return payload;
  }

  private Map<String, Object> requestCoordinatorMeasurementPayload(
      RuntimeMeasurementMetricsSnapshot snapshot) {
    LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
    payload.put("coordinateCalls", snapshot.coordinateCalls());
    payload.put("coordinateEmpty", snapshot.coordinateEmpty());
    payload.put("coordinateFailures", snapshot.coordinateFailures());
    payload.put("coordinateDurationMillisTotal", snapshot.coordinateDurationMillisTotal());
    payload.put("coordinateDurationMillisMax", snapshot.coordinateDurationMillisMax());
    payload.put("acknowledgeCalls", snapshot.acknowledgeCalls());
    payload.put("acknowledgeFailures", snapshot.acknowledgeFailures());
    payload.put(
        "acknowledgeDurationMillisTotal", snapshot.acknowledgeDurationMillisTotal());
    payload.put("acknowledgeDurationMillisMax", snapshot.acknowledgeDurationMillisMax());
    return payload;
  }

  private Map<String, FullDumpRequiredSignal> indexFullDumpSignals(
      List<FullDumpRequiredSignal> signals) {
    LinkedHashMap<String, FullDumpRequiredSignal> indexed = new LinkedHashMap<>();
    for (FullDumpRequiredSignal signal : signals) {
      if (signal.tableId() != null) {
        indexed.put(signal.tableId().displayName(), signal);
      }
    }
    return indexed;
  }

  private Map<String, SchemaUncertaintySignal> indexSchemaUncertaintySignals(
      List<SchemaUncertaintySignal> signals) {
    LinkedHashMap<String, SchemaUncertaintySignal> indexed = new LinkedHashMap<>();
    for (SchemaUncertaintySignal signal : signals) {
      if (signal.tableId() != null) {
        indexed.put(signal.tableId().displayName(), signal);
      }
    }
    return indexed;
  }

  public record RequestView(
      String requestId,
      String scope,
      Map<String, Object> table,
      String state,
      List<String> primaryKeyLiterals,
      List<String> missingPrimaryKeyLiterals,
      Map<String, Object> activeWindow,
      String failureReason,
      Instant createdAt,
      Instant updatedAt) {}
}
