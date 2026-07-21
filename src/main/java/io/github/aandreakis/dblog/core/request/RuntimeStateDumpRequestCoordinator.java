package io.github.aandreakis.dblog.core.request;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.FullDumpRequiredSignal;
import io.github.aandreakis.dblog.core.schema.SchemaDriftException;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.state.api.DumpRequestRepository;
import io.github.aandreakis.dblog.state.api.SchemaStateRepository;
import io.github.aandreakis.dblog.tap.Tap;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RuntimeStateDumpRequestCoordinator<TX extends SourceTransaction<?>>
    implements DumpRequestCoordinator<TX> {
  private static final Logger log = LoggerFactory.getLogger(RuntimeStateDumpRequestCoordinator.class);

  private final String adapterLabel;
  private final String sourceId;
  private final DumpRequestRepository dumpRequests;
  private final SchemaStateRepository schemas;
  private final DumpWindowCoordinator<TX> dumpWindowCoordinator;
  private final TargetedRepairCoordinator<TX> targetedRepairCoordinator;
  private final Supplier<List<TableSchema>> capturedSchemasSupplier;
  private final int chunkSize;
  private final Tap tap;

  public RuntimeStateDumpRequestCoordinator(
      String adapterLabel,
      String sourceId,
      DumpRequestRepository dumpRequests,
      SchemaStateRepository schemas,
      DumpWindowCoordinator<TX> dumpWindowCoordinator,
      TargetedRepairCoordinator<TX> targetedRepairCoordinator,
      Supplier<List<TableSchema>> capturedSchemasSupplier,
      int chunkSize,
      Tap tap) {
    this.adapterLabel = requireNonBlank(adapterLabel, "adapterLabel");
    this.sourceId = requireNonBlank(sourceId, "sourceId");
    this.dumpRequests = Objects.requireNonNull(dumpRequests, "dumpRequests");
    this.schemas = Objects.requireNonNull(schemas, "schemas");
    this.dumpWindowCoordinator =
        Objects.requireNonNull(dumpWindowCoordinator, "dumpWindowCoordinator");
    this.targetedRepairCoordinator =
        Objects.requireNonNull(targetedRepairCoordinator, "targetedRepairCoordinator");
    this.capturedSchemasSupplier =
        Objects.requireNonNull(capturedSchemasSupplier, "capturedSchemasSupplier");
    if (chunkSize <= 0) {
      throw new IllegalArgumentException("chunkSize must be > 0");
    }
    this.chunkSize = chunkSize;
    this.tap = Objects.requireNonNull(tap, "tap");
  }

  @Override
  public Optional<ScheduledRequestBatch<TX>> coordinateNextBatch() {
    List<TableSchema> orderedSchemas = List.copyOf(capturedSchemasSupplier.get());
    Map<TableId, TableSchema> schemasById = schemasById(orderedSchemas);

    for (DumpRequest request : dumpRequests.loadPending()) {
      Optional<DumpRequestStatus> status = dumpRequests.loadStatus(request.requestId());
      if (status.isPresent()) {
        DumpRequestState persistedState = status.orElseThrow().state();
        if (persistedState == DumpRequestState.COMPLETED
            || persistedState == DumpRequestState.FAILED) {
          continue;
        }
      }
      switch (request.scope()) {
        case TABLE -> {
          TableSchema schema = resolveTableSchema(request, schemasById);
          if (schema == null) {
            failRequestWarn(
                request,
                adapterLabel
                    + " dump request targets a table that is not present in the current capturedSchemas set: "
                    + (request.tableId() == null ? "<none>" : request.tableId().displayName()));
            continue;
          }
          if (!supportsCurrentDumpContract(schema)) {
            failRequestAndRequireFullDump(
                request,
                adapterLabel
                    + " cannot continue TABLE dump request because the current primary-key shape no longer supports dump ordering for "
                    + schema.tableId().displayName());
            continue;
          }
          if (status.isEmpty() || status.orElseThrow().state() != DumpRequestState.ACTIVE) {
            if (!saveRuntimeOwnedStatus(
                request,
                DumpRequestStatus.active(request),
                previousState(status),
                null,
                true)) {
              continue;
            }
          }
          Optional<DumpWindowOutcome<TX>> outcome;
          try {
            outcome = dumpWindowCoordinator.coordinateNextTableChunk(request.requestId(), schema, chunkSize);
          } catch (SchemaDriftException schemaDrift) {
            failRequestAndRequireFullDump(
                request,
                adapterLabel
                    + " detected schema drift while running TABLE dump request: "
                    + schemaDrift.getMessage());
            continue;
          }
          if (outcome.isEmpty()) {
            saveRuntimeOwnedStatus(
                request,
                DumpRequestStatus.completed(request, List.of()),
                DumpRequestState.ACTIVE,
                null,
                false);
            continue;
          }
          return Optional.of(ScheduledRequestBatch.table(request, outcome.orElseThrow()));
        }
        case PRIMARY_KEYS -> {
          TableSchema schema = resolveTableSchema(request, schemasById);
          if (schema == null) {
            failRequestWarn(
                request,
                adapterLabel
                    + " dump request targets a table that is not present in the current capturedSchemas set: "
                    + (request.tableId() == null ? "<none>" : request.tableId().displayName()));
            continue;
          }
          if (!supportsCurrentDumpContract(schema)) {
            failRequestAndRequireFullDump(
                request,
                adapterLabel
                    + " cannot continue PRIMARY_KEYS request because the current primary-key shape no longer supports targeted repair for "
                    + schema.tableId().displayName());
            continue;
          }
          Optional<ScheduledRequestBatch<TX>> batch;
          try {
            batch = coordinatePrimaryKeyRequest(request, status, schema);
          } catch (SchemaDriftException schemaDrift) {
            failRequestAndRequireFullDump(
                request,
                adapterLabel
                    + " detected schema drift while running PRIMARY_KEYS request: "
                    + schemaDrift.getMessage());
            continue;
          } catch (IllegalArgumentException invalidRequest) {
            failRequestWarn(
                request,
                adapterLabel
                    + " PRIMARY_KEYS request contains invalid primary-key literal input for "
                    + schema.tableId().displayName()
                    + ": "
                    + invalidRequest.getMessage());
            continue;
          }
          if (batch.isPresent()) {
            return batch;
          }
        }
        case ALL_TABLES -> {
          Optional<ScheduledRequestBatch<TX>> batch;
          try {
            batch = coordinateAllTablesRequest(request, status, orderedSchemas);
          } catch (SchemaDriftException schemaDrift) {
            failRequestAndRequireFullDump(
                request,
                adapterLabel
                    + " detected schema drift while running ALL_TABLES request: "
                    + schemaDrift.getMessage());
            continue;
          }
          if (batch.isPresent()) {
            return batch;
          }
        }
      }
    }
    return Optional.empty();
  }

  @Override
  public void acknowledgeCompletedBatch(ScheduledRequestBatch<TX> batch) {
    Objects.requireNonNull(batch, "batch");
    if (batch.request().scope() == DumpScope.TABLE
        || batch.request().scope() == DumpScope.ALL_TABLES) {
      dumpWindowCoordinator.acknowledgeCompletedBatch(batch.tableOutcome());
      if (batch.finalRequestBatch()) {
        saveRuntimeOwnedStatus(
            batch.request(),
            DumpRequestStatus.completed(batch.request(), List.of()),
            DumpRequestState.ACTIVE,
            null,
            false);
      }
      return;
    }
    if (batch.request().scope() == DumpScope.PRIMARY_KEYS) {
      targetedRepairCoordinator.acknowledge(batch.targetedOutcome());
      saveRuntimeOwnedStatus(
          batch.request(),
          DumpRequestStatus.completed(batch.request(), batch.missingPrimaryKeyTuples()),
          DumpRequestState.ACTIVE,
          null,
          false);
      return;
    }
    throw new IllegalArgumentException(
        "Unsupported scheduled request scope for acknowledgement: " + batch.request().scope());
  }

  @Override
  public int pendingRequestCount() {
    return dumpRequests.countPending();
  }

  private Optional<ScheduledRequestBatch<TX>> coordinatePrimaryKeyRequest(
      DumpRequest request, Optional<DumpRequestStatus> status, TableSchema schema) {
    DumpRequestStatus activeStatus =
        status.filter(saved -> saved.state() == DumpRequestState.ACTIVE).orElse(null);
    if (activeStatus == null) {
      if (!saveRuntimeOwnedStatus(
          request,
          DumpRequestStatus.active(request),
          previousState(status),
          null,
          true)) {
        return Optional.empty();
      }
    }
    return coordinateFreshPrimaryKeyRequest(request, schema);
  }

  private Optional<ScheduledRequestBatch<TX>> coordinateFreshPrimaryKeyRequest(
      DumpRequest request, TableSchema schema) {
    TargetedRepairResult<TX> result = targetedRepairCoordinator.coordinate(request, schema);
    if (result.outcome().isEmpty()) {
      saveRuntimeOwnedStatus(
          request,
          DumpRequestStatus.completed(request, result.missingPrimaryKeyTuples()),
          DumpRequestState.ACTIVE,
          null,
          false);
      return Optional.empty();
    }

    TargetedRepairOutcome<TX> outcome = result.outcome().orElseThrow();
    // The save-status call here re-confirms ACTIVE over ACTIVE; no state transition, so no
    // request.transition event is emitted (saveRuntimeOwnedStatus filters on previous != current).
    if (!saveRuntimeOwnedStatus(
        request,
        DumpRequestStatus.active(request),
        DumpRequestState.ACTIVE,
        null,
        false)) {
      return Optional.empty();
    }
    return Optional.of(ScheduledRequestBatch.targetedRepair(request, outcome));
  }

  private Optional<ScheduledRequestBatch<TX>> coordinateAllTablesRequest(
      DumpRequest request,
      Optional<DumpRequestStatus> status,
      List<TableSchema> capturedSchemas) {
    if (status.isEmpty() || status.orElseThrow().state() != DumpRequestState.ACTIVE) {
      if (!saveRuntimeOwnedStatus(
          request,
          DumpRequestStatus.active(request),
          previousState(status),
          null,
          true)) {
        return Optional.empty();
      }
    }

    for (int index = 0; index < capturedSchemas.size(); index++) {
      TableSchema schema = capturedSchemas.get(index);
      if (!supportsCurrentDumpContract(schema)) {
        failRequestAndRequireFullDump(
            request,
            adapterLabel
                + " cannot continue ALL_TABLES request because the current primary-key shape no longer supports dump ordering for "
                + schema.tableId().displayName());
        return Optional.empty();
      }

      Optional<DumpWindowOutcome<TX>> outcome =
          dumpWindowCoordinator.coordinateNextTableChunk(request.requestId(), schema, chunkSize);
      if (outcome.isEmpty()) {
        continue;
      }

      // ALL_TABLES batches never carry finalRequestBatch=true. Eagerly proving "no later table
      // has work" required one chunk-sized SELECT per unstarted later table at every table
      // transition — O(N^2) wasted SELECTs per dump. The natural completion path below marks
      // the request COMPLETED on the next coordinator poll once every table has hit its
      // persisted upper bound (a pure in-memory check). That poll happens within one idle
      // refresh tick (~250ms); callers see the same COMPLETED status, just slightly later.
      return Optional.of(ScheduledRequestBatch.table(request, outcome.orElseThrow(), false));
    }

    saveRuntimeOwnedStatus(
        request,
        DumpRequestStatus.completed(request, List.of()),
        DumpRequestState.ACTIVE,
        null,
        false);
    return Optional.empty();
  }

  private void failRequestWarn(DumpRequest request, String reason) {
    failRequest(request, reason, true);
  }

  private void failRequest(DumpRequest request, String reason, boolean warnOnly) {
    // `previous` is passed as null because failRequest is invoked from paths that have not
    // loaded the current status row; the tap event's prev_state field is documented as
    // optional, so omitting it is acceptable.
    boolean saved =
        saveRuntimeOwnedStatus(
            request,
            DumpRequestStatus.failed(request, reason),
            null,
            reason,
            true);
    if (!saved) {
      return;
    }
    String sanitizedReason = sanitizeForLog(reason);
    String tableIdForLog = tableIdOrAllTables(request);
    if (warnOnly) {
      log.warn(
          "dump request failed requestId={} scope={} tableId={} reason={}",
          request.requestId(),
          request.scope(),
          tableIdForLog,
          sanitizedReason);
    } else {
      log.error(
          "dump request failed and requires a full dump requestId={} scope={} tableId={} reason={}",
          request.requestId(),
          request.scope(),
          tableIdForLog,
          sanitizedReason);
    }
  }

  private void failRequestAndRequireFullDump(DumpRequest request, String reason) {
    failRequest(request, reason, false);
    schemas.saveFullDumpRequiredSignal(
        new FullDumpRequiredSignal(sourceId, request.tableId(), reason, Instant.now()));
  }

  private static String tableIdOrAllTables(DumpRequest request) {
    return request.tableId() == null ? "<all-tables>" : request.tableId().displayName();
  }

  /**
   * Strips CR/LF from strings we interpolate into log lines. Our callers build {@code reason}
   * from adapter-supplied labels and exception messages; neither is attacker-controlled, but
   * sanitizing keeps log records on a single line even if an upstream source slips a newline
   * into an error.
   */
  private static String sanitizeForLog(String value) {
    if (value == null) {
      return "<none>";
    }
    return value.replace('\n', ' ').replace('\r', ' ');
  }

  private static boolean supportsCurrentDumpContract(TableSchema schema) {
    return schema.supportsDumpPrimaryKeyContract();
  }

  /**
   * Persists {@code newStatus} iff the currently-stored state is {@code ACTIVE} (or missing when
   * {@code allowMissing=true}), and emits a {@code request.transition} tap event when the save
   * succeeds AND the state actually changed. Same-state idempotent refreshes do not emit.
   */
  private boolean saveRuntimeOwnedStatus(
      DumpRequest request,
      DumpRequestStatus newStatus,
      DumpRequestState previous,
      String reason,
      boolean allowMissing) {
    boolean saved =
        dumpRequests.saveStatusIfCurrentStateIn(
            newStatus, allowMissing, DumpRequestState.ACTIVE);
    if (saved && previous != newStatus.state()) {
      tap.onRequestTransition(
          request.requestId(),
          request.scope(),
          request.tableId(),
          previous,
          newStatus.state(),
          reason);
    }
    if (saved && newStatus.state() == DumpRequestState.COMPLETED) {
      // Bound state-store growth: a fresh COMPLETED supersedes its prior
      // terminal-state entries for the same scope/table. Failed requests stay
      // visible for forensics until a same-scope same-table COMPLETED replaces
      // them. See DumpRequestRepository#pruneSupersededTerminalRequests.
      dumpRequests.pruneSupersededTerminalRequests(request);
    }
    return saved;
  }

  private static DumpRequestState previousState(Optional<DumpRequestStatus> status) {
    return status.map(DumpRequestStatus::state).orElse(null);
  }

  private static Map<TableId, TableSchema> schemasById(
      List<TableSchema> capturedSchemas) {
    Map<TableId, TableSchema> byId = new LinkedHashMap<>();
    for (TableSchema schema : capturedSchemas) {
      Objects.requireNonNull(schema, "capturedSchemas element");
      TableSchema previous = byId.put(schema.tableId(), schema);
      if (previous != null) {
        throw new IllegalArgumentException(
            "capturedSchemas must not contain duplicate tables: "
                + schema.tableId().displayName());
      }
    }
    return Map.copyOf(byId);
  }

  private static TableSchema resolveTableSchema(
      DumpRequest request, Map<TableId, TableSchema> schemasById) {
    return schemasById.get(request.tableId());
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
