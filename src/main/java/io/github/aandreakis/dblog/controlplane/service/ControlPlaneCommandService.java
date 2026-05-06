package io.github.aandreakis.dblog.controlplane.service;

import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import java.util.List;
import java.util.Objects;

/**
 * Validates and submits operator requests into the runtime coordinator boundary.
 *
 * <p>The control plane is intentionally submit-only. It accepts new dump or targeted-repair
 * requests and exposes their state through query surfaces, but it does not provide pause, resume,
 * or cancel semantics for running work. Operators throttle or pause ingest by stopping and
 * starting the DBLog process; active work resumes from durable chunk boundaries on restart.
 */
public final class ControlPlaneCommandService {
  private final RuntimeStateStore stateStore;
  private final RuntimeStatusProvider runtimeStatusProvider;
  private final RequestSubmissionGateway requestSubmissionGateway;

  public ControlPlaneCommandService(
      RuntimeStateStore stateStore,
      RuntimeStatusProvider runtimeStatusProvider,
      RequestSubmissionGateway requestSubmissionGateway) {
    this.stateStore = Objects.requireNonNull(stateStore, "stateStore");
    this.runtimeStatusProvider = Objects.requireNonNull(runtimeStatusProvider, "runtimeStatusProvider");
    this.requestSubmissionGateway =
        Objects.requireNonNull(requestSubmissionGateway, "requestSubmissionGateway");
  }

  public RuntimeStateStore stateStore() {
    return stateStore;
  }

  /**
   * Validate an operator request payload and submit it to the runtime coordinator.
   *
   * <p>Validation order is: runtime availability → scope shape → table identity → primary-key
   * literal shape and schema-aware canonicalization. Each step fails fast with a typed exception:
   *
   * <ul>
   *   <li>{@link RequestSubmissionUnavailableException} when the runtime is not currently
   *       accepting submissions: no request-processing runtime is active in this process, the
   *       active runtime has stopped, or the active runtime failed closed. The exception
   *       message comes from the runtime status snapshot.
   *   <li>{@link IllegalArgumentException} for any malformed payload — unknown scope,
   *       missing/extra {@code table}, missing/extra {@code primaryKeyLiterals}, or
   *       {@code PRIMARY_KEYS} literals that fail schema-aware canonicalization against the
   *       captured-schema set.
   * </ul>
   *
   * <p>On success the request is durably persisted as {@code QUEUED} via the submission gateway
   * and the returned {@link DumpRequest} carries the assigned numeric request id. This method
   * does not start the request — the runtime coordinator picks queued requests up on its next
   * poll.
   */
  public DumpRequest submit(RequestPayload payload) {
    Objects.requireNonNull(payload, "payload");
    RuntimeStatusProvider.RuntimeStatusSnapshot runtime = runtimeStatusProvider.snapshot();
    if (!runtime.requestSubmissionAvailable()) {
      throw new RequestSubmissionUnavailableException(runtime.requestSubmissionMessage());
    }
    DumpScope scope = parseScope(payload.scope());
    List<String> primaryKeyLiterals =
        payload.primaryKeyLiterals() == null ? List.of() : List.copyOf(payload.primaryKeyLiterals());
    TableId tableId = validateAndParseTable(scope, payload.table(), primaryKeyLiterals);
    return requestSubmissionGateway.submit(
        scope, tableId, resolvePrimaryKeyTuples(scope, tableId, primaryKeyLiterals));
  }

  private DumpScope parseScope(String raw) {
    if (raw == null || raw.isBlank()) {
      throw new IllegalArgumentException("scope must not be blank");
    }
    try {
      return DumpScope.valueOf(raw.trim().toUpperCase(java.util.Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("unsupported scope: " + raw, e);
    }
  }

  private TableId validateAndParseTable(
      DumpScope scope, TablePayload payload, List<String> primaryKeyLiterals) {
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(primaryKeyLiterals, "primaryKeyLiterals");
    return switch (scope) {
      case ALL_TABLES -> {
        if (payload != null) {
          throw new IllegalArgumentException("ALL_TABLES requests must not include table");
        }
        if (!primaryKeyLiterals.isEmpty()) {
          throw new IllegalArgumentException("ALL_TABLES requests must not include primaryKeyLiterals");
        }
        yield null;
      }
      case TABLE -> {
        if (payload == null) {
          throw new IllegalArgumentException("TABLE requests must include table");
        }
        if (!primaryKeyLiterals.isEmpty()) {
          throw new IllegalArgumentException("TABLE requests must not include primaryKeyLiterals");
        }
        yield new TableId(
            requireNonBlank(payload.databaseName(), "table.databaseName"),
            requireNonBlank(payload.schemaName(), "table.schemaName"),
            requireNonBlank(payload.tableName(), "table.tableName"));
      }
      case PRIMARY_KEYS -> {
        if (payload == null) {
          throw new IllegalArgumentException("PRIMARY_KEYS requests must include table");
        }
        if (primaryKeyLiterals.isEmpty()) {
          throw new IllegalArgumentException(
              "PRIMARY_KEYS requests must include at least one primaryKeyLiterals entry");
        }
        yield new TableId(
            requireNonBlank(payload.databaseName(), "table.databaseName"),
            requireNonBlank(payload.schemaName(), "table.schemaName"),
            requireNonBlank(payload.tableName(), "table.tableName"));
      }
    };
  }

  private List<PrimaryKeyTuple> resolvePrimaryKeyTuples(
      DumpScope scope, TableId tableId, List<String> primaryKeyLiterals) {
    if (scope != DumpScope.PRIMARY_KEYS) {
      return List.of();
    }
    TableSchema schema =
        stateStore
            .schemas()
            .loadContractSchema(tableId.displayName())
            .or(() -> stateStore.schemas().loadObservedSchema(tableId.displayName()))
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "PRIMARY_KEYS requests require a known captured schema for "
                            + tableId.displayName()));
    return schema.primaryKeyTuplesFromLiterals(primaryKeyLiterals);
  }

  private static String requireNonBlank(String value, String fieldName) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(fieldName + " must not be blank");
    }
    return value.trim();
  }

  public record RequestPayload(
      String scope, TablePayload table, List<String> primaryKeyLiterals) {}

  public record TablePayload(String databaseName, String schemaName, String tableName) {}

  public static final class RequestSubmissionUnavailableException extends IllegalStateException {
    public RequestSubmissionUnavailableException(String message) {
      super(message);
    }
  }
}
