package io.github.aandreakis.dblog.state.api;

import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.core.request.DumpRequestState;
import io.github.aandreakis.dblog.core.request.DumpRequestStatus;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import java.util.List;
import java.util.Optional;

/**
 * Durable store for operator-submitted dump and targeted-repair requests, plus their lifecycle
 * status.
 *
 * <p>A request and its {@link DumpRequestStatus} are stored separately: the request is the
 * operator's intent (immutable after submit); the status records progression through {@code
 * PENDING → ACTIVE → COMPLETED|FAILED}. The two-table split lets the runtime advance status
 * without rewriting the request payload.
 */
public interface DumpRequestRepository {
  void upsert(DumpRequest request);

  /** Inserts {@code request} only if no request with the same id already exists; returns {@code
   * true} on insert. */
  boolean createIfAbsent(DumpRequest request);

  /**
   * Generates a new internal request and persists it. The implementation owns id generation so
   * callers cannot collide with operator-submitted ids.
   */
  DumpRequest createGenerated(DumpScope scope, TableId tableId, List<PrimaryKeyTuple> primaryKeyTuples);

  List<DumpRequest> loadPending();

  List<DumpRequest> loadAll();

  List<StoredDumpRequest> loadAllDetailed();

  default List<StoredDumpRequestDetail> loadAllDetailedWithStatus() {
    return loadAllDetailed().stream()
        .map(
            storedRequest ->
                new StoredDumpRequestDetail(
                    storedRequest,
                    loadStatus(storedRequest.request().requestId()).orElse(null)))
        .toList();
  }

  int countPending();

  Optional<DumpRequest> loadRequest(String requestId);

  Optional<StoredDumpRequest> loadRequestDetailed(String requestId);

  default Optional<StoredDumpRequestDetail> loadRequestDetailedWithStatus(String requestId) {
    Optional<StoredDumpRequest> storedRequest = loadRequestDetailed(requestId);
    if (storedRequest.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        new StoredDumpRequestDetail(
            storedRequest.orElseThrow(), loadStatus(requestId).orElse(null)));
  }

  void saveStatus(DumpRequestStatus status);

  /**
   * Convenience for {@link #saveStatusIfCurrentStateIn} with {@code allowMissing=true} and the
   * single allowed state {@code ACTIVE}.
   */
  boolean saveStatusIfAbsentOrActive(DumpRequestStatus status);

  /**
   * Conditional status update. Writes {@code status} only if the persisted current state is in
   * {@code allowedCurrentStates}, or — when {@code allowMissing} is true — if no status row exists
   * yet. Returns whether the write happened. Implementations must enforce the guard atomically
   * (single-statement or transactional read-modify-write) so concurrent transitions cannot race.
   */
  boolean saveStatusIfCurrentStateIn(
      DumpRequestStatus status, boolean allowMissing, DumpRequestState... allowedCurrentStates);

  Optional<DumpRequestStatus> loadStatus(String requestId);

  void failNonTerminalRequests(String reason);

  /**
   * Prune historic terminal-state requests that the given just-completed request supersedes.
   *
   * <p>Called immediately after a request transitions to {@link DumpRequestState#COMPLETED}. The
   * implementation must, in a single transaction:
   *
   * <ul>
   *   <li>For a {@code TABLE} or {@code PRIMARY_KEYS} scope request: delete every other request in
   *       a terminal state ({@code COMPLETED} or {@code FAILED}) that targets the same
   *       {@code (databaseName, schemaName, tableName)} and has a strictly lower
   *       {@code REQUEST_SEQUENCE}. The just-completed request itself is preserved.
   *   <li>For an {@code ALL_TABLES} scope request: delete every other {@code ALL_TABLES} request
   *       in a terminal state with a strictly lower {@code REQUEST_SEQUENCE}.
   *   <li>Across both cases: never touch requests in non-terminal states ({@code ACTIVE} or no
   *       persisted status), and never delete the just-completed request itself.
   *   <li>Each pruned request must be removed atomically across all dependent tables
   *       ({@code DUMP_REQUEST_STATUS}, {@code DUMP_REQUEST}, {@code DUMP_REQUEST_KEY},
   *       {@code DUMP_REQUEST_MISSING_KEY}, {@code DUMP_TABLE_PROGRESS}); a half-deleted request
   *       resurrects as pending through the {@code LEFT JOIN} in {@code loadPending}.
   * </ul>
   *
   * <p>Pruning never crosses scopes: completing {@code TABLE T} does not delete prior
   * {@code ALL_TABLES} requests, and completing {@code ALL_TABLES} does not delete prior
   * single-table requests. Operators inspecting historical activity through
   * {@code GET /api/v1/requests} will only see entries reclaimed by a same-scope, same-table newer
   * completion.
   */
  void pruneSupersededTerminalRequests(DumpRequest justCompletedRequest);
}
