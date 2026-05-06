package io.github.aandreakis.dblog.state.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.core.request.DumpRequestState;
import io.github.aandreakis.dblog.core.request.DumpRequestStatus;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.request.DumpTableProgress;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Direct coverage for {@link JdbcDumpRequestRepository}: unique-key protection and the
 * compare-and-swap primitive used by the coordinator to own request-state transitions.
 */
class JdbcDumpRequestRepositoryTests {
  @TempDir Path tempDir;

  private DumpRequest request(String requestId) {
    return new DumpRequest(requestId, DumpScope.TABLE, new TableId("source", "appdb", "widgets"), List.of());
  }

  private DumpRequest tableRequest(String requestId, TableId tableId) {
    return new DumpRequest(requestId, DumpScope.TABLE, tableId, List.of());
  }

  private DumpRequest primaryKeysRequest(String requestId, TableId tableId, List<String> literals) {
    java.util.List<String> columnNames = java.util.List.of("id");
    java.util.List<io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple> tuples =
        literals.stream()
            .map(
                literal ->
                    io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple.fromValues(
                        columnNames,
                        java.util.List.of(
                            io.github.aandreakis.dblog.core.schema.PrimaryKeyValue
                                .fromLiteral(
                                    io.github.aandreakis.dblog.core.schema
                                        .NeutralColumnType.INTEGER,
                                    literal))))
            .toList();
    return new DumpRequest(requestId, DumpScope.PRIMARY_KEYS, tableId, tuples);
  }

  private DumpRequest allTablesRequest(String requestId) {
    return new DumpRequest(requestId, DumpScope.ALL_TABLES, null, List.of());
  }

  private static void completeAndPrune(H2RuntimeStateStore store, DumpRequest request) {
    store.dumpRequests().saveStatus(DumpRequestStatus.completed(request, List.of()));
    store.dumpRequests().pruneSupersededTerminalRequests(request);
  }

  private static void failNonPrune(H2RuntimeStateStore store, DumpRequest request, String reason) {
    store.dumpRequests().saveStatus(DumpRequestStatus.failed(request, reason));
  }

  /**
   * Two back-to-back {@code upsert(request)} calls with the same requestId must not produce two
   * rows. The second call updates the existing row. {@code DUMP_REQUEST_REQUEST_ID_UQ} protects
   * against any regression that would insert a duplicate.
   */
  @Test
  void duplicateUpsertDoesNotInsertSecondRow() {
    try (H2RuntimeStateStore store = new H2RuntimeStateStore(tempDir.resolve("unique-key"))) {
      DumpRequest req = request("dup-1");
      store.dumpRequests().upsert(req);
      store.dumpRequests().upsert(req);
      assertThat(store.dumpRequests().countPending()).isEqualTo(1);
    }
  }

  /**
   * {@code saveStatusIfCurrentStateIn} is the compare-and-swap primitive the coordinator uses to
   * move a request between {@code ACTIVE}, {@code COMPLETED}, and {@code FAILED}. When the
   * current state matches an allowed state, the write lands; when it doesn't, the call returns
   * false without mutating state.
   */
  @Test
  void saveStatusIfCurrentStateInHonorsAllowedStates() {
    try (H2RuntimeStateStore store = new H2RuntimeStateStore(tempDir.resolve("cas-primitive"))) {
      DumpRequest req = request("cas-primitive-1");
      store.dumpRequests().upsert(req);

      // No status yet; allowMissing=true must accept.
      boolean applied1 =
          store.dumpRequests().saveStatusIfCurrentStateIn(DumpRequestStatus.active(req), true);
      assertThat(applied1).isTrue();

      // Attempting to write ACTIVE when state is ACTIVE with allowedCurrentStates=[COMPLETED]
      // must reject: the current state is not in the allowed set.
      boolean applied2 =
          store.dumpRequests()
              .saveStatusIfCurrentStateIn(
                  DumpRequestStatus.active(req), false, DumpRequestState.COMPLETED);
      assertThat(applied2).isFalse();

      // With allowedCurrentStates=[ACTIVE], the write lands (identity update).
      boolean applied3 =
          store.dumpRequests()
              .saveStatusIfCurrentStateIn(
                  DumpRequestStatus.active(req), false, DumpRequestState.ACTIVE);
      assertThat(applied3).isTrue();
    }
  }

  /**
   * A newer COMPLETED TABLE request supersedes prior terminal-state TABLE/PRIMARY_KEYS requests
   * that target the same {@code (databaseName, schemaName, tableName)}. After pruning the
   * historic entries are gone from {@code DUMP_REQUEST_STATUS}, {@code DUMP_REQUEST}, and the
   * dependent key tables, while the just-completed request itself is preserved.
   */
  @Test
  void pruneSupersededTerminalRequestsDeletesPriorSameTableTerminalEntries() {
    try (H2RuntimeStateStore store = new H2RuntimeStateStore(tempDir.resolve("prune-same-table"))) {
      TableId widgets = new TableId("source", "appdb", "widgets");

      DumpRequest old1 = tableRequest("old-1", widgets);
      DumpRequest old2 = primaryKeysRequest("old-2", widgets, List.of("42", "43"));
      DumpRequest current = tableRequest("current", widgets);

      store.dumpRequests().upsert(old1);
      store.dumpRequests().upsert(old2);
      store.dumpRequests().upsert(current);
      failNonPrune(store, old1, "boom");
      store.dumpRequests().saveStatus(DumpRequestStatus.completed(old2, List.of()));

      completeAndPrune(store, current);

      assertThat(store.dumpRequests().loadStatus("old-1")).isEmpty();
      assertThat(store.dumpRequests().loadRequest("old-1")).isEmpty();
      assertThat(store.dumpRequests().loadStatus("old-2")).isEmpty();
      assertThat(store.dumpRequests().loadRequest("old-2")).isEmpty();
      assertThat(store.dumpRequests().loadRequest("current")).isPresent();
      assertThat(store.dumpRequests().loadStatus("current"))
          .map(DumpRequestStatus::state)
          .contains(DumpRequestState.COMPLETED);
    }
  }

  /**
   * Pruning is scoped: a TABLE completion on one table must not delete entries that target a
   * different table or that use {@code ALL_TABLES} scope. Operators rely on this to keep
   * unrelated history visible through {@code GET /api/v1/requests}.
   */
  @Test
  void pruneSupersededTerminalRequestsLeavesOtherTablesAndAllTablesAlone() {
    try (H2RuntimeStateStore store = new H2RuntimeStateStore(tempDir.resolve("prune-scope"))) {
      TableId widgets = new TableId("source", "appdb", "widgets");
      TableId gadgets = new TableId("source", "appdb", "gadgets");

      DumpRequest otherTable = tableRequest("other-table", gadgets);
      DumpRequest allTablesOld = allTablesRequest("all-old");
      DumpRequest current = tableRequest("current", widgets);

      store.dumpRequests().upsert(otherTable);
      store.dumpRequests().upsert(allTablesOld);
      store.dumpRequests().upsert(current);
      store.dumpRequests().saveStatus(DumpRequestStatus.completed(otherTable, List.of()));
      failNonPrune(store, allTablesOld, "boom");

      completeAndPrune(store, current);

      assertThat(store.dumpRequests().loadRequest("other-table")).isPresent();
      assertThat(store.dumpRequests().loadRequest("all-old")).isPresent();
      assertThat(store.dumpRequests().loadRequest("current")).isPresent();
    }
  }

  /**
   * A newer COMPLETED ALL_TABLES request supersedes prior terminal-state ALL_TABLES requests
   * regardless of which tables they implicitly covered, but never crosses scope into
   * single-table requests.
   */
  @Test
  void pruneSupersededTerminalRequestsForAllTablesDeletesPriorAllTablesOnly() {
    try (H2RuntimeStateStore store = new H2RuntimeStateStore(tempDir.resolve("prune-all-tables"))) {
      TableId widgets = new TableId("source", "appdb", "widgets");

      DumpRequest oldAllTables1 = allTablesRequest("all-1");
      DumpRequest oldAllTables2 = allTablesRequest("all-2");
      DumpRequest tableRequest = tableRequest("table-historic", widgets);
      DumpRequest current = allTablesRequest("all-current");

      store.dumpRequests().upsert(oldAllTables1);
      store.dumpRequests().upsert(oldAllTables2);
      store.dumpRequests().upsert(tableRequest);
      store.dumpRequests().upsert(current);
      store.dumpRequests().saveStatus(DumpRequestStatus.completed(oldAllTables1, List.of()));
      failNonPrune(store, oldAllTables2, "earlier failure");
      store.dumpRequests().saveStatus(DumpRequestStatus.completed(tableRequest, List.of()));

      completeAndPrune(store, current);

      assertThat(store.dumpRequests().loadRequest("all-1")).isEmpty();
      assertThat(store.dumpRequests().loadRequest("all-2")).isEmpty();
      assertThat(store.dumpRequests().loadRequest("table-historic"))
          .as("ALL_TABLES completion must not reach across scope into TABLE requests")
          .isPresent();
      assertThat(store.dumpRequests().loadRequest("all-current")).isPresent();
    }
  }

  /**
   * Pruning never touches non-terminal-state requests. An ACTIVE request with a lower
   * REQUEST_SEQUENCE on the same table must survive a newer completion — it represents work
   * still in flight, and resurrecting it as pending would be silently correct but losing the
   * row would orphan in-progress chunk progress.
   */
  @Test
  void pruneSupersededTerminalRequestsLeavesActiveRequestsAlone() {
    try (H2RuntimeStateStore store = new H2RuntimeStateStore(tempDir.resolve("prune-active"))) {
      TableId widgets = new TableId("source", "appdb", "widgets");

      DumpRequest activeOlder = tableRequest("active-older", widgets);
      DumpRequest current = tableRequest("current", widgets);

      store.dumpRequests().upsert(activeOlder);
      store.dumpRequests().upsert(current);
      store.dumpRequests().saveStatus(DumpRequestStatus.active(activeOlder));

      completeAndPrune(store, current);

      assertThat(store.dumpRequests().loadStatus("active-older"))
          .map(DumpRequestStatus::state)
          .contains(DumpRequestState.ACTIVE);
      assertThat(store.dumpRequests().loadRequest("active-older")).isPresent();
    }
  }

  /**
   * Pruning a request with PRIMARY_KEYS scope removes its dependent key rows. The just-completed
   * request keeps its own key rows. This is the safety property that prevents
   * {@code loadPending}'s {@code LEFT JOIN} from resurrecting a partially-deleted request: every
   * dependent table of a pruned request must be cleaned up in the same transaction.
   */
  @Test
  void pruneSupersededTerminalRequestsDeletesDependentKeyRows() {
    try (H2RuntimeStateStore store = new H2RuntimeStateStore(tempDir.resolve("prune-keys"))) {
      TableId widgets = new TableId("source", "appdb", "widgets");

      DumpRequest oldPk = primaryKeysRequest("old-pk", widgets, List.of("1", "2", "3"));
      DumpRequest currentPk = primaryKeysRequest("current-pk", widgets, List.of("9"));

      store.dumpRequests().upsert(oldPk);
      store.dumpRequests().upsert(currentPk);
      failNonPrune(store, oldPk, "boom");

      completeAndPrune(store, currentPk);

      // After prune, old-pk and its key rows are gone; the LEFT JOIN can no longer resurrect
      // it as pending because the parent DUMP_REQUEST row is also deleted.
      assertThat(store.dumpRequests().countPending()).isEqualTo(0);
      assertThat(store.dumpRequests().loadRequest("old-pk")).isEmpty();
      assertThat(store.dumpRequests().loadRequest("current-pk"))
          .map(DumpRequest::primaryKeyTuples)
          .map(java.util.List::size)
          .contains(1);
    }
  }

  /**
   * The dependent {@code DUMP_TABLE_PROGRESS} rows that survived the request lifecycle (e.g. a
   * dump that failed before the per-table chunk loop pruned its own progress row) must be
   * cleaned up when the parent request is pruned. Without this, the request lineage is only
   * half-deleted and the on-disk state-store keeps growing even though the operator-visible
   * request list looks correctly pruned.
   */
  @Test
  void pruneSupersededTerminalRequestsDeletesDumpTableProgressRowsForPrunedRequests() {
    try (H2RuntimeStateStore store =
        new H2RuntimeStateStore(tempDir.resolve("prune-table-progress"))) {
      TableId widgets = new TableId("source", "appdb", "widgets");
      String tableDisplayName = widgets.displayName();

      DumpRequest oldRequest = tableRequest("old-with-progress", widgets);
      DumpRequest current = tableRequest("current", widgets);

      store.dumpRequests().upsert(oldRequest);
      store.dumpRequests().upsert(current);
      // Seed a progress row for the soon-to-be-pruned request and one for the just-completed
      // request. The pruned-request row must disappear; the just-completed row must survive
      // because pruning never touches the row it was triggered by.
      store
          .dumpProgress()
          .save(
              DumpTableProgress.initial(oldRequest.requestId(), tableDisplayName, "fp-old"));
      store
          .dumpProgress()
          .save(
              DumpTableProgress.initial(current.requestId(), tableDisplayName, "fp-current"));
      failNonPrune(store, oldRequest, "boom");

      assertThat(store.dumpProgress().load(oldRequest.requestId(), tableDisplayName))
          .as("precondition: pruned request must have a progress row to start with")
          .isPresent();

      completeAndPrune(store, current);

      assertThat(store.dumpProgress().load(oldRequest.requestId(), tableDisplayName))
          .as("DUMP_TABLE_PROGRESS row for the pruned request must be deleted")
          .isEmpty();
      assertThat(store.dumpProgress().load(current.requestId(), tableDisplayName))
          .as("DUMP_TABLE_PROGRESS row for the just-completed request must be preserved")
          .isPresent();
    }
  }

  /**
   * Calling prune for a request that was already deleted (e.g. concurrent operator clean-up)
   * is a no-op rather than an error. The coordinator runs prune unconditionally after every
   * COMPLETED transition, so robustness against a missing parent row matters.
   */
  @Test
  void pruneSupersededTerminalRequestsIsNoopWhenJustCompletedRequestIsAbsent() {
    try (H2RuntimeStateStore store = new H2RuntimeStateStore(tempDir.resolve("prune-missing"))) {
      DumpRequest ghost = tableRequest("ghost", new TableId("source", "appdb", "widgets"));
      // Note: no upsert. The request id is unknown to the state store.
      store.dumpRequests().pruneSupersededTerminalRequests(ghost);
      assertThat(store.dumpRequests().countPending()).isEqualTo(0);
    }
  }
}
