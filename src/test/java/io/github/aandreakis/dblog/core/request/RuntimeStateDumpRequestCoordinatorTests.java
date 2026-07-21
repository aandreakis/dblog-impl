package io.github.aandreakis.dblog.core.request;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.reconcile.WatermarkWindow;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import io.github.aandreakis.dblog.testsupport.ChangeEventTestFixtures;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RuntimeStateDumpRequestCoordinatorTests {
  @TempDir Path tempDir;

  @Test
  void coordinatesTableRequestAndMarksItCompletedOnAcknowledge() throws Exception {
    TableSchema schema = schema();
    DumpRequest request = new DumpRequest("42", DumpScope.TABLE, schema.tableId(), List.of());
    TestTransaction transaction = new TestTransaction("tx-1", new ComparablePosition(1), List.of());
    DumpWindowBatch<TestTransaction> batch =
        new DumpWindowBatch<>(
            DumpTableProgress.initial(request.requestId(), schema.tableId().displayName(), schema.fingerprint())
                .captureRequestUpperBound(schema, "1")
                .beginChunk(
                    Chunk.fromMapRows(
                        request.requestId(),
                        schema.tableId().displayName(),
                        schema,
                        null,
                        List.of(Map.of("id", "1", "name", "a")),
                        "1",
                        true),
                    new WatermarkWindow(
                        new io.github.aandreakis.dblog.core.model.WatermarkToken("lw"),
                        new io.github.aandreakis.dblog.core.model.WatermarkToken("hw"))),
            Chunk.fromMapRows(
                request.requestId(),
                schema.tableId().displayName(),
                schema,
                null,
                List.of(Map.of("id", "1", "name", "a")),
                "1",
                true),
            new WatermarkWindow(
                new io.github.aandreakis.dblog.core.model.WatermarkToken("lw"),
                new io.github.aandreakis.dblog.core.model.WatermarkToken("hw")),
            List.of(
                ChangeEventTestFixtures.fromRowMaps(
                    schema.tableId(),
                    OperationType.UPDATE,
                    CaptureOrigin.SELECT,
                    Map.of("id", "1"),
                    null,
                    Map.of("id", "1", "name", "a"),
                    new OpaqueSourcePosition("snapshot:1"),
                    null,
                    request.requestId())),
            transaction);

    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(tempDir.resolve("coordinator-state"))) {
      stateStore.dumpRequests().upsert(request);
      RuntimeStateDumpRequestCoordinator<TestTransaction> coordinator =
          new RuntimeStateDumpRequestCoordinator<>(
              "MySQL",
              "sourceA",
              stateStore.dumpRequests(),
              stateStore.schemas(),
              new FakeDumpWindowCoordinator(batch),
              new FakeTargetedRepairCoordinator(),
              () -> List.of(schema),
              100,
              io.github.aandreakis.dblog.tap.NoopTap.INSTANCE);

      ScheduledRequestBatch<TestTransaction> scheduled = coordinator.coordinateNextBatch().orElseThrow();
      coordinator.acknowledgeCompletedBatch(scheduled);

      assertThat(scheduled.request().requestId()).isEqualTo("42");
      assertThat(stateStore.dumpRequests().loadStatus("42")).isPresent();
      assertThat(stateStore.dumpRequests().loadStatus("42").orElseThrow().state())
          .isEqualTo(DumpRequestState.COMPLETED);
    }
  }

  @Test
  void coordinatesPrimaryKeyRepairWithoutPersistingActiveWindowState() throws Exception {
    TableSchema schema = schema();
    DumpRequest request =
        DumpRequest.fromPrimaryKeyLiterals(
            "repair-42", DumpScope.PRIMARY_KEYS, schema.tableId(), schema, List.of("1", "2"));
    TargetedRepairBatch<TestTransaction> batch =
        new TargetedRepairBatch<>(
            request,
            Chunk.fromMapRows(
                request.requestId(),
                schema.tableId().displayName(),
                schema,
                null,
                List.of(Map.of("id", "1", "name", "a")),
                "1",
                true),
            new WatermarkWindow(
                new io.github.aandreakis.dblog.core.model.WatermarkToken("lw"),
                new io.github.aandreakis.dblog.core.model.WatermarkToken("hw")),
            schema.primaryKeyTuplesFromLiterals(List.of("2")),
            List.of(
                ChangeEventTestFixtures.fromRowMaps(
                    schema.tableId(),
                    OperationType.UPDATE,
                    CaptureOrigin.SELECT,
                    Map.of("id", "1"),
                    null,
                    Map.of("id", "1", "name", "a"),
                    new OpaqueSourcePosition("snapshot:1"),
                    null,
                    request.requestId())),
            new TestTransaction("tx-1", new ComparablePosition(1), List.of()));

    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(tempDir.resolve("coordinator-targeted-state"))) {
      stateStore.dumpRequests().upsert(request);
      RuntimeStateDumpRequestCoordinator<TestTransaction> coordinator =
          new RuntimeStateDumpRequestCoordinator<>(
              "MySQL",
              "sourceA",
              stateStore.dumpRequests(),
              stateStore.schemas(),
              new FakeDumpWindowCoordinator(null),
              new FakeTargetedRepairCoordinator(batch),
              () -> List.of(schema),
              100,
              io.github.aandreakis.dblog.tap.NoopTap.INSTANCE);

      ScheduledRequestBatch<TestTransaction> scheduled = coordinator.coordinateNextBatch().orElseThrow();

      assertThat(scheduled.request().scope()).isEqualTo(DumpScope.PRIMARY_KEYS);
      DumpRequestStatus status = stateStore.dumpRequests().loadStatus("repair-42").orElseThrow();
      assertThat(status.state()).isEqualTo(DumpRequestState.ACTIVE);

      coordinator.acknowledgeCompletedBatch(scheduled);

      DumpRequestStatus completed = stateStore.dumpRequests().loadStatus("repair-42").orElseThrow();
      assertThat(completed.state()).isEqualTo(DumpRequestState.COMPLETED);
      assertThat(completed.missingPrimaryKeyLiterals()).containsExactly("2");
    }
  }

  @Test
  void coordinatesDrainOnlyPrimaryKeyRepairAndPersistsMissingKeysOnAcknowledge()
      throws Exception {
    TableSchema schema = schema();
    DumpRequest request =
        DumpRequest.fromPrimaryKeyLiterals(
            "repair-missing", DumpScope.PRIMARY_KEYS, schema.tableId(), schema, List.of("9"));
    TargetedRepairDrainBatch<TestTransaction> drain =
        new TargetedRepairDrainBatch<>(
            request,
            new WatermarkWindow(
                new io.github.aandreakis.dblog.core.model.WatermarkToken("lw"),
                new io.github.aandreakis.dblog.core.model.WatermarkToken("hw")),
            schema.primaryKeyTuplesFromLiterals(List.of("9")),
            List.of(),
            new TestTransaction("tx-hw", new ComparablePosition(1), List.of()));

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("coordinator-targeted-drain-state"))) {
      stateStore.dumpRequests().upsert(request);
      RuntimeStateDumpRequestCoordinator<TestTransaction> coordinator =
          new RuntimeStateDumpRequestCoordinator<>(
              "MySQL",
              "sourceA",
              stateStore.dumpRequests(),
              stateStore.schemas(),
              new FakeDumpWindowCoordinator(null),
              new FakeTargetedRepairCoordinator(drain),
              () -> List.of(schema),
              100,
              io.github.aandreakis.dblog.tap.NoopTap.INSTANCE);

      ScheduledRequestBatch<TestTransaction> scheduled = coordinator.coordinateNextBatch().orElseThrow();

      assertThat(scheduled.request().scope()).isEqualTo(DumpScope.PRIMARY_KEYS);
      assertThat(scheduled.missingPrimaryKeyLiterals()).containsExactly("9");
      assertThat(stateStore.dumpRequests().loadStatus("repair-missing").orElseThrow().state())
          .isEqualTo(DumpRequestState.ACTIVE);

      coordinator.acknowledgeCompletedBatch(scheduled);

      DumpRequestStatus completed =
          stateStore.dumpRequests().loadStatus("repair-missing").orElseThrow();
      assertThat(completed.state()).isEqualTo(DumpRequestState.COMPLETED);
      assertThat(completed.missingPrimaryKeyLiterals()).containsExactly("9");
    }
  }

  /**
   * Locks the coordinator's PK-type rejection gate: a TABLE request whose captured schema carries
   * an unsupported primary-key type must transition to FAILED and write a
   * FULL_DUMP_REQUIRED_SIGNAL keyed by sourceId, without ever entering the dump-window
   * coordinator.
   */
  @Test
  void failsTableRequestClosedWhenPrimaryKeyTypeIsUnsupported() throws Exception {
    TableSchema unsupportedSchema =
        TableSchema.create(
            new TableId("source", "appdb", "docs"),
            List.of(
                new ColumnDefinition("body", "json", NeutralColumnType.JSON, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-10T00:00:00Z"));
    DumpRequest request =
        new DumpRequest("pk-unsupported-1", DumpScope.TABLE, unsupportedSchema.tableId(), List.of());

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("coordinator-pk-unsupported"))) {
      stateStore.dumpRequests().upsert(request);
      FakeDumpWindowCoordinator windowCoordinator = new FakeDumpWindowCoordinator(null);
      RuntimeStateDumpRequestCoordinator<TestTransaction> coordinator =
          new RuntimeStateDumpRequestCoordinator<>(
              "MySQL",
              "sourceA",
              stateStore.dumpRequests(),
              stateStore.schemas(),
              windowCoordinator,
              new FakeTargetedRepairCoordinator(),
              () -> List.of(unsupportedSchema),
              100,
              io.github.aandreakis.dblog.tap.NoopTap.INSTANCE);

      assertThat(coordinator.coordinateNextBatch()).isEmpty();

      DumpRequestStatus status =
          stateStore.dumpRequests().loadStatus("pk-unsupported-1").orElseThrow();
      assertThat(status.state()).isEqualTo(DumpRequestState.FAILED);
      // The window coordinator must never have been reached for an unsupported-PK table.
      assertThat(windowCoordinator.acknowledgeCallCount).isEqualTo(0);
      // Full-dump-required signal is recorded, keyed by the configured sourceId.
      assertThat(
              stateStore.schemas().loadFullDumpRequiredSignals().stream()
                  .anyMatch(signal -> "sourceA".equals(signal.sourceId())))
          .as("FullDumpRequiredSignal must be written for sourceA")
          .isTrue();
    }
  }

  @Test
  void failsTableRequestClosedWhenTimetzPrimaryKeyCannotPreserveIdentity() throws Exception {
    TableSchema timetzPrimaryKeySchema =
        TableSchema.create(
            new TableId("source", "public", "timed_widgets"),
            List.of(
                new ColumnDefinition(
                    "observed_at", "time with time zone", NeutralColumnType.TIME, true, false),
                new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-07-21T00:00:00Z"));
    DumpRequest request =
        new DumpRequest(
            "pk-timetz-unsupported",
            DumpScope.TABLE,
            timetzPrimaryKeySchema.tableId(),
            List.of());

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("coordinator-pk-timetz-unsupported"))) {
      stateStore.dumpRequests().upsert(request);
      RuntimeStateDumpRequestCoordinator<TestTransaction> coordinator =
          new RuntimeStateDumpRequestCoordinator<>(
              "PostgreSQL",
              "sourceA",
              stateStore.dumpRequests(),
              stateStore.schemas(),
              new FakeDumpWindowCoordinator(null),
              new FakeTargetedRepairCoordinator(),
              () -> List.of(timetzPrimaryKeySchema),
              100,
              io.github.aandreakis.dblog.tap.NoopTap.INSTANCE);

      assertThat(coordinator.coordinateNextBatch()).isEmpty();

      DumpRequestStatus status =
          stateStore.dumpRequests().loadStatus("pk-timetz-unsupported").orElseThrow();
      assertThat(status.state()).isEqualTo(DumpRequestState.FAILED);
      assertThat(stateStore.schemas().loadFullDumpRequiredSignals())
          .anySatisfy(
              signal -> {
                assertThat(signal.sourceId()).isEqualTo("sourceA");
                assertThat(signal.tableId()).isEqualTo(timetzPrimaryKeySchema.tableId());
              });
    }
  }

  @Test
  void failsPrimaryKeysRequestClosedWhenTimetzPrimaryKeyCannotPreserveIdentity()
      throws Exception {
    TableSchema timetzPrimaryKeySchema =
        TableSchema.create(
            new TableId("source", "public", "timed_widgets"),
            List.of(
                new ColumnDefinition(
                    "observed_at", "time with time zone", NeutralColumnType.TIME, true, false),
                new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-07-21T00:00:00Z"));
    DumpRequest request =
        DumpRequest.fromPrimaryKeyLiterals(
            "pk-repair-timetz-unsupported",
            DumpScope.PRIMARY_KEYS,
            timetzPrimaryKeySchema.tableId(),
            timetzPrimaryKeySchema,
            List.of("10:00:00+02"));

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("coordinator-pk-repair-timetz-unsupported"))) {
      stateStore.dumpRequests().upsert(request);
      FakeTargetedRepairCoordinator targetedRepairCoordinator =
          new FakeTargetedRepairCoordinator();
      RuntimeStateDumpRequestCoordinator<TestTransaction> coordinator =
          new RuntimeStateDumpRequestCoordinator<>(
              "PostgreSQL",
              "sourceA",
              stateStore.dumpRequests(),
              stateStore.schemas(),
              new FakeDumpWindowCoordinator(null),
              targetedRepairCoordinator,
              () -> List.of(timetzPrimaryKeySchema),
              100,
              io.github.aandreakis.dblog.tap.NoopTap.INSTANCE);

      assertThat(coordinator.coordinateNextBatch()).isEmpty();

      DumpRequestStatus status =
          stateStore.dumpRequests().loadStatus(request.requestId()).orElseThrow();
      assertThat(status.state()).isEqualTo(DumpRequestState.FAILED);
      assertThat(targetedRepairCoordinator.coordinateCallCount).isZero();
      assertThat(stateStore.schemas().loadFullDumpRequiredSignals())
          .anySatisfy(
              signal -> {
                assertThat(signal.sourceId()).isEqualTo("sourceA");
                assertThat(signal.tableId()).isEqualTo(timetzPrimaryKeySchema.tableId());
              });
    }
  }

  /**
   * Same contract for ALL_TABLES: if any captured table has an unsupported PK type, the whole
   * ALL_TABLES request fails closed.
   */
  @Test
  void failsAllTablesRequestClosedWhenAnyTableHasUnsupportedPrimaryKeyType() throws Exception {
    TableSchema supportedSchema = schema();
    TableSchema unsupportedSchema =
        TableSchema.create(
            new TableId("source", "appdb", "docs"),
            List.of(
                new ColumnDefinition("body", "json", NeutralColumnType.JSON, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-10T00:00:00Z"));
    DumpRequest request = new DumpRequest("pk-unsupported-all", DumpScope.ALL_TABLES, null, List.of());

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("coordinator-pk-unsupported-all"))) {
      stateStore.dumpRequests().upsert(request);
      FakeDumpWindowCoordinator windowCoordinator = new FakeDumpWindowCoordinator(null);
      RuntimeStateDumpRequestCoordinator<TestTransaction> coordinator =
          new RuntimeStateDumpRequestCoordinator<>(
              "MySQL",
              "sourceA",
              stateStore.dumpRequests(),
              stateStore.schemas(),
              windowCoordinator,
              new FakeTargetedRepairCoordinator(),
              () -> List.of(supportedSchema, unsupportedSchema),
              100,
              io.github.aandreakis.dblog.tap.NoopTap.INSTANCE);

      assertThat(coordinator.coordinateNextBatch()).isEmpty();

      DumpRequestStatus status =
          stateStore.dumpRequests().loadStatus("pk-unsupported-all").orElseThrow();
      assertThat(status.state()).isEqualTo(DumpRequestState.FAILED);
    }
  }

  // Neutral types such as JSON are gated earlier because literal parsing rejects them. A
  // source-specific lossy shape such as TIMETZ still parses as neutral TIME, so the coordinator's
  // shared contract gate rejects it for TABLE, PRIMARY_KEYS, and ALL_TABLES requests.

  /**
   * The coordinator's COMPLETED transition prunes prior terminal-state requests for the same
   * scope/table. The repository-level test pins the prune semantics; this coordinator test pins
   * that {@code saveRuntimeOwnedStatus} actually invokes them on a successful transition. A
   * prior FAILED request on the same table must disappear once a new TABLE request completes;
   * an unrelated table's history must remain.
   */
  @Test
  void completingTableRequestPrunesPriorTerminalStateForSameTable() throws Exception {
    TableSchema schema = schema();
    DumpRequest priorFailed =
        new DumpRequest("prior-failed", DumpScope.TABLE, schema.tableId(), List.of());
    DumpRequest unrelated =
        new DumpRequest(
            "unrelated",
            DumpScope.TABLE,
            new TableId("source", "appdb", "gadgets"),
            List.of());
    DumpRequest current = new DumpRequest("current", DumpScope.TABLE, schema.tableId(), List.of());
    TestTransaction transaction = new TestTransaction("tx-1", new ComparablePosition(1), List.of());
    DumpWindowBatch<TestTransaction> batch =
        new DumpWindowBatch<>(
            DumpTableProgress.initial(current.requestId(), schema.tableId().displayName(), schema.fingerprint())
                .captureRequestUpperBound(schema, "1")
                .beginChunk(
                    Chunk.fromMapRows(
                        current.requestId(),
                        schema.tableId().displayName(),
                        schema,
                        null,
                        List.of(Map.of("id", "1", "name", "a")),
                        "1",
                        true),
                    new WatermarkWindow(
                        new io.github.aandreakis.dblog.core.model.WatermarkToken("lw"),
                        new io.github.aandreakis.dblog.core.model.WatermarkToken("hw"))),
            Chunk.fromMapRows(
                current.requestId(),
                schema.tableId().displayName(),
                schema,
                null,
                List.of(Map.of("id", "1", "name", "a")),
                "1",
                true),
            new WatermarkWindow(
                new io.github.aandreakis.dblog.core.model.WatermarkToken("lw"),
                new io.github.aandreakis.dblog.core.model.WatermarkToken("hw")),
            List.of(
                ChangeEventTestFixtures.fromRowMaps(
                    schema.tableId(),
                    OperationType.UPDATE,
                    CaptureOrigin.SELECT,
                    Map.of("id", "1"),
                    null,
                    Map.of("id", "1", "name", "a"),
                    new OpaqueSourcePosition("snapshot:1"),
                    null,
                    current.requestId())),
            transaction);

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("coordinator-prune"))) {
      stateStore.dumpRequests().upsert(priorFailed);
      stateStore.dumpRequests().upsert(unrelated);
      stateStore.dumpRequests().upsert(current);
      stateStore
          .dumpRequests()
          .saveStatus(DumpRequestStatus.failed(priorFailed, "earlier failure"));
      stateStore.dumpRequests().saveStatus(DumpRequestStatus.completed(unrelated, List.of()));

      RuntimeStateDumpRequestCoordinator<TestTransaction> coordinator =
          new RuntimeStateDumpRequestCoordinator<>(
              "MySQL",
              "sourceA",
              stateStore.dumpRequests(),
              stateStore.schemas(),
              new FakeDumpWindowCoordinator(batch),
              new FakeTargetedRepairCoordinator(),
              () -> List.of(schema),
              100,
              io.github.aandreakis.dblog.tap.NoopTap.INSTANCE);

      ScheduledRequestBatch<TestTransaction> scheduled =
          coordinator.coordinateNextBatch().orElseThrow();
      coordinator.acknowledgeCompletedBatch(scheduled);

      assertThat(stateStore.dumpRequests().loadStatus("current").orElseThrow().state())
          .isEqualTo(DumpRequestState.COMPLETED);
      assertThat(stateStore.dumpRequests().loadRequest("prior-failed"))
          .as("prior FAILED entry on same table must be pruned by completing 'current'")
          .isEmpty();
      assertThat(stateStore.dumpRequests().loadRequest("unrelated"))
          .as("history for a different table must not be pruned")
          .isPresent();
    }
  }

  private static TableSchema schema() {
    return TableSchema.create(
        new TableId("source", "appdb", "widgets"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
        Instant.parse("2026-04-10T00:00:00Z"));
  }

  private record TestTransaction(
      String transactionId, ComparablePosition checkpointPosition, List<ChangeEvent> events)
      implements io.github.aandreakis.dblog.adapter.api.SourceTransaction<ComparablePosition> {
    @Override
    public Instant commitTimestamp() {
      return Instant.parse("2026-03-25T00:00:00Z");
    }
  }

  private record ComparablePosition(int ordinal)
      implements SourcePosition, Comparable<ComparablePosition> {
    @Override
    public String displayValue() {
      return "checkpoint:" + ordinal;
    }

    @Override
    public int compareTo(ComparablePosition other) {
      return Integer.compare(ordinal, other.ordinal);
    }
  }

  private static final class FakeDumpWindowCoordinator
      implements DumpWindowCoordinator<TestTransaction> {
    private final DumpWindowOutcome<TestTransaction> outcome;
    int acknowledgeCallCount;

    private FakeDumpWindowCoordinator(DumpWindowOutcome<TestTransaction> outcome) {
      this.outcome = outcome;
    }

    @Override
    public Optional<DumpWindowOutcome<TestTransaction>> coordinateNextTableChunk(
        String jobId, TableSchema schema, int chunkSize) {
      return Optional.ofNullable(outcome);
    }

    @Override
    public boolean hasRemainingTableWork(String jobId, TableSchema schema, int chunkSize) {
      return true;
    }

    @Override
    public void acknowledgeCompletedBatch(DumpWindowOutcome<TestTransaction> outcome) {
      acknowledgeCallCount++;
    }
  }

  private static final class FakeTargetedRepairCoordinator
      implements TargetedRepairCoordinator<TestTransaction> {
    private final TargetedRepairOutcome<TestTransaction> outcome;
    int acknowledgeCallCount;
    int coordinateCallCount;

    private FakeTargetedRepairCoordinator() {
      this(null);
    }

    private FakeTargetedRepairCoordinator(TargetedRepairOutcome<TestTransaction> outcome) {
      this.outcome = outcome;
    }

    @Override
    public TargetedRepairResult<TestTransaction> coordinate(DumpRequest request, TableSchema schema) {
      coordinateCallCount++;
      return new TargetedRepairResult<>(
          Optional.ofNullable(outcome),
          outcome == null ? List.of() : outcome.missingPrimaryKeyTuples());
    }

    @Override
    public void acknowledge(TargetedRepairOutcome<TestTransaction> outcome) {
      acknowledgeCallCount++;
    }
  }
}
