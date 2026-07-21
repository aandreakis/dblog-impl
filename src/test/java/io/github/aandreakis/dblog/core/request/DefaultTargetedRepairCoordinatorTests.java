package io.github.aandreakis.dblog.core.request;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.adapter.api.ChunkReadWithWindowWork;
import io.github.aandreakis.dblog.adapter.api.ChunkReadWork;
import io.github.aandreakis.dblog.adapter.api.ConnectionBoundChunkReader;
import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.api.WatermarkWindowResult;
import io.github.aandreakis.dblog.adapter.api.WatermarkWindowRuntime;
import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.model.WatermarkToken;
import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.reconcile.WatermarkMetadata;
import io.github.aandreakis.dblog.core.reconcile.WatermarkSequenceException;
import io.github.aandreakis.dblog.core.reconcile.WatermarkWindow;
import io.github.aandreakis.dblog.core.reconcile.WindowReconciler;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.SchemaDriftException;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.tap.NoopTap;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class DefaultTargetedRepairCoordinatorTests {
  @Test
  void deduplicatesRequestedPrimaryKeysPreservingFirstSeenOrder() throws Exception {
    // Per docs/IMPLEMENTATION.md §6.3 targeted-repair chunk selection canonicalises every
    // requested key and deduplicates while preserving first-seen order. A caller that
    // submits a key twice should see it appear once in the missing-key report and the chunk
    // reader should see it only once in its input list.
    TableSchema schema = schema();
    // Duplicate "1" and "2" interleaved with a unique "3"; canonical order should be 1, 2, 3.
    DumpRequest request =
        DumpRequest.fromPrimaryKeyLiterals(
            "repair-dedupe",
            DumpScope.PRIMARY_KEYS,
            schema.tableId(),
            schema,
            List.of("1", "2", "1", "3", "2"));
    Chunk chunk =
        Chunk.fromMapRows(
            request.requestId(),
            schema.tableId().displayName(),
            schema,
            null,
            List.of(Map.of("id", "1", "name", "a")), // only one key actually present
            "1",
            true);
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw-dedupe"), new WatermarkToken("hw-dedupe"));
    TestTransaction transaction =
        new TestTransaction(
            "tx-dedupe",
            new ComparablePosition(1),
            List.of(
                watermark(schema.tableId(), "lw-dedupe"),
                watermark(schema.tableId(), "hw-dedupe")));
    RecordingChunkReader chunkReader = new RecordingChunkReader(Optional.of(chunk));
    ExecutingFakeRuntime runtime = new ExecutingFakeRuntime(window, List.of(transaction));
    DefaultTargetedRepairCoordinator<TestTransaction> coordinator =
        new DefaultTargetedRepairCoordinator<>(
            "MySQL",
            "transaction",
            runtime,
            chunkReader,
            new WindowReconciler(NoopTap.INSTANCE),
            Duration.ofSeconds(1),
            Duration.ofMillis(1),
            NoopTap.INSTANCE);

    TargetedRepairResult<TestTransaction> result = coordinator.coordinate(request, schema);

    // Missing keys only contain each requested key once, in request order.
    assertThat(result.missingPrimaryKeys()).containsExactly("2", "3");
    // The chunk reader receives the canonical deduplicated list.
    assertThat(chunkReader.lastRequestedPrimaryKeyTuples).hasSize(3);
    assertThat(chunkReader.lastRequestedPrimaryKeyTuples.stream().map(t -> t.literal()).toList())
        .containsExactly("1", "2", "3");
  }

  @Test
  void trustsSourceMatchedRequestedKeysWhenCollationChangesReturnedLiteral() throws Exception {
    TableSchema schema =
        TableSchema.create(
            new TableId("source", "appdb", "collated_widgets"),
            List.of(
                new ColumnDefinition(
                    "id", "varchar(32)", NeutralColumnType.STRING, true, false),
                new ColumnDefinition(
                    "name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-07-21T00:00:00Z"));
    DumpRequest request =
        DumpRequest.fromPrimaryKeyLiterals(
            "repair-collated",
            DumpScope.PRIMARY_KEYS,
            schema.tableId(),
            schema,
            List.of("A"));
    var requestedKey = request.primaryKeyTuples().getFirst();
    var actualKey = schema.primaryKeyTupleFromLiteral("a");
    Chunk chunk =
        new Chunk(
            request.requestId(),
            schema.tableId().displayName(),
            schema,
            null,
            List.of(ImmutableRowImage.of(Map.of("id", "a", "name", "first"))),
            null,
            actualKey,
            true,
            List.of(requestedKey));
    WatermarkWindow window =
        new WatermarkWindow(
            new WatermarkToken("lw-collated"), new WatermarkToken("hw-collated"));
    TestTransaction transaction =
        new TestTransaction(
            "tx-collated",
            new ComparablePosition(1),
            List.of(
                watermark(schema.tableId(), "lw-collated"),
                watermark(schema.tableId(), "hw-collated")));
    RecordingChunkReader chunkReader = new RecordingChunkReader(Optional.of(chunk));
    ExecutingFakeRuntime runtime = new ExecutingFakeRuntime(window, List.of(transaction));
    DefaultTargetedRepairCoordinator<TestTransaction> coordinator =
        new DefaultTargetedRepairCoordinator<>(
            "MySQL",
            "transaction",
            runtime,
            chunkReader,
            new WindowReconciler(NoopTap.INSTANCE),
            Duration.ofSeconds(1),
            Duration.ofMillis(1),
            NoopTap.INSTANCE);

    TargetedRepairResult<TestTransaction> result = coordinator.coordinate(request, schema);

    assertThat(result.missingPrimaryKeys()).isEmpty();
    TargetedRepairBatch<TestTransaction> batch =
        (TargetedRepairBatch<TestTransaction>) result.outcome().orElseThrow();
    assertThat(batch.chunk().rows()).extracting(row -> row.get("id")).containsExactly("a");
  }

  @Test
  void failsClosedWhenTargetedRepairReadReturnsSelectedColumnSchemaDrift() throws Exception {
    TableSchema schema = schema();
    TableSchema driftedSchema =
        TableSchema.create(
            schema.tableId(),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "boolean", NeutralColumnType.BOOLEAN, false, true)),
            Instant.parse("2026-04-10T00:05:00Z"));
    DumpRequest request =
        DumpRequest.fromPrimaryKeyLiterals(
            "repair-drift", DumpScope.PRIMARY_KEYS, schema.tableId(), schema, List.of("1"));
    Chunk driftedChunk =
        Chunk.fromMapRows(
            request.requestId(),
            schema.tableId().displayName(),
            driftedSchema,
            null,
            List.of(Map.of("id", "1", "name", true)),
            "1",
            true);
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw-drift"), new WatermarkToken("hw-drift"));
    RecordingChunkReader chunkReader = new RecordingChunkReader(Optional.of(driftedChunk));
    ExecutingFakeRuntime runtime = new ExecutingFakeRuntime(window, List.of());
    DefaultTargetedRepairCoordinator<TestTransaction> coordinator =
        new DefaultTargetedRepairCoordinator<>(
            "MySQL",
            "transaction",
            runtime,
            chunkReader,
            new WindowReconciler(NoopTap.INSTANCE),
            Duration.ofSeconds(1),
            Duration.ofMillis(1),
            NoopTap.INSTANCE);

    assertThatThrownBy(() -> coordinator.coordinate(request, schema))
        .isInstanceOf(SchemaDriftException.class)
        .hasMessageContaining("selected-column contract changed")
        .hasMessageContaining("full dump required");
  }

  @Test
  void reportsAllRequestedKeysAsMissingWhenNoRowsAreFound() throws Exception {
    // When `targetedPrimaryKeyTuples` returns empty, the coordinator must:
    //   1. return a result with no batch,
    //   2. list every (canonicalised) requested key as missing,
    //   3. still drain the log until the matching high watermark arrives, so the runtime
    //      checkpoint does not stall on the coordinator's own watermark pair.
    TableSchema schema = schema();
    DumpRequest request =
        DumpRequest.fromPrimaryKeyLiterals(
            "repair-all-missing",
            DumpScope.PRIMARY_KEYS,
            schema.tableId(),
            schema,
            List.of("100", "200", "300"));
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw-miss"), new WatermarkToken("hw-miss"));
    // The drain path reconciles through LW and HW; both must appear in the streamed transactions
    // so the reconciler's strict state machine completes.
    TestTransaction lwTransaction =
        new TestTransaction(
            "tx-lw", new ComparablePosition(41), List.of(watermark(schema.tableId(), "lw-miss")));
    TestTransaction hwTransaction =
        new TestTransaction(
            "tx-hw", new ComparablePosition(42), List.of(watermark(schema.tableId(), "hw-miss")));
    RecordingChunkReader chunkReader = new RecordingChunkReader(Optional.empty());
    ExecutingFakeRuntime runtime =
        new ExecutingFakeRuntime(window, List.of(lwTransaction, hwTransaction));
    DefaultTargetedRepairCoordinator<TestTransaction> coordinator =
        new DefaultTargetedRepairCoordinator<>(
            "MySQL",
            "transaction",
            runtime,
            chunkReader,
            new WindowReconciler(NoopTap.INSTANCE),
            Duration.ofSeconds(1),
            Duration.ofMillis(1),
            NoopTap.INSTANCE);

    TargetedRepairResult<TestTransaction> result = coordinator.coordinate(request, schema);

    // Empty SELECT still opens LW/HW; the coordinator drains the window through the reconciler
    // and returns a drain outcome so the caller can advance the source checkpoint. The drain
    // outcome carries zero emitted events here (no log traffic in this test's queue).
    TargetedRepairDrainBatch<TestTransaction> drain =
        (TargetedRepairDrainBatch<TestTransaction>) result.outcome().orElseThrow();
    assertThat(drain.emittedEvents()).isEmpty();
    assertThat(drain.missingPrimaryKeys()).containsExactly("100", "200", "300");
    assertThat(result.missingPrimaryKeys()).containsExactly("100", "200", "300");
    // Verify the coordinator drained through the HW transaction; the stub should have no
    // more transactions available.
    assertThat(runtime.remainingTransactions()).isZero();
  }

  @Test
  void emptyChunkDrainMustNotSilentlyEatLiveTransactions() throws Exception {
    // Bug repro, targeted-repair variant. When the watermark-scoped targeted-key read returns
    // an empty chunk, the coordinator opens LW/HW and then "drains to HW" by repeatedly calling
    // readPendingTransaction() and discarding everything that is not the matching HW token.
    // readPendingTransaction() is destructive, so an ordinary LOG transaction that committed
    // between LW and HW is popped and dropped without reaching the sink.
    //
    // Scenario:
    //   - Targeted repair for keys ["100","200","300"], none currently present in the table.
    //   - The queue contains [tx-live (insert of an unrelated row), tx-hw (the matching HW)].
    //   - Contract: tx-live must either be surfaced inside a returned batch, or left in the
    //     queue for the streaming pump to pick up next. Today the drain silently eats it.
    TableSchema schema = schema();
    DumpRequest request =
        DumpRequest.fromPrimaryKeyLiterals(
            "repair-empty-live",
            DumpScope.PRIMARY_KEYS,
            schema.tableId(),
            schema,
            List.of("100", "200", "300"));
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw-live"), new WatermarkToken("hw-live"));
    TestTransaction lwTransaction =
        new TestTransaction(
            "tx-lw",
            new ComparablePosition(40),
            List.of(watermark(schema.tableId(), "lw-live")));
    TestTransaction liveTransaction =
        new TestTransaction(
            "tx-live",
            new ComparablePosition(41),
            List.of(logInsert(schema.tableId(), "99", "late-live-row", 41)));
    TestTransaction hwTransaction =
        new TestTransaction(
            "tx-hw",
            new ComparablePosition(42),
            List.of(watermark(schema.tableId(), "hw-live")));
    RecordingChunkReader chunkReader = new RecordingChunkReader(Optional.empty());
    ExecutingFakeRuntime runtime =
        new ExecutingFakeRuntime(window, List.of(lwTransaction, liveTransaction, hwTransaction));
    DefaultTargetedRepairCoordinator<TestTransaction> coordinator =
        new DefaultTargetedRepairCoordinator<>(
            "MySQL",
            "transaction",
            runtime,
            chunkReader,
            new WindowReconciler(NoopTap.INSTANCE),
            Duration.ofSeconds(1),
            Duration.ofMillis(1),
            NoopTap.INSTANCE);

    TargetedRepairResult<TestTransaction> result = coordinator.coordinate(request, schema);

    // Under the fix, the coordinator drives the drain through a reconciler session, so the
    // live transaction's log events arrive as emittedEvents on a drain outcome instead of
    // being silently consumed from the queue.
    TargetedRepairDrainBatch<TestTransaction> drain =
        (TargetedRepairDrainBatch<TestTransaction>) result.outcome().orElseThrow();
    assertThat(drain.emittedEvents())
        .as("live log events must be surfaced through the drain outcome")
        .extracting(e -> String.valueOf(e.primaryKey().get("id")))
        .containsExactly("99");
    assertThat(drain.missingPrimaryKeys()).containsExactly("100", "200", "300");
    assertThat(drain.checkpointTransaction()).isEqualTo(hwTransaction);
    assertThat(runtime.remainingTransactions()).isZero();
  }

  @Test
  void emptyTargetedRepairDrainStillFailsClosedOnUnexpectedWatermarkTokens() throws Exception {
    TableSchema schema = schema();
    DumpRequest request =
        DumpRequest.fromPrimaryKeyLiterals(
            "repair-empty-strict",
            DumpScope.PRIMARY_KEYS,
            schema.tableId(),
            schema,
            List.of("100"));
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw-empty-strict"), new WatermarkToken("hw-empty-strict"));
    TestTransaction unexpectedToken =
        new TestTransaction(
            "tx-bad-token",
            new ComparablePosition(41),
            List.of(watermark(schema.tableId(), "unexpected-token")));
    RecordingChunkReader chunkReader = new RecordingChunkReader(Optional.empty());
    ExecutingFakeRuntime runtime =
        new ExecutingFakeRuntime(window, List.of(unexpectedToken));
    DefaultTargetedRepairCoordinator<TestTransaction> coordinator =
        new DefaultTargetedRepairCoordinator<>(
            "MySQL",
            "transaction",
            runtime,
            chunkReader,
            new WindowReconciler(NoopTap.INSTANCE),
            Duration.ofSeconds(1),
            Duration.ofMillis(1),
            NoopTap.INSTANCE);

    assertThatThrownBy(() -> coordinator.coordinate(request, schema))
        .isInstanceOf(WatermarkSequenceException.class)
        .hasMessageContaining("unexpected watermark token");
  }

  @Test
  void coordinatesTargetedRepairAndReportsMissingKeys() throws Exception {
    TableSchema schema = schema();
    DumpRequest request =
        DumpRequest.fromPrimaryKeyLiterals(
            "repair-1", DumpScope.PRIMARY_KEYS, schema.tableId(), schema, List.of("1", "2"));
    Chunk chunk =
        Chunk.fromMapRows(
            request.requestId(),
            schema.tableId().displayName(),
            schema,
            null,
            List.of(Map.of("id", "1", "name", "a")),
            "1",
            true);
    WatermarkWindow window = new WatermarkWindow(new WatermarkToken("lw-1"), new WatermarkToken("hw-1"));
    TestTransaction transaction =
        new TestTransaction(
            "tx-1",
            new ComparablePosition(1),
            List.of(watermark(schema.tableId(), "lw-1"), watermark(schema.tableId(), "hw-1")));
    FakeRuntime runtime =
        new FakeRuntime(Optional.of(chunk), window, List.of(transaction));
    DefaultTargetedRepairCoordinator<TestTransaction> coordinator =
        new DefaultTargetedRepairCoordinator<>(
            "MySQL",
            "transaction",
            runtime,
            new FakeChunkReader(chunk),
            new WindowReconciler(NoopTap.INSTANCE),
            Duration.ofSeconds(1),
            Duration.ofMillis(1),
            NoopTap.INSTANCE);

    TargetedRepairResult<TestTransaction> result = coordinator.coordinate(request, schema);
    TargetedRepairBatch<TestTransaction> batch =
        (TargetedRepairBatch<TestTransaction>) result.outcome().orElseThrow();
    coordinator.acknowledge(batch);

    assertThat(result.missingPrimaryKeys()).containsExactly("2");
    assertThat(batch.emittedEvents()).hasSize(1);
    assertThat(runtime.acknowledged).containsExactly(transaction);
  }

  private static TableSchema schema() {
    return TableSchema.create(
        new TableId("source", "appdb", "widgets"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
        Instant.parse("2026-04-10T00:00:00Z"));
  }

  private static ChangeEvent watermark(TableId tableId, String token) {
    return new ChangeEvent(
        WatermarkMetadata.tableIdFor(tableId.databaseName()),
        OperationType.WATERMARK,
        CaptureOrigin.LOG,
        WatermarkMetadata.singletonPrimaryKey(),
        null,
        ImmutableRowImage.of(Map.of(WatermarkMetadata.TOKEN_COLUMN, token)),
        new OpaqueSourcePosition("wm:" + token),
        null,
        null);
  }

  private static ChangeEvent logInsert(
      TableId tableId, String primaryKey, String name, int ordinal) {
    return new ChangeEvent(
        tableId,
        OperationType.INSERT,
        CaptureOrigin.LOG,
        ImmutableRowImage.of(Map.of("id", primaryKey)),
        null,
        ImmutableRowImage.of(Map.of("id", primaryKey, "name", name)),
        new OpaqueSourcePosition("lsn:" + ordinal),
        "tx-" + ordinal,
        null);
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

  private static final class FakeRuntime implements WatermarkWindowRuntime<TestTransaction> {
    private final Optional<Chunk> windowChunk;
    private final WatermarkWindow window;
    private final Deque<TestTransaction> transactions;
    private final java.util.List<TestTransaction> acknowledged = new java.util.ArrayList<>();

    private FakeRuntime(Optional<Chunk> windowChunk, WatermarkWindow window, List<TestTransaction> transactions) {
      this.windowChunk = windowChunk;
      this.window = window;
      this.transactions = new ArrayDeque<>(transactions);
    }

    @Override
    public <T> T executeChunkRead(SourceChunkReader reader, ChunkReadWork<T> work) {
      @SuppressWarnings("unchecked")
      T value = (T) windowChunk;
      return value;
    }

    @Override
    public <T> WatermarkWindowResult<T> executeChunkReadInWatermarkWindow(
        SourceChunkReader reader, ChunkReadWithWindowWork<T> work) {
      @SuppressWarnings("unchecked")
      T value = (T) windowChunk;
      return new WatermarkWindowResult<>(value, window);
    }

    @Override
    public Optional<TestTransaction> readPendingTransaction() {
      return Optional.ofNullable(transactions.pollFirst());
    }

    @Override
    public void acknowledge(TestTransaction transaction) {
      acknowledged.add(transaction);
    }

    int remainingTransactions() {
      return transactions.size();
    }
  }

  /**
   * Runtime stub that actually invokes the work lambda passed to
   * {@link WatermarkWindowRuntime#executeChunkReadInWatermarkWindow} so tests that inspect the
   * chunk-reader's received arguments can assert on them.
   */
  private static final class ExecutingFakeRuntime implements WatermarkWindowRuntime<TestTransaction> {
    private final WatermarkWindow window;
    private final Deque<TestTransaction> transactions;

    private ExecutingFakeRuntime(WatermarkWindow window, List<TestTransaction> transactions) {
      this.window = window;
      this.transactions = new ArrayDeque<>(transactions);
    }

    @Override
    public <T> T executeChunkRead(SourceChunkReader reader, ChunkReadWork<T> work) {
      try {
        return work.execute(new ConnectionBoundChunkReader(null, reader));
      } catch (RuntimeException ex) {
        throw ex;
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public <T> WatermarkWindowResult<T> executeChunkReadInWatermarkWindow(
        SourceChunkReader reader, ChunkReadWithWindowWork<T> work) {
      try {
        T value = work.execute(new ConnectionBoundChunkReader(null, reader), window);
        return new WatermarkWindowResult<>(value, window);
      } catch (RuntimeException ex) {
        throw ex;
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public Optional<TestTransaction> readPendingTransaction() {
      return Optional.ofNullable(transactions.pollFirst());
    }

    @Override
    public void acknowledge(TestTransaction transaction) {}

    int remainingTransactions() {
      return transactions.size();
    }
  }

  /**
   * Chunk-reader stub that captures the requested primary-key tuples so tests can assert on
   * what the coordinator actually asked for (e.g. deduplication behaviour).
   */
  private static final class RecordingChunkReader implements SourceChunkReader {
    private final Optional<Chunk> chunk;
    java.util.List<io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple>
        lastRequestedPrimaryKeyTuples = java.util.List.of();

    private RecordingChunkReader(Optional<Chunk> chunk) {
      this.chunk = chunk;
    }

    @Override
    public Optional<io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple>
        tableScanUpperBoundPrimaryKeyTuple(java.sql.Connection connection, TableSchema schema) {
      return Optional.empty();
    }

    @Override
    public Optional<Chunk> nextTableChunk(
        java.sql.Connection connection,
        String jobId,
        TableSchema schema,
        io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple startAfterPrimaryKey,
        io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple stopAtPrimaryKey,
        int chunkSize) {
      return Optional.empty();
    }

    @Override
    public Optional<Chunk> targetedPrimaryKeyTuples(
        java.sql.Connection connection,
        String jobId,
        TableSchema schema,
        List<io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple> requestedPrimaryKeys) {
      this.lastRequestedPrimaryKeyTuples = List.copyOf(requestedPrimaryKeys);
      return chunk;
    }
  }

  private static final class FakeChunkReader implements SourceChunkReader {
    private final Chunk chunk;

    private FakeChunkReader(Chunk chunk) {
      this.chunk = chunk;
    }

    @Override
    public Optional<io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple>
        tableScanUpperBoundPrimaryKeyTuple(java.sql.Connection connection, TableSchema schema) {
      return Optional.empty();
    }

    @Override
    public Optional<Chunk> nextTableChunk(
        java.sql.Connection connection,
        String jobId,
        TableSchema schema,
        io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple startAfterPrimaryKey,
        io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple stopAtPrimaryKey,
        int chunkSize) {
      return Optional.empty();
    }

    @Override
    public Optional<Chunk> targetedPrimaryKeyTuples(
        java.sql.Connection connection,
        String jobId,
        TableSchema schema,
        List<io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple> requestedPrimaryKeys) {
      return Optional.of(chunk);
    }
  }
}
