package io.github.aandreakis.dblog.core.request;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.adapter.api.BoundChunkReader;
import io.github.aandreakis.dblog.adapter.api.ChunkReadWithWindowWork;
import io.github.aandreakis.dblog.adapter.api.ChunkReadWork;
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
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import io.github.aandreakis.dblog.tap.NoopTap;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DefaultDumpWindowCoordinatorTests {
  @TempDir Path tempDir;

  @Test
  void coordinatesOneDumpWindowAndPersistsCompletedProgress() throws Exception {
    TableSchema schema = schema();
    Chunk chunk =
        Chunk.fromMapRows(
            "job-1",
            schema.tableId().displayName(),
            schema,
            null,
            List.of(Map.of("id", "1", "name", "a"), Map.of("id", "2", "name", "b")),
            "2",
            true);
    WatermarkWindow window = new WatermarkWindow(new WatermarkToken("lw-1"), new WatermarkToken("hw-1"));
    TestTransaction transaction =
        new TestTransaction(
            "tx-1",
            new ComparablePosition(1),
            List.of(
                watermark(schema.tableId(), "lw-1"),
                watermark(schema.tableId(), "hw-1")));
    FakeRuntime runtime =
        new FakeRuntime(
            window,
            List.of(Optional.of(schema.primaryKeyTupleFromLiteral("2")), Optional.of(chunk)),
            Optional.of(chunk),
            List.of(transaction));

    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(tempDir.resolve("dump-window-state"))) {
      DefaultDumpWindowCoordinator<TestTransaction> coordinator =
          new DefaultDumpWindowCoordinator<>(
              "MySQL",
              "transaction",
              runtime,
              stateStore.dumpProgress(),
              stateStore.schemas(),
              new FakeChunkReader(chunk, Optional.of("2")),
              new WindowReconciler(NoopTap.INSTANCE),
              Duration.ofSeconds(1),
              Duration.ofMillis(1), NoopTap.INSTANCE);

      DumpWindowBatch<TestTransaction> batch = (DumpWindowBatch<TestTransaction>)
          coordinator.coordinateNextTableChunk("job-1", schema, 100).orElseThrow();
      coordinator.acknowledgeCompletedBatch(batch);

      assertThat(batch.emittedEvents()).hasSize(2);
      assertThat(runtime.acknowledged).containsExactly(transaction);
      assertThat(stateStore.dumpProgress().load("job-1", schema.tableId().displayName()))
          .isPresent()
          .get()
          .extracting(DumpTableProgress::lastCompletedPrimaryKey)
          .isEqualTo("2");
    }
  }

  @Test
  void failsClosedWhenChunkReadReturnsSelectedColumnSchemaDrift() throws Exception {
    TableSchema schema = schema();
    TableSchema driftedSchema =
        TableSchema.create(
            schema.tableId(),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "boolean", NeutralColumnType.BOOLEAN, false, true)),
            Instant.parse("2026-04-10T00:05:00Z"));
    Chunk driftedChunk =
        Chunk.fromMapRows(
            "job-drift",
            schema.tableId().displayName(),
            driftedSchema,
            null,
            List.of(Map.of("id", "1", "name", true)),
            "1",
            true);
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw-drift"), new WatermarkToken("hw-drift"));
    FakeRuntime runtime =
        new FakeRuntime(
            window,
            List.of(Optional.of(schema.primaryKeyTupleFromLiteral("1"))),
            Optional.of(driftedChunk),
            List.of());

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("dump-window-schema-drift"))) {
      DefaultDumpWindowCoordinator<TestTransaction> coordinator =
          new DefaultDumpWindowCoordinator<>(
              "MySQL",
              "transaction",
              runtime,
              stateStore.dumpProgress(),
              stateStore.schemas(),
              new FakeChunkReader(driftedChunk, Optional.of("1")),
              new WindowReconciler(NoopTap.INSTANCE),
              Duration.ofSeconds(1),
              Duration.ofMillis(1),
              NoopTap.INSTANCE);

      assertThatThrownBy(() -> coordinator.coordinateNextTableChunk("job-drift", schema, 100))
          .isInstanceOf(SchemaDriftException.class)
          .hasMessageContaining("selected-column contract changed")
          .hasMessageContaining("full dump required");
    }
  }

  @Test
  void returnsEmptyWhenTableHasNoRowsBecauseUpperBoundCannotBeCaptured() throws Exception {
    // An empty table returns `Optional.empty()` from `tableScanUpperBoundPrimaryKeyTuple`.
    // The coordinator must treat that as "no work for this table": return empty, and never
    // attempt to open a watermark window (which would pointlessly write LW/HW for nothing).
    TableSchema schema = schema();
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw-empty"), new WatermarkToken("hw-empty"));
    RecordingRuntime runtime = new RecordingRuntime(window, java.util.List.of(Optional.empty()));
    FakeChunkReader chunkReader = new FakeChunkReader(null, Optional.empty());

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("empty-table-state"))) {
      DefaultDumpWindowCoordinator<TestTransaction> coordinator =
          new DefaultDumpWindowCoordinator<>(
              "MySQL",
              "transaction",
              runtime,
              stateStore.dumpProgress(),
              stateStore.schemas(),
              chunkReader,
              new WindowReconciler(NoopTap.INSTANCE),
              Duration.ofSeconds(1),
              Duration.ofMillis(1), NoopTap.INSTANCE);

      Optional<DumpWindowOutcome<TestTransaction>> batch =
          coordinator.coordinateNextTableChunk("job-empty", schema, 100);

      assertThat(batch).isEmpty();
      // No watermark window was opened — the coordinator must avoid emitting pointless LW/HW
      // when the upper bound is null.
      assertThat(runtime.windowOpenCount).isZero();
      // Progress row persisted, but it marks the table as having no work (null upper bound).
      DumpTableProgress progress =
          stateStore
              .dumpProgress()
              .load("job-empty", schema.tableId().displayName())
              .orElseThrow();
      assertThat(progress.requestUpperBoundPrimaryKey()).isNull();
      assertThat(progress.hasActiveChunk()).isFalse();
    }
  }

  @Test
  void coordinatesChunkWhenPrimaryKeysHaveGaps() throws Exception {
    // A table with non-contiguous primary keys (id=1, 3, 5, 7) must be handled normally: the
    // coordinator cares about ordered selection, not dense PK values. This locks in that the
    // chunk engine does not rely on PK adjacency anywhere.
    TableSchema schema = schema();
    Chunk chunk =
        Chunk.fromMapRows(
            "job-gaps",
            schema.tableId().displayName(),
            schema,
            null,
            List.of(
                Map.of("id", "1", "name", "a"),
                Map.of("id", "3", "name", "c"),
                Map.of("id", "5", "name", "e"),
                Map.of("id", "7", "name", "g")),
            "7",
            true);
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw-gaps"), new WatermarkToken("hw-gaps"));
    TestTransaction transaction =
        new TestTransaction(
            "tx-gaps",
            new ComparablePosition(1),
            List.of(
                watermark(schema.tableId(), "lw-gaps"), watermark(schema.tableId(), "hw-gaps")));
    FakeRuntime runtime =
        new FakeRuntime(
            window,
            List.of(Optional.of(schema.primaryKeyTupleFromLiteral("7"))),
            Optional.of(chunk),
            List.of(transaction));

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("gaps-state"))) {
      DefaultDumpWindowCoordinator<TestTransaction> coordinator =
          new DefaultDumpWindowCoordinator<>(
              "MySQL",
              "transaction",
              runtime,
              stateStore.dumpProgress(),
              stateStore.schemas(),
              new FakeChunkReader(chunk, Optional.of("7")),
              new WindowReconciler(NoopTap.INSTANCE),
              Duration.ofSeconds(1),
              Duration.ofMillis(1), NoopTap.INSTANCE);

      DumpWindowBatch<TestTransaction> batch = (DumpWindowBatch<TestTransaction>)
          coordinator.coordinateNextTableChunk("job-gaps", schema, 100).orElseThrow();
      coordinator.acknowledgeCompletedBatch(batch);

      // All four non-contiguous rows emitted.
      assertThat(batch.emittedEvents()).hasSize(4);
      assertThat(batch.emittedEvents())
          .extracting(e -> String.valueOf(e.primaryKey().get("id")))
          .containsExactly("1", "3", "5", "7");
      assertThat(
              stateStore
                  .dumpProgress()
                  .load("job-gaps", schema.tableId().displayName())
                  .orElseThrow()
                  .lastCompletedPrimaryKey())
          .isEqualTo("7");
    }
  }

  @Test
  void returnsEmptyWhenLastCompletedAlreadyReachedRequestUpperBound() throws Exception {
    // Pre-seed persisted progress that is already at the upper bound, and verify the
    // coordinator returns empty without opening a new watermark window.
    TableSchema schema = schema();
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw-done"), new WatermarkToken("hw-done"));
    RecordingRuntime runtime = new RecordingRuntime(window, List.of());

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("done-state"))) {
      // Seed a progress row that is already completed at upper bound "5".
      DumpTableProgress seeded =
          DumpTableProgress.initial(
                  "job-done", schema.tableId().displayName(), schema.fingerprint())
              .captureRequestUpperBound(schema, "5");
      Chunk seedChunk =
          Chunk.fromMapRows(
              "job-done",
              schema.tableId().displayName(),
              schema,
              null,
              List.of(Map.of("id", "5", "name", "e")),
              "5",
              true);
      DumpTableProgress completed = seeded.beginChunk(seedChunk, window).completeChunk(seedChunk);
      stateStore.dumpProgress().save(completed);

      DefaultDumpWindowCoordinator<TestTransaction> coordinator =
          new DefaultDumpWindowCoordinator<>(
              "MySQL",
              "transaction",
              runtime,
              stateStore.dumpProgress(),
              stateStore.schemas(),
              new FakeChunkReader(seedChunk, Optional.of("5")),
              new WindowReconciler(NoopTap.INSTANCE),
              Duration.ofSeconds(1),
              Duration.ofMillis(1), NoopTap.INSTANCE);

      Optional<DumpWindowOutcome<TestTransaction>> batch =
          coordinator.coordinateNextTableChunk("job-done", schema, 100);

      assertThat(batch).isEmpty();
      // No fresh watermark window should be opened — the coordinator short-circuits because
      // the persisted progress shows the upper bound has been reached.
      assertThat(runtime.windowOpenCount).isZero();
    }
  }

  @Test
  void continuesTextKeyDumpWhenSourceCollationDiffersFromJavaOrdering() throws Exception {
    TableSchema schema = collatedTextKeySchema();
    WatermarkWindow firstWindow =
        new WatermarkWindow(new WatermarkToken("lw-first"), new WatermarkToken("hw-first"));
    Chunk firstChunk =
        Chunk.fromMapRows(
            "job-collation",
            schema.tableId().displayName(),
            schema,
            null,
            List.of(Map.of("id", "a", "name", "first")),
            "a",
            false);
    DumpTableProgress persistedProgress =
        DumpTableProgress.initial(
                "job-collation", schema.tableId().displayName(), schema.fingerprint())
            .captureRequestUpperBound(schema, "Z")
            .beginChunk(firstChunk, firstWindow)
            .completeChunk(firstChunk);

    WatermarkWindow secondWindow =
        new WatermarkWindow(new WatermarkToken("lw-second"), new WatermarkToken("hw-second"));
    Chunk secondChunk =
        Chunk.fromMapRows(
            "job-collation",
            schema.tableId().displayName(),
            schema,
            "a",
            List.of(Map.of("id", "b", "name", "second")),
            "b",
            false);
    TestTransaction transaction =
        new TestTransaction(
            "tx-second",
            new ComparablePosition(2),
            List.of(
                watermark(schema.tableId(), "lw-second"),
                watermark(schema.tableId(), "hw-second")));
    FakeRuntime runtime =
        new FakeRuntime(secondWindow, List.of(), Optional.of(secondChunk), List.of(transaction));

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("collated-text-key-state"))) {
      stateStore.dumpProgress().save(persistedProgress);
      DefaultDumpWindowCoordinator<TestTransaction> coordinator =
          new DefaultDumpWindowCoordinator<>(
              "MySQL",
              "transaction",
              runtime,
              stateStore.dumpProgress(),
              stateStore.schemas(),
              new FakeChunkReader(secondChunk, Optional.of("Z")),
              new WindowReconciler(NoopTap.INSTANCE),
              Duration.ofSeconds(1),
              Duration.ofMillis(1),
              NoopTap.INSTANCE);

      DumpWindowBatch<TestTransaction> batch =
          (DumpWindowBatch<TestTransaction>)
              coordinator
                  .coordinateNextTableChunk("job-collation", schema, 1)
                  .orElseThrow();
      coordinator.acknowledgeCompletedBatch(batch);

      assertThat(batch.chunk().rows())
          .extracting(row -> row.get("id"))
          .containsExactly("b");
      assertThat(
              stateStore
                  .dumpProgress()
                  .load("job-collation", schema.tableId().displayName())
                  .orElseThrow()
                  .lastCompletedPrimaryKey())
          .isEqualTo("b");
    }
  }

  @Test
  void emptyChunkDrainMustNotSilentlyEatLiveTransactions() throws Exception {
    // Bug repro: the empty-chunk fast path opens LW/HW, sees a zero-row chunk, then "drains to
    // HW" by repeatedly calling readPendingTransaction() and discarding whatever is not the
    // matching HW token. readPendingTransaction() is destructive, so any ordinary LOG
    // transaction that committed between LW and HW is popped and dropped without reaching the
    // sink.
    //
    // Scenario:
    //   - Upper bound was captured as pk "9".
    //   - The watermark-scoped SELECT returns an empty chunk (e.g. tail rows deleted after the
    //     upper bound was captured, or the table was fully dumped by an earlier request).
    //   - The source queue contains [tx-live (ordinary insert), tx-hw (the matching HW)].
    //
    // Contract: tx-live's events must reach the sink. Because the empty-chunk path returns
    // Optional.empty() (no batch), the only way that contract can hold is for tx-live to remain
    // in the source queue after coordinateNextTableChunk returns, so the streaming pump can
    // pick it up on the next turn. Today, the drain loop pops tx-live, sees it has no HW
    // token, and silently discards it — leaving an empty queue.
    TableSchema schema = schema();
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw-tail"), new WatermarkToken("hw-tail"));
    TestTransaction lwTransaction =
        new TestTransaction(
            "tx-lw",
            new ComparablePosition(40),
            List.of(watermark(schema.tableId(), "lw-tail")));
    TestTransaction liveTransaction =
        new TestTransaction(
            "tx-live",
            new ComparablePosition(41),
            List.of(logInsert(schema.tableId(), "99", "late-live-row", 41)));
    TestTransaction hwTransaction =
        new TestTransaction(
            "tx-hw",
            new ComparablePosition(42),
            List.of(watermark(schema.tableId(), "hw-tail")));
    FakeRuntime runtime =
        new FakeRuntime(
            window,
            List.of(Optional.of(schema.primaryKeyTupleFromLiteral("9"))),
            Optional.empty(),
            List.of(lwTransaction, liveTransaction, hwTransaction));

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("empty-chunk-drain-state"))) {
      DefaultDumpWindowCoordinator<TestTransaction> coordinator =
          new DefaultDumpWindowCoordinator<>(
              "MySQL",
              "transaction",
              runtime,
              stateStore.dumpProgress(),
              stateStore.schemas(),
              new FakeChunkReader(null, Optional.of("9")),
              new WindowReconciler(NoopTap.INSTANCE),
              Duration.ofSeconds(1),
              Duration.ofMillis(1),
              NoopTap.INSTANCE);

      DumpWindowDrainBatch<TestTransaction> drain =
          (DumpWindowDrainBatch<TestTransaction>)
              coordinator
                  .coordinateNextTableChunk("job-empty-tail", schema, 100)
                  .orElseThrow();

      // Under the fix, the empty-chunk branch drives the drain through a reconciler session so
      // live-log transactions committed between LW and HW are surfaced as emittedEvents on a
      // drain outcome instead of being silently consumed from the queue.
      assertThat(drain.emittedEvents())
          .as("live log events must be surfaced through the drain outcome")
          .extracting(e -> String.valueOf(e.primaryKey().get("id")))
          .containsExactly("99");
      assertThat(drain.checkpointTransaction()).isEqualTo(hwTransaction);
    }
  }

  @Test
  void emptyChunkDrainStillFailsClosedOnUnexpectedWatermarkTokens() throws Exception {
    TableSchema schema = schema();
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw-empty-strict"), new WatermarkToken("hw-empty-strict"));
    TestTransaction unexpectedToken =
        new TestTransaction(
            "tx-bad-token",
            new ComparablePosition(41),
            List.of(watermark(schema.tableId(), "unexpected-token")));
    FakeRuntime runtime =
        new FakeRuntime(
            window,
            List.of(Optional.of(schema.primaryKeyTupleFromLiteral("9"))),
            Optional.empty(),
            List.of(unexpectedToken));

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("empty-chunk-drain-strictness-state"))) {
      DefaultDumpWindowCoordinator<TestTransaction> coordinator =
          new DefaultDumpWindowCoordinator<>(
              "MySQL",
              "transaction",
              runtime,
              stateStore.dumpProgress(),
              stateStore.schemas(),
              new FakeChunkReader(null, Optional.of("9")),
              new WindowReconciler(NoopTap.INSTANCE),
              Duration.ofSeconds(1),
              Duration.ofMillis(1),
              NoopTap.INSTANCE);

      assertThatThrownBy(() -> coordinator.coordinateNextTableChunk("job-empty-strict", schema, 100))
          .isInstanceOf(WatermarkSequenceException.class)
          .hasMessageContaining("unexpected watermark token");
    }
  }

  @Test
  void acknowledgingEmptyChunkDrainMarksProgressAtCapturedUpperBound() throws Exception {
    TableSchema schema = schema();
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw-ack"), new WatermarkToken("hw-ack"));
    TestTransaction lwTransaction =
        new TestTransaction(
            "tx-lw",
            new ComparablePosition(40),
            List.of(watermark(schema.tableId(), "lw-ack")));
    TestTransaction hwTransaction =
        new TestTransaction(
            "tx-hw",
            new ComparablePosition(42),
            List.of(watermark(schema.tableId(), "hw-ack")));
    FakeRuntime runtime =
        new FakeRuntime(
            window,
            List.of(Optional.of(schema.primaryKeyTupleFromLiteral("9"))),
            Optional.empty(),
            List.of(lwTransaction, hwTransaction));

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("empty-chunk-drain-ack-state"))) {
      DefaultDumpWindowCoordinator<TestTransaction> coordinator =
          new DefaultDumpWindowCoordinator<>(
              "MySQL",
              "transaction",
              runtime,
              stateStore.dumpProgress(),
              stateStore.schemas(),
              new FakeChunkReader(null, Optional.of("9")),
              new WindowReconciler(NoopTap.INSTANCE),
              Duration.ofSeconds(1),
              Duration.ofMillis(1),
              NoopTap.INSTANCE);

      DumpWindowDrainBatch<TestTransaction> drain =
          (DumpWindowDrainBatch<TestTransaction>)
              coordinator
                  .coordinateNextTableChunk("job-empty-ack", schema, 100)
                  .orElseThrow();

      coordinator.acknowledgeCompletedBatch(drain);

      DumpTableProgress progress =
          stateStore.dumpProgress().load("job-empty-ack", schema.tableId().displayName()).orElseThrow();
      assertThat(progress.chunkCompleted()).isTrue();
      assertThat(progress.lastCompletedPrimaryKey()).isEqualTo("9");
      assertThat(runtime.acknowledged).containsExactly(hwTransaction);
    }
  }

  @Test
  void collatedTextDumpCompletesAtMissingCapturedUpperBoundAfterEmptyDrain()
      throws Exception {
    TableSchema schema = collatedTextKeySchema();
    Chunk firstChunk =
        Chunk.fromMapRows(
            "job-collation-missing-upper",
            schema.tableId().displayName(),
            schema,
            null,
            List.of(Map.of("id", "a", "name", "first")),
            "a",
            false);
    DumpTableProgress persistedProgress =
        DumpTableProgress.initial(
                "job-collation-missing-upper",
                schema.tableId().displayName(),
                schema.fingerprint())
            .captureRequestUpperBound(schema, "Z")
            .beginChunk(
                firstChunk,
                new WatermarkWindow(
                    new WatermarkToken("lw-first"), new WatermarkToken("hw-first")))
            .completeChunk(firstChunk);

    WatermarkWindow drainWindow =
        new WatermarkWindow(new WatermarkToken("lw-drain"), new WatermarkToken("hw-drain"));
    TestTransaction lowWatermark =
        new TestTransaction(
            "tx-lw", new ComparablePosition(40), List.of(watermark(schema.tableId(), "lw-drain")));
    TestTransaction highWatermark =
        new TestTransaction(
            "tx-hw", new ComparablePosition(42), List.of(watermark(schema.tableId(), "hw-drain")));
    FakeRuntime runtime =
        new FakeRuntime(
            drainWindow,
            List.of(),
            Optional.empty(),
            List.of(lowWatermark, highWatermark));

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("collated-missing-upper-bound-state"))) {
      stateStore.dumpProgress().save(persistedProgress);
      DefaultDumpWindowCoordinator<TestTransaction> coordinator =
          new DefaultDumpWindowCoordinator<>(
              "MySQL",
              "transaction",
              runtime,
              stateStore.dumpProgress(),
              stateStore.schemas(),
              new FakeChunkReader(null, Optional.of("Z")),
              new WindowReconciler(NoopTap.INSTANCE),
              Duration.ofSeconds(1),
              Duration.ofMillis(1),
              NoopTap.INSTANCE);

      DumpWindowDrainBatch<TestTransaction> drain =
          (DumpWindowDrainBatch<TestTransaction>)
              coordinator
                  .coordinateNextTableChunk("job-collation-missing-upper", schema, 1)
                  .orElseThrow();
      coordinator.acknowledgeCompletedBatch(drain);

      DumpTableProgress completed =
          stateStore
              .dumpProgress()
              .load("job-collation-missing-upper", schema.tableId().displayName())
              .orElseThrow();
      assertThat(completed.lastCompletedPrimaryKey()).isEqualTo("Z");
      assertThat(runtime.windowOpenCount).isEqualTo(1);

      assertThat(
              coordinator.coordinateNextTableChunk(
                  "job-collation-missing-upper", schema, 1))
          .isEmpty();
      assertThat(runtime.windowOpenCount)
          .as("completed collated dump must not reopen a watermark window")
          .isEqualTo(1);
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

  private static TableSchema collatedTextKeySchema() {
    return TableSchema.create(
        new TableId("source", "appdb", "collated_widgets"),
        List.of(
            new ColumnDefinition("id", "varchar(32)", NeutralColumnType.STRING, true, false),
            new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
        Instant.parse("2026-04-10T00:00:00Z"));
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
    private final WatermarkWindow window;
    private final Deque<Object> sqlResults;
    private final Optional<Chunk> windowChunk;
    private final Deque<TestTransaction> transactions;
    private final java.util.List<TestTransaction> acknowledged = new java.util.ArrayList<>();
    private int windowOpenCount;

    private FakeRuntime(
        WatermarkWindow window,
        List<Object> sqlResults,
        Optional<Chunk> windowChunk,
        List<TestTransaction> transactions) {
      this.window = window;
      this.sqlResults = new ArrayDeque<>(sqlResults);
      this.windowChunk = windowChunk;
      this.transactions = new ArrayDeque<>(transactions);
    }

    @Override
    public <T> T executeChunkRead(SourceChunkReader reader, ChunkReadWork<T> work) {
      @SuppressWarnings("unchecked")
      T value = (T) sqlResults.removeFirst();
      return value;
    }

    @Override
    public <T> WatermarkWindowResult<T> executeChunkReadInWatermarkWindow(
        SourceChunkReader reader, ChunkReadWithWindowWork<T> work) {
      windowOpenCount++;
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
  }

  /**
   * Runtime stub that counts how many times {@code executeChunkReadInWatermarkWindow} was invoked,
   * so tests can assert that the coordinator short-circuits without opening a pointless
   * watermark window (e.g. empty-table case, already-at-upper-bound case).
   */
  private static final class RecordingRuntime implements WatermarkWindowRuntime<TestTransaction> {
    private final WatermarkWindow window;
    private final Deque<Object> sqlResults;
    int windowOpenCount;

    private RecordingRuntime(WatermarkWindow window, List<Object> sqlResults) {
      this.window = window;
      this.sqlResults = new ArrayDeque<>(sqlResults);
    }

    @Override
    public <T> T executeChunkRead(SourceChunkReader reader, ChunkReadWork<T> work) {
      if (sqlResults.isEmpty()) {
        throw new AssertionError(
            "RecordingRuntime.executeChunkRead called without a canned result — test expected zero"
                + " SQL calls at this point.");
      }
      @SuppressWarnings("unchecked")
      T value = (T) sqlResults.removeFirst();
      return value;
    }

    @Override
    public <T> WatermarkWindowResult<T> executeChunkReadInWatermarkWindow(
        SourceChunkReader reader, ChunkReadWithWindowWork<T> work) {
      windowOpenCount++;
      return new WatermarkWindowResult<>(null, window);
    }

    @Override
    public Optional<TestTransaction> readPendingTransaction() {
      return Optional.empty();
    }

    @Override
    public void acknowledge(TestTransaction transaction) {}
  }

  private static final class FakeChunkReader implements SourceChunkReader {
    private final Chunk chunk;
    private final Optional<String> upperBound;

    private FakeChunkReader(Chunk chunk, Optional<String> upperBound) {
      this.chunk = chunk;
      this.upperBound = upperBound;
    }

    @Override
    public Optional<io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple>
        tableScanUpperBoundPrimaryKeyTuple(java.sql.Connection connection, TableSchema schema) {
      return upperBound.map(schema::primaryKeyTupleFromLiteral);
    }

    @Override
    public Optional<Chunk> nextTableChunk(
        java.sql.Connection connection,
        String jobId,
        TableSchema schema,
        io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple startAfterPrimaryKey,
        io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple stopAtPrimaryKey,
        int chunkSize) {
      return chunk == null ? Optional.empty() : Optional.of(chunk);
    }

    @Override
    public Optional<Chunk> targetedPrimaryKeyTuples(
        java.sql.Connection connection,
        String jobId,
        TableSchema schema,
        List<io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple> requestedPrimaryKeys) {
      return Optional.empty();
    }
  }
}
