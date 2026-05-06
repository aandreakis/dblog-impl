package io.github.aandreakis.dblog.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.postgres.PostgresSourceAdapter;
import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.reconcile.WatermarkMetadata;
import io.github.aandreakis.dblog.core.reconcile.WatermarkSequenceException;
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.core.request.DumpRequestState;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.request.DumpTableProgress;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.e2e.support.PostgresInspectionOnlyDependencies;
import io.github.aandreakis.dblog.runtime.bootstrap.DbLogApplication;
import io.github.aandreakis.dblog.runtime.bootstrap.InspectionOnlySourceRuntime.InspectionCheckpointPosition;
import io.github.aandreakis.dblog.runtime.bootstrap.InspectionOnlySourceRuntime.InspectionOnlyTransaction;
import io.github.aandreakis.dblog.runtime.bootstrap.InspectionOnlySourceRuntime;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class InspectionModePostgresMetadataContaminationTests {
  @TempDir Path tempDir;

  @Test
  void failsClosedWhenWatermarkMetadataEventIsMissingARunId() throws Exception {
    // Writer-contract violation path: a watermark row with a NULL/blank run_id can never come
    // from a correctly-implemented runtime (both the metadata table CHECK constraint and the
    // runtime writer enforce non-blank run_id). Seeing one inside an active window is evidence
    // of a contract break, not a recoverable orphan, and must still fail closed.
    String h2Jdbc = "jdbc:h2:mem:phase30_postgres_metadata;MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
    TableSchema schema =
        TableSchema.create(
            new TableId("appdb", "public", "customers"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-10T00:00:00Z"));
    try (Connection connection = DriverManager.getConnection(h2Jdbc)) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("CREATE SCHEMA IF NOT EXISTS \"public\"");
        statement.execute(
            "CREATE TABLE \"public\".\"customers\" (\"id\" BIGINT PRIMARY KEY, \"name\" VARCHAR(255))");
        statement.execute(
            "INSERT INTO \"public\".\"customers\" (\"id\", \"name\") VALUES (1, 'one'), (2, 'two')");
      }
    }

    PostgresSourceAdapter adapter =
        new PostgresSourceAdapter(
            PostgresInspectionOnlyDependencies.withFixedSchema(h2Jdbc, schema));
    RelationalSourceConfig config =
        new RelationalSourceConfig(
            "sourceA",
            "jdbc:postgresql://127.0.0.1:5432/appdb",
            "postgres",
            "secret",
            "appdb",
            List.of("public.customers"),
            java.util.Map.of(),
            false);

    try (H2RuntimeStateStore stateStore =
            new H2RuntimeStateStore(tempDir.resolve("phase30-postgres-metadata-state"));
        DbLogApplication<InspectionOnlyTransaction> app = cast(
            DbLogApplication.open(adapter, config, stateStore, null, new RecordingSink(), 100))) {
      DumpRequest request = app.submit(DumpScope.TABLE, schema.tableId(), List.of());
      app.enqueueCommittedTransaction(missingRunIdWatermarkTransaction(schema.tableId()));

      assertThatThrownBy(() -> app.processPendingRequests(Duration.ofMillis(5)))
          .isInstanceOf(WatermarkSequenceException.class)
          .hasMessageContaining("unexpected watermark token");

      assertThat(stateStore.dumpRequests().loadStatus(request.requestId()).orElseThrow().state())
          .isEqualTo(DumpRequestState.ACTIVE);
      DumpTableProgress progress =
          stateStore.dumpProgress().load(request.requestId(), schema.tableId().displayName()).orElseThrow();
      assertThat(progress.hasActiveChunk()).isTrue();
    }
  }

  @Test
  void recoversFromStaleOwnRunWatermarksByDrainingThemOnRetryWithinTheSameSession()
      throws Exception {
    // A mid-window contract-violation (missing-runId watermark) crashes the first attempt
    // after the runtime has already written its own low/high to dblog_meta.watermarks. The
    // synthetic own-run transaction is left queued. On retry, the coordinator opens a new
    // window with fresh tokens; the stale own-run tokens from the previous attempt arrive
    // first in the reconciler and are drained via the runId-tagged-drain branch so the new
    // window can complete.
    String h2Jdbc =
        "jdbc:h2:mem:phase30_postgres_metadata_stale;MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
    TableSchema schema =
        TableSchema.create(
            new TableId("appdb", "public", "customers"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-10T00:00:00Z"));
    try (Connection connection = DriverManager.getConnection(h2Jdbc)) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("CREATE SCHEMA IF NOT EXISTS \"public\"");
        statement.execute(
            "CREATE TABLE \"public\".\"customers\" (\"id\" BIGINT PRIMARY KEY, \"name\" VARCHAR(255))");
        statement.execute(
            "INSERT INTO \"public\".\"customers\" (\"id\", \"name\") VALUES (1, 'one'), (2, 'two')");
      }
    }

    PostgresSourceAdapter adapter =
        new PostgresSourceAdapter(
            PostgresInspectionOnlyDependencies.withFixedSchema(h2Jdbc, schema));
    RelationalSourceConfig config =
        new RelationalSourceConfig(
            "sourceA",
            "jdbc:postgresql://127.0.0.1:5432/appdb",
            "postgres",
            "secret",
            "appdb",
            List.of("public.customers"),
            java.util.Map.of(),
            false);

    RecordingSink sink = new RecordingSink();
    try (H2RuntimeStateStore stateStore =
            new H2RuntimeStateStore(tempDir.resolve("phase30-postgres-metadata-stale-state"));
        DbLogApplication<InspectionOnlyTransaction> app = cast(
            DbLogApplication.open(adapter, config, stateStore, null, sink, 100))) {
      DumpRequest request = app.submit(DumpScope.TABLE, schema.tableId(), List.of());

      // Contract-violation contamination forces the first attempt to fail closed after the
      // runtime has already queued its own synthetic low/high.
      app.enqueueCommittedTransaction(missingRunIdWatermarkTransaction(schema.tableId()));

      assertThatThrownBy(() -> app.processPendingRequests(Duration.ofMillis(50)))
          .isInstanceOf(WatermarkSequenceException.class)
          .hasMessageContaining("unexpected watermark token");
      assertThat(stateStore.dumpRequests().loadStatus(request.requestId()).orElseThrow().state())
          .isEqualTo(DumpRequestState.ACTIVE);

      // Retry within the same session. The queue still holds the synthetic watermark
      // transaction that the first attempt's executeWithinWatermarkWindow enqueued (own-run
      // tokens, previous attempt's low/high). The runId-tagged drain silently discards them
      // so the new window's tokens complete the chunk.
      app.processPendingRequests(Duration.ofMillis(500));

      assertThat(stateStore.dumpRequests().loadStatus(request.requestId()).orElseThrow().state())
          .isEqualTo(DumpRequestState.COMPLETED);
      assertThat(sink.emittedEvents).hasSize(2);
    }
  }

  private static InspectionOnlyTransaction missingRunIdWatermarkTransaction(TableId tableId) {
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    // Deliberately omit RUN_ID_COLUMN — this is the contract-violation shape.
    afterRow.put(WatermarkMetadata.TOKEN_COLUMN, "contract-violation-token");
    InspectionCheckpointPosition foreignPosition =
        new InspectionCheckpointPosition(999_999L, "missing-run-id");
    return new InspectionOnlyTransaction(
        "missing-run-id-watermark",
        foreignPosition,
        Instant.parse("2026-04-10T00:00:01Z"),
        List.of(
            new ChangeEvent(
                WatermarkMetadata.tableIdFor(tableId.databaseName()),
                OperationType.WATERMARK,
                CaptureOrigin.LOG,
                WatermarkMetadata.singletonPrimaryKey(),
                null,
                ImmutableRowImage.of(afterRow),
                foreignPosition,
                "missing-run-id-watermark",
                null)));
  }

  /**
   * Cross-restart orphan recovery. Phase A starts a dump, fails mid-window, and is torn down
   * with the request still ACTIVE in the state store. Phase B is a fresh {@link
   * DbLogApplication} — new runtime, new {@code runId}, same state store. In a real
   * restart, phase A's {@code dblog_meta.watermarks} row still sits in the source log and is
   * re-delivered as phase B resumes streaming. We simulate that by enqueueing an orphan
   * watermark stamped with phase A's runId into phase B's queue before phase B opens its
   * replacement window.
   *
   * <p>The reconciler recognizes the orphan as a runId-tagged stale token and drains it
   * silently per {@code SPEC.md §8/§12} ("control rows are only honored for the current DBLog
   * run"). Phase B opens a fresh window, the orphan passes through the drain branch, and the
   * chunk completes without operator intervention.
   */
  @Test
  void recoversFromCrossRestartOrphanWatermarksLeftByAPriorProcess() throws Exception {
    String h2Jdbc =
        "jdbc:h2:mem:phase30_postgres_cross_restart_orphan;MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
    TableSchema schema =
        TableSchema.create(
            new TableId("appdb", "public", "customers"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-10T00:00:00Z"));
    try (Connection connection = DriverManager.getConnection(h2Jdbc)) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("CREATE SCHEMA IF NOT EXISTS \"public\"");
        statement.execute(
            "CREATE TABLE \"public\".\"customers\" (\"id\" BIGINT PRIMARY KEY, \"name\" VARCHAR(255))");
        statement.execute(
            "INSERT INTO \"public\".\"customers\" (\"id\", \"name\") VALUES (1, 'one'), (2, 'two')");
      }
    }

    PostgresSourceAdapter adapter =
        new PostgresSourceAdapter(
            PostgresInspectionOnlyDependencies.withFixedSchema(h2Jdbc, schema));
    RelationalSourceConfig config =
        new RelationalSourceConfig(
            "sourceA",
            "jdbc:postgresql://127.0.0.1:5432/appdb",
            "postgres",
            "secret",
            "appdb",
            List.of("public.customers"),
            java.util.Map.of(),
            false);

    Path statePath = tempDir.resolve("phase30-postgres-cross-restart-orphan-state");

    String phaseARunId;
    String requestId;

    // Phase A: submit a dump; a contract-violation contamination crashes it mid-window so the
    // state store ends up with ACTIVE request + hasActiveChunk progress — mirroring a real
    // mid-window crash.
    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(statePath);
        DbLogApplication<InspectionOnlyTransaction> app =
            cast(
                DbLogApplication.open(
                    adapter, config, stateStore, null, new RecordingSink(), 100))) {
      DumpRequest request = app.submit(DumpScope.TABLE, schema.tableId(), List.of());
      requestId = request.requestId();
      phaseARunId = ((InspectionOnlySourceRuntime) app.stack().session().runtime()).currentRunId();

      app.enqueueCommittedTransaction(missingRunIdWatermarkTransaction(schema.tableId()));
      assertThatThrownBy(() -> app.processPendingRequests(Duration.ofMillis(50)))
          .isInstanceOf(WatermarkSequenceException.class);

      assertThat(stateStore.dumpRequests().loadStatus(requestId).orElseThrow().state())
          .isEqualTo(DumpRequestState.ACTIVE);
      assertThat(
              stateStore
                  .dumpProgress()
                  .load(requestId, schema.tableId().displayName())
                  .orElseThrow()
                  .hasActiveChunk())
          .isTrue();
    }

    // Phase B: new application = new runtime = new runId, same state store. Inject an orphan
    // watermark stamped with phase A's runId to simulate the source log replaying phase A's
    // dblog_meta.watermarks row into phase B's stream. Expect: phase B opens its own window,
    // drains the orphan (runId-tagged but not matching low/high), completes the chunk.
    RecordingSink sinkB = new RecordingSink();
    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(statePath);
        DbLogApplication<InspectionOnlyTransaction> app =
            cast(
                DbLogApplication.open(
                    adapter, config, stateStore, null, sinkB, 100))) {
      String phaseBRunId =
          ((InspectionOnlySourceRuntime) app.stack().session().runtime()).currentRunId();
      assertThat(phaseBRunId).isNotEqualTo(phaseARunId);

      app.enqueueCommittedTransaction(
          orphanWatermarkTransactionWithRunId(
              schema.tableId(), phaseARunId, "phase-a-orphan-token"));

      app.processPendingRequests(Duration.ofMillis(500));

      assertThat(stateStore.dumpRequests().loadStatus(requestId).orElseThrow().state())
          .isEqualTo(DumpRequestState.COMPLETED);
      assertThat(sinkB.emittedEvents).hasSize(2);
    }
  }

  private static InspectionOnlyTransaction orphanWatermarkTransactionWithRunId(
      TableId tableId, String runId, String token) {
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put(WatermarkMetadata.RUN_ID_COLUMN, runId);
    afterRow.put(WatermarkMetadata.TOKEN_COLUMN, token);
    InspectionCheckpointPosition position =
        new InspectionCheckpointPosition(888_777L, "phase-a-orphan");
    return new InspectionOnlyTransaction(
        "phase-a-orphan-watermark",
        position,
        Instant.parse("2026-04-10T00:00:00.250Z"),
        List.of(
            new ChangeEvent(
                WatermarkMetadata.tableIdFor(tableId.databaseName()),
                OperationType.WATERMARK,
                CaptureOrigin.LOG,
                WatermarkMetadata.singletonPrimaryKey(),
                null,
                ImmutableRowImage.of(afterRow),
                position,
                "phase-a-orphan-watermark",
                null)));
  }

  @SuppressWarnings("unchecked")
  private static <T> T cast(Object value) {
    return (T) value;
  }

  private static final class RecordingSink implements io.github.aandreakis.dblog.sink.api.ChangeEventSink {
    private final List<ChangeEvent> emittedEvents = new ArrayList<>();

    @Override
    public void appendEvents(List<ChangeEvent> events) {
      emittedEvents.addAll(events);
    }
  }
}
