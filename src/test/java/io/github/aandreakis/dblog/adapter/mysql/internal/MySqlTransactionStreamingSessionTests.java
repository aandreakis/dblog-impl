package io.github.aandreakis.dblog.adapter.mysql.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.github.aandreakis.dblog.adapter.api.SourceRequiresFullDumpException;
import io.github.aandreakis.dblog.adapter.api.SourceSchemaUncertaintyException;
import io.github.aandreakis.dblog.adapter.mysql.MySqlBinlogTransaction;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourceCheckpointStore;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourcePosition;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.reconcile.HeartbeatMetadata;
import io.github.aandreakis.dblog.core.reconcile.WatermarkMetadata;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.runtime.sql.SourceFlowControlSnapshot;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import io.github.aandreakis.dblog.testsupport.ThrowingSchemaSignalRuntimeStateStore;
import java.nio.file.Path;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MySqlTransactionStreamingSessionTests {
  @TempDir Path tempDir;

  @Test
  void readsTransactionsAndPersistsCheckpointOnAcknowledge() throws Exception {
    TableSchema schema =
        TableSchema.create(
            new TableId("sourceA", "appdb", "widgets"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-11T00:00:00Z"));
    MySqlBinlogTransaction transaction =
        new MySqlBinlogTransaction(
            "tx-1",
            null,
            new MySqlSourcePosition("mysql-bin.000001", 42L, null),
            Instant.parse("2026-04-11T00:00:01Z"),
            List.<ChangeEvent>of());

    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(tempDir.resolve("mysql-streaming-session"))) {
      MySqlSourceCheckpointStore checkpointStore = new MySqlSourceCheckpointStore(stateStore);
      MySqlTransactionStreamingSession session =
          new MySqlTransactionStreamingSession(
              "sourceA",
              List.of(schema),
              new MySqlTransactionStream() {
                private boolean returned;

                @Override
                public Optional<MySqlBinlogTransaction> readPendingTransaction() {
                  if (returned) {
                    return Optional.empty();
                  }
                  returned = true;
                  return Optional.of(transaction);
                }

                @Override
                public SourceFlowControlSnapshot sourceFlowControlSnapshot() {
                  return SourceFlowControlSnapshot.directPoll();
                }
              },
              checkpointStore);

      assertThat(session.readPendingTransaction()).contains(transaction);
      session.acknowledge(transaction);
      assertThat(checkpointStore.load("sourceA")).contains(transaction.checkpointPosition());
      assertThat(session.currentCapturedSchemas()).containsExactly(schema);
      assertThat(session.sourceFlowControlSnapshot().mode())
          .isEqualTo(SourceFlowControlSnapshot.Mode.DIRECT_POLL);
    }
  }

  @Test
  void propagatesStreamSqlExceptionForRuntimeRetry() throws Exception {
    TableSchema schema =
        TableSchema.create(
            new TableId("sourceA", "appdb", "widgets"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-11T00:00:00Z"));
    SQLException streamFailure =
        new SQLTransientConnectionException("MySQL binlog stream disconnected unexpectedly");

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("mysql-stream-sql-failure"))) {
      MySqlTransactionStreamingSession session =
          new MySqlTransactionStreamingSession(
              "sourceA",
              List.of(schema),
              new MySqlTransactionStream() {
                @Override
                public Optional<MySqlBinlogTransaction> readPendingTransaction()
                    throws SQLException {
                  throw streamFailure;
                }
              },
              new MySqlSourceCheckpointStore(stateStore));

      Throwable failure = catchThrowable(session::readPendingTransaction);

      assertThat(failure).isSameAs(streamFailure);
      assertThat(stateStore.schemas().loadFullDumpRequiredSignals()).isEmpty();
      assertThat(stateStore.schemas().loadSchemaUncertaintySignals()).isEmpty();
    }
  }

  @Test
  void recordsFullDumpRequiredSignalWhenBinlogFailsClosedOnDestructiveDdl()
      throws Exception {
    TableSchema schema =
        TableSchema.create(
            new TableId("sourceA", "appdb", "widgets"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-11T00:00:00Z"));
    MySqlBinlogSession binlogSession =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "sourceA",
            List.of(schema),
            new StubBinlogStream(
                List.of(
                    new MySqlBinlogMessage.Query(
                        "appdb",
                        "TRUNCATE TABLE `appdb`.`widgets`",
                        new MySqlSourcePosition("mysql-bin.000001", 88L, null),
                        Instant.parse("2026-04-11T00:00:01Z")))));

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("mysql-destructive-ddl-full-dump-signal"))) {
      MySqlSourceCheckpointStore checkpointStore = new MySqlSourceCheckpointStore(stateStore);
      MySqlTransactionStreamingSession session =
          new MySqlTransactionStreamingSession(
              "sourceA", List.of(schema), binlogSession, checkpointStore);

      Throwable failure = catchThrowable(session::readPendingTransaction);

      assertThat(failure).isInstanceOf(IllegalStateException.class);
      assertThat(stateStore.schemas().loadFullDumpRequiredSignals())
          .singleElement()
          .satisfies(
              signal -> {
                assertThat(signal.sourceId()).isEqualTo("sourceA");
                assertThat(signal.tableId()).isNull();
                assertThat(signal.reason()).contains("full dump required");
                assertThat(signal.reason()).contains("TRUNCATE TABLE");
              });
      assertThat(checkpointStore.load("sourceA")).isEmpty();
    }
  }

  @Test
  void preservesOriginalFullDumpRequiredSignalWhenSignalPersistenceFails()
      throws Exception {
    TableSchema schema =
        TableSchema.create(
            new TableId("sourceA", "appdb", "widgets"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-11T00:00:00Z"));
    MySqlBinlogSession binlogSession =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "sourceA",
            List.of(schema),
            new StubBinlogStream(
                List.of(
                    new MySqlBinlogMessage.Query(
                        "appdb",
                        "TRUNCATE TABLE `appdb`.`widgets`",
                        new MySqlSourcePosition("mysql-bin.000001", 89L, null),
                        Instant.parse("2026-04-11T00:00:01Z")))));

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("mysql-full-dump-signal-save-failure"))) {
      RuntimeException persistenceFailure =
          new IllegalStateException("injected schema signal write failure");
      MySqlSourceCheckpointStore checkpointStore =
          new MySqlSourceCheckpointStore(
              ThrowingSchemaSignalRuntimeStateStore.failFullDumpRequiredSignal(
                  stateStore, persistenceFailure));
      MySqlTransactionStreamingSession session =
          new MySqlTransactionStreamingSession(
              "sourceA", List.of(schema), binlogSession, checkpointStore);

      Throwable failure = catchThrowable(session::readPendingTransaction);

      assertThat(failure)
          .isInstanceOf(SourceRequiresFullDumpException.class)
          .hasMessageContaining("full dump required")
          .hasMessageContaining("TRUNCATE TABLE");
      assertThat(failure.getSuppressed()).containsExactly(persistenceFailure);
      assertThat(stateStore.schemas().loadFullDumpRequiredSignals()).isEmpty();
      assertThat(checkpointStore.load("sourceA")).isEmpty();
    }
  }

  @Test
  void recordsFullDumpRequiredSignalWhenTableMapDropsCapturedColumn()
      throws Exception {
    TableSchema schema =
        TableSchema.create(
            new TableId("sourceA", "appdb", "widgets"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-11T00:00:00Z"));
    MySqlBinlogSession binlogSession =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "sourceA",
            List.of(schema),
            new StubBinlogStream(
                List.of(
                    new MySqlBinlogMessage.TableMap(
                        7L,
                        "appdb",
                        "widgets",
                        null,
                        null,
                        null,
                        List.of("id"),
                        List.of(0),
                        null,
                        List.of(),
                        new MySqlSourcePosition("mysql-bin.000001", 90L, null),
                        Instant.parse("2026-04-11T00:00:01Z")),
                    new MySqlBinlogMessage.WriteRows(
                        7L,
                        List.<Object[]>of(new Object[] {1L}),
                        new MySqlSourcePosition("mysql-bin.000001", 91L, null),
                        Instant.parse("2026-04-11T00:00:02Z")),
                    new MySqlBinlogMessage.Commit(
                        "xid-table-map-drift",
                        new MySqlSourcePosition("mysql-bin.000001", 92L, null),
                        Instant.parse("2026-04-11T00:00:03Z")))));

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("mysql-table-map-column-full-dump-signal"))) {
      MySqlSourceCheckpointStore checkpointStore = new MySqlSourceCheckpointStore(stateStore);
      MySqlTransactionStreamingSession session =
          new MySqlTransactionStreamingSession(
              "sourceA", List.of(schema), binlogSession, checkpointStore);

      Throwable failure = catchThrowable(session::readPendingTransaction);

      assertThat(failure)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("table-map columns")
          .hasMessageContaining("missing name");
      assertThat(stateStore.schemas().loadFullDumpRequiredSignals())
          .singleElement()
          .satisfies(
              signal -> {
                assertThat(signal.sourceId()).isEqualTo("sourceA");
                assertThat(signal.tableId()).isEqualTo(schema.tableId());
                assertThat(signal.reason()).contains("full dump required");
                assertThat(signal.reason()).contains("missing name");
              });
      assertThat(checkpointStore.load("sourceA")).isEmpty();
    }
  }

  @Test
  void recordsFullDumpRequiredSignalWhenWriteRowTupleIsShortAfterValidTableMap()
      throws Exception {
    TableSchema schema =
        TableSchema.create(
            new TableId("sourceA", "appdb", "widgets"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-11T00:00:00Z"));
    MySqlBinlogSession binlogSession =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "sourceA",
            List.of(schema),
            new StubBinlogStream(
                List.of(
                    new MySqlBinlogMessage.TableMap(
                        7L,
                        "appdb",
                        "widgets",
                        null,
                        null,
                        null,
                        List.of("id", "name"),
                        List.of(0),
                        null,
                        List.of(),
                        new MySqlSourcePosition("mysql-bin.000001", 110L, null),
                        Instant.parse("2026-04-11T00:00:01Z")),
                    new MySqlBinlogMessage.WriteRows(
                        7L,
                        List.<Object[]>of(new Object[] {1L}),
                        new MySqlSourcePosition("mysql-bin.000001", 111L, null),
                        Instant.parse("2026-04-11T00:00:02Z")),
                    new MySqlBinlogMessage.Commit(
                        "xid-short-valid-table-map",
                        new MySqlSourcePosition("mysql-bin.000001", 112L, null),
                        Instant.parse("2026-04-11T00:00:03Z")))));

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("mysql-short-row-valid-table-map"))) {
      MySqlSourceCheckpointStore checkpointStore = new MySqlSourceCheckpointStore(stateStore);
      MySqlTransactionStreamingSession session =
          new MySqlTransactionStreamingSession(
              "sourceA", List.of(schema), binlogSession, checkpointStore);

      Throwable failure = catchThrowable(session::readPendingTransaction);

      assertThat(failure)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("row tuple")
          .hasMessageContaining("full dump required");
      assertThat(stateStore.schemas().loadFullDumpRequiredSignals())
          .singleElement()
          .satisfies(
              signal -> {
                assertThat(signal.sourceId()).isEqualTo("sourceA");
                assertThat(signal.tableId()).isEqualTo(schema.tableId());
                assertThat(signal.reason()).contains("full dump required");
                assertThat(signal.reason()).contains("row tuple");
              });
      assertThat(checkpointStore.load("sourceA")).isEmpty();
    }
  }

  @Test
  void recordsFullDumpRequiredSignalWhenWriteRowTupleIsShortAfterTableMapMetadataFallback()
      throws Exception {
    TableSchema schema =
        TableSchema.create(
            new TableId("sourceA", "appdb", "widgets"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-11T00:00:00Z"));
    MySqlBinlogSession binlogSession =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "sourceA",
            List.of(schema),
            new StubBinlogStream(
                List.of(
                    new MySqlBinlogMessage.TableMap(
                        7L,
                        "appdb",
                        "widgets",
                        null,
                        null,
                        null,
                        List.of(),
                        List.of(0),
                        null,
                        List.of(),
                        new MySqlSourcePosition("mysql-bin.000001", 120L, null),
                        Instant.parse("2026-04-11T00:00:01Z")),
                    new MySqlBinlogMessage.WriteRows(
                        7L,
                        List.<Object[]>of(new Object[] {1L}),
                        new MySqlSourcePosition("mysql-bin.000001", 121L, null),
                        Instant.parse("2026-04-11T00:00:02Z")),
                    new MySqlBinlogMessage.Commit(
                        "xid-short-fallback-table-map",
                        new MySqlSourcePosition("mysql-bin.000001", 122L, null),
                        Instant.parse("2026-04-11T00:00:03Z")))));

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("mysql-short-row-fallback-table-map"))) {
      MySqlSourceCheckpointStore checkpointStore = new MySqlSourceCheckpointStore(stateStore);
      MySqlTransactionStreamingSession session =
          new MySqlTransactionStreamingSession(
              "sourceA", List.of(schema), binlogSession, checkpointStore);

      Throwable failure = catchThrowable(session::readPendingTransaction);

      assertThat(failure)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("row tuple")
          .hasMessageContaining("full dump required");
      assertThat(stateStore.schemas().loadFullDumpRequiredSignals())
          .singleElement()
          .satisfies(
              signal -> {
                assertThat(signal.sourceId()).isEqualTo("sourceA");
                assertThat(signal.tableId()).isEqualTo(schema.tableId());
                assertThat(signal.reason()).contains("full dump required");
                assertThat(signal.reason()).contains("row tuple");
              });
      assertThat(checkpointStore.load("sourceA")).isEmpty();
    }
  }

  @Test
  void recordsFullDumpRequiredSignalWhenPrimaryKeyUpdateIsObserved()
      throws Exception {
    TableSchema schema =
        TableSchema.create(
            new TableId("sourceA", "appdb", "widgets"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-11T00:00:00Z"));
    MySqlBinlogSession binlogSession =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "sourceA",
            List.of(schema),
            new StubBinlogStream(
                List.of(
                    new MySqlBinlogMessage.TableMap(
                        7L,
                        "appdb",
                        "widgets",
                        null,
                        null,
                        null,
                        List.of("id", "name"),
                        List.of(0),
                        null,
                        List.of(),
                        new MySqlSourcePosition("mysql-bin.000001", 100L, null),
                        Instant.parse("2026-04-11T00:00:01Z")),
                    new MySqlBinlogMessage.UpdateRows(
                        7L,
                        List.of(
                            new MySqlBinlogMessage.RowChange(
                                new Object[] {1L, "one"}, new Object[] {2L, "two"})),
                        new MySqlSourcePosition("mysql-bin.000001", 101L, null),
                        Instant.parse("2026-04-11T00:00:02Z")),
                    new MySqlBinlogMessage.Commit(
                        "xid-primary-key-update",
                        new MySqlSourcePosition("mysql-bin.000001", 102L, null),
                        Instant.parse("2026-04-11T00:00:03Z")))));

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("mysql-primary-key-update-full-dump-signal"))) {
      MySqlSourceCheckpointStore checkpointStore = new MySqlSourceCheckpointStore(stateStore);
      MySqlTransactionStreamingSession session =
          new MySqlTransactionStreamingSession(
              "sourceA", List.of(schema), binlogSession, checkpointStore);

      Throwable failure = catchThrowable(session::readPendingTransaction);

      assertThat(failure)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Primary-key update")
          .hasMessageContaining("widgets");
      assertThat(stateStore.schemas().loadFullDumpRequiredSignals())
          .singleElement()
          .satisfies(
              signal -> {
                assertThat(signal.sourceId()).isEqualTo("sourceA");
                assertThat(signal.tableId()).isEqualTo(schema.tableId());
                assertThat(signal.reason()).contains("full dump required");
                assertThat(signal.reason()).contains("primary-key update");
              });
      assertThat(checkpointStore.load("sourceA")).isEmpty();
    }
  }

  @Test
  void recordsSchemaUncertaintySignalWhenWatermarkMetadataRowIsMalformed()
      throws Exception {
    TableSchema schema =
        TableSchema.create(
            new TableId("sourceA", "appdb", "widgets"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-11T00:00:00Z"));
    MySqlBinlogSession binlogSession =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "sourceA",
            List.of(schema),
            new StubBinlogStream(
                List.of(
                    new MySqlBinlogMessage.TableMap(
                        99L,
                        "dblog_meta",
                        "watermarks",
                        new MySqlSourcePosition("mysql-bin.000001", 130L, null),
                        Instant.parse("2026-04-11T00:00:01Z")),
                    new MySqlBinlogMessage.WriteRows(
                        99L,
                        List.<Object[]>of(new Object[] {1L, "test-run", null}),
                        new MySqlSourcePosition("mysql-bin.000001", 131L, null),
                        Instant.parse("2026-04-11T00:00:02Z")))));

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("mysql-malformed-watermark-diagnostic"))) {
      MySqlSourceCheckpointStore checkpointStore = new MySqlSourceCheckpointStore(stateStore);
      MySqlTransactionStreamingSession session =
          new MySqlTransactionStreamingSession(
              "sourceA", List.of(schema), binlogSession, checkpointStore);

      Throwable failure = catchThrowable(session::readPendingTransaction);

      assertThat(failure)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("watermark metadata row");
      assertThat(stateStore.schemas().loadSchemaUncertaintySignals())
          .singleElement()
          .satisfies(
              signal -> {
                assertThat(signal.sourceId()).isEqualTo("sourceA");
                assertThat(signal.tableId()).isEqualTo(WatermarkMetadata.tableIdFor("sourceA"));
                assertThat(signal.reason()).contains("watermark metadata row");
              });
      assertThat(checkpointStore.load("sourceA")).isEmpty();
    }
  }

  @Test
  void preservesSchemaUncertaintyCauseWhenWatermarkMetadataRowShapeIsMalformed()
      throws Exception {
    TableSchema schema =
        TableSchema.create(
            new TableId("sourceA", "appdb", "widgets"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-11T00:00:00Z"));
    MySqlBinlogSession binlogSession =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "sourceA",
            List.of(schema),
            new StubBinlogStream(
                List.of(
                    new MySqlBinlogMessage.TableMap(
                        99L,
                        "dblog_meta",
                        "watermarks",
                        new MySqlSourcePosition("mysql-bin.000001", 132L, null),
                        Instant.parse("2026-04-11T00:00:01Z")),
                    new MySqlBinlogMessage.WriteRows(
                        99L,
                        List.<Object[]>of(new Object[] {1L}),
                        new MySqlSourcePosition("mysql-bin.000001", 133L, null),
                        Instant.parse("2026-04-11T00:00:02Z")))));

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("mysql-short-watermark-diagnostic"))) {
      MySqlSourceCheckpointStore checkpointStore = new MySqlSourceCheckpointStore(stateStore);
      MySqlTransactionStreamingSession session =
          new MySqlTransactionStreamingSession(
              "sourceA", List.of(schema), binlogSession, checkpointStore);

      Throwable failure = catchThrowable(session::readPendingTransaction);

      assertThat(failure)
          .isInstanceOf(SourceSchemaUncertaintyException.class)
          .hasMessageContaining("expected singleton columns")
          .hasCauseInstanceOf(IllegalStateException.class);
      assertThat(failure.getCause()).hasMessageContaining("expected singleton columns");
      assertThat(stateStore.schemas().loadSchemaUncertaintySignals())
          .singleElement()
          .satisfies(
              signal -> {
                assertThat(signal.sourceId()).isEqualTo("sourceA");
                assertThat(signal.tableId()).isEqualTo(WatermarkMetadata.tableIdFor("sourceA"));
                assertThat(signal.reason()).contains("expected singleton columns");
              });
      assertThat(checkpointStore.load("sourceA")).isEmpty();
    }
  }

  @Test
  void preservesOriginalSchemaUncertaintySignalWhenSignalPersistenceFails()
      throws Exception {
    TableSchema schema =
        TableSchema.create(
            new TableId("sourceA", "appdb", "widgets"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-11T00:00:00Z"));
    MySqlBinlogSession binlogSession =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "sourceA",
            List.of(schema),
            new StubBinlogStream(
                List.of(
                    new MySqlBinlogMessage.TableMap(
                        99L,
                        "dblog_meta",
                        "watermarks",
                        new MySqlSourcePosition("mysql-bin.000001", 134L, null),
                        Instant.parse("2026-04-11T00:00:01Z")),
                    new MySqlBinlogMessage.WriteRows(
                        99L,
                        List.<Object[]>of(new Object[] {1L}),
                        new MySqlSourcePosition("mysql-bin.000001", 135L, null),
                        Instant.parse("2026-04-11T00:00:02Z")))));

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("mysql-schema-signal-save-failure"))) {
      RuntimeException persistenceFailure =
          new IllegalStateException("injected schema uncertainty write failure");
      MySqlSourceCheckpointStore checkpointStore =
          new MySqlSourceCheckpointStore(
              ThrowingSchemaSignalRuntimeStateStore.failSchemaUncertaintySignal(
                  stateStore, persistenceFailure));
      MySqlTransactionStreamingSession session =
          new MySqlTransactionStreamingSession(
              "sourceA", List.of(schema), binlogSession, checkpointStore);

      Throwable failure = catchThrowable(session::readPendingTransaction);

      assertThat(failure)
          .isInstanceOf(SourceSchemaUncertaintyException.class)
          .hasMessageContaining("expected singleton columns")
          .hasCauseInstanceOf(IllegalStateException.class);
      assertThat(failure.getSuppressed()).containsExactly(persistenceFailure);
      assertThat(stateStore.schemas().loadSchemaUncertaintySignals()).isEmpty();
      assertThat(checkpointStore.load("sourceA")).isEmpty();
    }
  }

  @Test
  void recordsSchemaUncertaintySignalWhenForeignHeartbeatConflictsOnSameSourceStream()
      throws Exception {
    TableSchema schema =
        TableSchema.create(
            new TableId("sourceA", "appdb", "widgets"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-11T00:00:00Z"));
    Instant ownBeat = Instant.parse("2026-04-11T00:01:00Z");
    Instant foreignBeat = ownBeat.plusSeconds(60);
    MySqlBinlogSession binlogSession =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "sourceA",
            List.of(schema),
            new StubBinlogStream(
                List.of(
                    new MySqlBinlogMessage.TableMap(
                        100L,
                        "dblog_meta",
                        "heartbeats",
                        new MySqlSourcePosition("mysql-bin.000001", 140L, null),
                        Instant.parse("2026-04-11T00:00:01Z")),
                    new MySqlBinlogMessage.UpdateRows(
                        100L,
                        List.of(
                            new MySqlBinlogMessage.RowChange(
                                new Object[] {1L, null, null, null},
                                new Object[] {
                                  1L, "test-run", "internal-stream", Timestamp.from(ownBeat)
                                }),
                            new MySqlBinlogMessage.RowChange(
                                new Object[] {
                                  1L, "test-run", "internal-stream", Timestamp.from(ownBeat)
                                },
                                new Object[] {
                                  1L, "other-run", "internal-stream", Timestamp.from(foreignBeat)
                                })),
                        new MySqlSourcePosition("mysql-bin.000001", 141L, null),
                        Instant.parse("2026-04-11T00:00:02Z")))));

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("mysql-foreign-heartbeat-diagnostic"))) {
      MySqlSourceCheckpointStore checkpointStore = new MySqlSourceCheckpointStore(stateStore);
      MySqlTransactionStreamingSession session =
          new MySqlTransactionStreamingSession(
              "sourceA", List.of(schema), binlogSession, checkpointStore);

      Throwable failure = catchThrowable(session::readPendingTransaction);

      assertThat(failure)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("same source stream");
      assertThat(stateStore.schemas().loadSchemaUncertaintySignals())
          .singleElement()
          .satisfies(
              signal -> {
                assertThat(signal.sourceId()).isEqualTo("sourceA");
                assertThat(signal.tableId()).isEqualTo(HeartbeatMetadata.tableIdFor("sourceA"));
                assertThat(signal.reason()).contains("same source stream");
              });
      assertThat(checkpointStore.load("sourceA")).isEmpty();
    }
  }

  private static final class StubBinlogStream implements MySqlBinlogStream {
    private final Deque<MySqlBinlogMessage> messages;

    private StubBinlogStream(List<MySqlBinlogMessage> messages) {
      this.messages = new ArrayDeque<>(messages);
    }

    @Override
    public Optional<MySqlBinlogMessage> readMessage() throws SQLException {
      return Optional.ofNullable(messages.pollFirst());
    }

    @Override
    public void close() {}
  }
}
