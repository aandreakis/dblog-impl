package io.github.aandreakis.dblog.adapter.postgres.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.github.aandreakis.dblog.adapter.api.SourceRequiresFullDumpException;
import io.github.aandreakis.dblog.adapter.api.SourceSchemaUncertaintyException;
import io.github.aandreakis.dblog.adapter.postgres.PostgresLsn;
import io.github.aandreakis.dblog.adapter.postgres.PostgresPgoutputTransaction;
import io.github.aandreakis.dblog.adapter.postgres.PostgresSourceCheckpointStore;
import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.reconcile.HeartbeatMetadata;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import io.github.aandreakis.dblog.testsupport.ThrowingSchemaSignalRuntimeStateStore;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PostgresTransactionStreamingSessionTests {
  private static final Instant TX_TIME = Instant.parse("2026-03-20T00:00:00Z");

  @TempDir Path tempDir;

  @Test
  void decodesCommittedUserTableTransactionAndAdvancesCheckpointWhenAcknowledged()
      throws Exception {
    StubStream stream =
        new StubStream(
            List.of(
                relation(
                    7,
                    "public",
                    "widgets",
                    'f',
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("id", true, 20),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("name", false, 25)),
                begin("0/16DA010", TX_TIME, 42),
                insert(7, "1", "one"),
                update(7, tuple("1", "one"), tuple("1", "two")),
                delete(7, tuple("1", "two")),
                commit("0/16DA018", "0/16DA020", TX_TIME)),
            java.util.Arrays.asList(
                null, null, null, null, null, PostgresLsn.parse("0/16DA020")));
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("next-pgoutput-checkpoint"))) {
      PostgresSourceCheckpointStore checkpointStore = new PostgresSourceCheckpointStore(stateStore);
      PostgresTransactionStreamingSession session =
          new PostgresTransactionStreamingSession(
              "appdb", "test-run", "internal-stream", "postgres-source", List.of(widgetSchema()), stream, checkpointStore);

      PostgresPgoutputTransaction transaction = session.readPendingTransaction().orElseThrow();

      assertThat(transaction.transactionId()).isEqualTo("42");
      assertThat(transaction.beginFinalLsn()).isEqualTo(PostgresLsn.parse("0/16DA010"));
      assertThat(transaction.commitLsn()).isEqualTo(PostgresLsn.parse("0/16DA018"));
      assertThat(transaction.endLsn()).isEqualTo(PostgresLsn.parse("0/16DA020"));
      assertThat(transaction.checkpointLsn()).isEqualTo(PostgresLsn.parse("0/16DA020"));
      assertThat(transaction.events()).hasSize(3);
      assertThat(transaction.events())
          .extracting(event -> event.operationType().name())
          .containsExactly("INSERT", "UPDATE", "DELETE");
      assertThat(transaction.events())
          .extracting(event -> event.captureOrigin().name())
          .containsOnly("LOG");
      assertThat(transaction.events().get(0).afterRow().asMap()).containsEntry("id", 1L).containsEntry("name", "one");
      assertThat(transaction.events().get(1).beforeRow().asMap()).containsEntry("name", "one");
      assertThat(transaction.events().get(1).afterRow().asMap()).containsEntry("name", "two");
      assertThat(transaction.events().get(2).beforeRow().asMap()).containsEntry("name", "two");

      session.acknowledge(transaction);

      assertThat(checkpointStore.load("postgres-source")).contains(PostgresLsn.parse("0/16DA020"));
      assertThat(stream.appliedLsn()).isEqualTo(PostgresLsn.parse("0/16DA020"));
      assertThat(stream.flushedLsn()).isEqualTo(PostgresLsn.parse("0/16DA020"));
    }
  }

  @Test
  void propagatesStreamSqlExceptionForRuntimeRetry() throws Exception {
    SQLException streamFailure =
        new SQLTransientConnectionException("PostgreSQL pgoutput stream disconnected");
    PostgresPgoutputStream stream =
        new PostgresPgoutputStream() {
          @Override
          public Optional<ByteBuffer> readPending() throws SQLException {
            throw streamFailure;
          }

          @Override
          public Optional<PostgresLsn> lastReceiveLsn() {
            return Optional.empty();
          }

          @Override
          public void setAppliedLsn(PostgresLsn lsn) {}

          @Override
          public void setFlushedLsn(PostgresLsn lsn) {}

          @Override
          public void forceUpdateStatus() {}

          @Override
          public void close() {}
        };

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("next-pgoutput-stream-sql-failure"))) {
      PostgresTransactionStreamingSession session =
          new PostgresTransactionStreamingSession(
              "appdb",
              "test-run",
              "internal-stream",
              "postgres-source",
              List.of(widgetSchema()),
              stream,
              new PostgresSourceCheckpointStore(stateStore));

      Throwable failure = catchThrowable(session::readPendingTransaction);

      assertThat(failure).isSameAs(streamFailure);
      assertThat(stateStore.schemas().loadFullDumpRequiredSignals()).isEmpty();
      assertThat(stateStore.schemas().loadSchemaUncertaintySignals()).isEmpty();
    }
  }

  @Test
  void ignoresExtraRelationColumnsWhenSelectedContractStillMatches() throws Exception {
    StubStream stream =
        new StubStream(
            List.of(
                relation(
                    7,
                    "public",
                    "widgets",
                    'f',
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("id", true, 20),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("name", false, 25),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("note", false, 25)),
                begin("0/16DA010", TX_TIME, 42),
                insert(7, "1", "one", "ignored by contract"),
                commit("0/16DA018", "0/16DA020", TX_TIME)),
            java.util.Arrays.asList(null, null, null, PostgresLsn.parse("0/16DA020")));
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("next-pgoutput-extra-relation-column"))) {
      PostgresTransactionStreamingSession session =
          new PostgresTransactionStreamingSession(
              "appdb",
              "test-run",
              "internal-stream",
              "postgres-source",
              List.of(widgetSchema()),
              stream,
              new PostgresSourceCheckpointStore(stateStore));

      PostgresPgoutputTransaction transaction = session.readPendingTransaction().orElseThrow();

      assertThat(transaction.events()).hasSize(1);
      assertThat(transaction.events().getFirst().primaryKey().asMap())
          .containsExactlyEntriesOf(java.util.Map.of("id", 1L));
      assertThat(transaction.events().getFirst().afterRow().asMap())
          .containsEntry("id", 1L)
          .containsEntry("name", "one")
          .doesNotContainKey("note");
      assertThat(stateStore.schemas().loadFullDumpRequiredSignals()).isEmpty();
    }
  }

  @Test
  void documentsLiveRelationTypeOidChangesAreNotRuntimeSchemaDriftDetection()
      throws Exception {
    // Startup/restart schema inspection catches selected-column type drift. The live pgoutput
    // guard only maps by selected column name, so relation type OID drift is not a runtime
    // full-dump trigger by itself.
    StubStream stream =
        new StubStream(
            List.of(
                relation(
                    7,
                    "public",
                    "widgets",
                    'f',
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("id", true, 20),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("name", false, 20)),
                begin("0/16DA010", TX_TIME, 42),
                insert(7, "1", "123"),
                commit("0/16DA018", "0/16DA020", TX_TIME)),
            java.util.Arrays.asList(null, null, null, PostgresLsn.parse("0/16DA020")));
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("next-pgoutput-relation-type-oid-drift"))) {
      PostgresTransactionStreamingSession session =
          new PostgresTransactionStreamingSession(
              "appdb",
              "test-run",
              "internal-stream",
              "postgres-source",
              List.of(widgetSchema()),
              stream,
              new PostgresSourceCheckpointStore(stateStore));

      PostgresPgoutputTransaction transaction = session.readPendingTransaction().orElseThrow();

      assertThat(transaction.events()).hasSize(1);
      assertThat(transaction.events().getFirst().afterRow().asMap())
          .containsEntry("id", 1L)
          .containsEntry("name", "123");
      assertThat(stateStore.schemas().loadFullDumpRequiredSignals()).isEmpty();
    }
  }

  @Test
  void preservesNullableUserColumnValuesWhenDecodingCommittedTransactions() throws Exception {
    StubStream stream =
        new StubStream(
            List.of(
                relation(
                    7,
                    "public",
                    "widgets",
                    'f',
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("id", true, 20),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("name", false, 25)),
                begin("0/16DA010", TX_TIME, 42),
                insert(7, "1", null),
                commit("0/16DA018", "0/16DA020", TX_TIME)),
            java.util.Arrays.asList(null, null, null, PostgresLsn.parse("0/16DA020")));
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("next-pgoutput-nullable-values"))) {
      PostgresTransactionStreamingSession session =
          new PostgresTransactionStreamingSession(
              "appdb",
              "test-run",
              "internal-stream",
              "postgres-source",
              List.of(widgetSchema()),
              stream,
              new PostgresSourceCheckpointStore(stateStore));

      PostgresPgoutputTransaction transaction = session.readPendingTransaction().orElseThrow();

      assertThat(transaction.events()).hasSize(1);
      assertThat(transaction.events().getFirst().afterRow().asMap())
          .containsEntry("id", 1L)
          .containsEntry("name", null);
    }
  }

  @Test
  void surfacesMetadataTableUpdatesAsInternalWatermarkAndHeartbeatEvents() throws Exception {
    Instant beat = Instant.parse("2026-03-20T00:05:00Z");
    StubStream stream =
        new StubStream(
            List.of(
                relation(
                    99,
                    "dblog_meta",
                    "watermarks",
                    'd',
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("id", true, 20),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("run_id", false, 25),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("token", false, 25)),
                relation(
                    100,
                    "dblog_meta",
                    "heartbeats",
                    'd',
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("id", true, 20),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("run_id", false, 25),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("source_stream_id", false, 25),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("last_beat_at", false, 1184)),
                begin("0/31", TX_TIME, 78),
                update(99, tuple("1", null, null), tuple("1", "test-run", "lw-1")),
                update(
                    100,
                    tuple("1", null, null, null),
                    tuple("1", "test-run", "internal-stream", "2026-03-20T00:05:00Z")),
                commit("0/38", "0/40", TX_TIME)),
            java.util.Arrays.asList(null, null, null, null, null, PostgresLsn.parse("0/40")));
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("next-pgoutput-metadata"))) {
      PostgresTransactionStreamingSession session =
          new PostgresTransactionStreamingSession(
              "appdb",
              "test-run",
              "internal-stream",
              "postgres-source",
              List.of(),
              stream,
              new PostgresSourceCheckpointStore(stateStore));

      PostgresPgoutputTransaction transaction = session.readPendingTransaction().orElseThrow();

      assertThat(transaction.events()).hasSize(2);
      assertThat(transaction.events().get(0).tableId())
          .isEqualTo(new TableId("appdb", "dblog_meta", "watermarks"));
      assertThat(transaction.events().get(0).operationType()).isEqualTo(OperationType.WATERMARK);
      assertThat(transaction.events().get(0).captureOrigin()).isEqualTo(CaptureOrigin.LOG);
      assertThat(transaction.events().get(0).afterRow().asMap()).containsEntry("token", "lw-1");

      assertThat(transaction.events().get(1).tableId())
          .isEqualTo(new TableId("appdb", "dblog_meta", "heartbeats"));
      assertThat(transaction.events().get(1).operationType()).isEqualTo(OperationType.HEARTBEAT);
      assertThat(transaction.events().get(1).captureOrigin()).isEqualTo(CaptureOrigin.LOG);
      assertThat(transaction.events().get(1).afterRow().asMap()).containsEntry("last_beat_at", beat);
    }
  }

  @Test
  void failsClosedWhenCapturedUserRelationStopsUsingReplicaIdentityFull() throws Exception {
    StubStream stream =
        new StubStream(
            List.of(
                relation(
                    7,
                    "public",
                    "widgets",
                    'd',
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("id", true, 20),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("name", false, 25))),
            java.util.Arrays.asList((PostgresLsn) null));
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("next-pgoutput-replica-identity"))) {
      PostgresTransactionStreamingSession session =
          new PostgresTransactionStreamingSession(
              "appdb",
              "test-run",
              "internal-stream",
              "postgres-source",
              List.of(widgetSchema()),
              stream,
              new PostgresSourceCheckpointStore(stateStore));

      assertThatThrownBy(session::readPendingTransaction)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("REPLICA IDENTITY FULL")
          .hasMessageContaining("<state-path>.mv.db");
    }
  }

  @Test
  void recordsFullDumpRequiredSignalWhenCapturedRelationLosesReplicaIdentityFull()
      throws Exception {
    StubStream stream =
        new StubStream(
            List.of(
                relation(
                    7,
                    "public",
                    "widgets",
                    'd',
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("id", true, 20),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("name", false, 25))),
            java.util.Arrays.asList(PostgresLsn.parse("0/16DA010")));
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("next-pgoutput-replica-identity-full-dump-signal"))) {
      PostgresSourceCheckpointStore checkpointStore = new PostgresSourceCheckpointStore(stateStore);
      PostgresTransactionStreamingSession session =
          new PostgresTransactionStreamingSession(
              "appdb",
              "test-run",
              "internal-stream",
              "postgres-source",
              List.of(widgetSchema()),
              stream,
              checkpointStore);

      Throwable failure = catchThrowable(session::readPendingTransaction);

      assertThat(failure)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("REPLICA IDENTITY FULL");
      assertThat(stateStore.schemas().loadFullDumpRequiredSignals())
          .singleElement()
          .satisfies(
              signal -> {
                assertThat(signal.sourceId()).isEqualTo("postgres-source");
                assertThat(signal.tableId()).isEqualTo(widgetSchema().tableId());
                assertThat(signal.reason()).contains("full dump required");
                assertThat(signal.reason()).contains("REPLICA IDENTITY FULL");
              });
      assertThat(checkpointStore.load("postgres-source")).isEmpty();
      assertThat(stream.appliedLsn()).isNull();
      assertThat(stream.flushedLsn()).isNull();
    }
  }

  @Test
  void preservesOriginalFullDumpRequiredSignalWhenSignalPersistenceFails()
      throws Exception {
    StubStream stream =
        new StubStream(
            List.of(
                relation(
                    7,
                    "public",
                    "widgets",
                    'd',
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("id", true, 20),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("name", false, 25))),
            java.util.Arrays.asList(PostgresLsn.parse("0/16DA010")));
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("next-pgoutput-full-dump-signal-save-failure"))) {
      RuntimeException persistenceFailure =
          new IllegalStateException("injected full dump signal write failure");
      PostgresSourceCheckpointStore checkpointStore =
          new PostgresSourceCheckpointStore(
              ThrowingSchemaSignalRuntimeStateStore.failFullDumpRequiredSignal(
                  stateStore, persistenceFailure));
      PostgresTransactionStreamingSession session =
          new PostgresTransactionStreamingSession(
              "appdb",
              "test-run",
              "internal-stream",
              "postgres-source",
              List.of(widgetSchema()),
              stream,
              checkpointStore);

      Throwable failure = catchThrowable(session::readPendingTransaction);

      assertThat(failure)
          .isInstanceOf(SourceRequiresFullDumpException.class)
          .hasMessageContaining("REPLICA IDENTITY FULL")
          .hasMessageContaining("full dump required");
      assertThat(failure.getSuppressed()).containsExactly(persistenceFailure);
      assertThat(stateStore.schemas().loadFullDumpRequiredSignals()).isEmpty();
      assertThat(checkpointStore.load("postgres-source")).isEmpty();
      assertThat(stream.appliedLsn()).isNull();
      assertThat(stream.flushedLsn()).isNull();
    }
  }

  @Test
  void recordsFullDumpRequiredSignalWhenRelationMetadataDropsCapturedColumn()
      throws Exception {
    StubStream stream =
        new StubStream(
            List.of(
                relation(
                    7,
                    "public",
                    "widgets",
                    'f',
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("id", true, 20))),
            java.util.Arrays.asList(PostgresLsn.parse("0/16DA010")));
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("next-pgoutput-relation-column-full-dump-signal"))) {
      PostgresSourceCheckpointStore checkpointStore = new PostgresSourceCheckpointStore(stateStore);
      PostgresTransactionStreamingSession session =
          new PostgresTransactionStreamingSession(
              "appdb",
              "test-run",
              "internal-stream",
              "postgres-source",
              List.of(widgetSchema()),
              stream,
              checkpointStore);

      Throwable failure = catchThrowable(session::readPendingTransaction);

      assertThat(failure)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("pgoutput relation columns")
          .hasMessageContaining("missing name");
      assertThat(stateStore.schemas().loadFullDumpRequiredSignals())
          .singleElement()
          .satisfies(
              signal -> {
                assertThat(signal.sourceId()).isEqualTo("postgres-source");
                assertThat(signal.tableId()).isEqualTo(widgetSchema().tableId());
                assertThat(signal.reason()).contains("full dump required");
                assertThat(signal.reason()).contains("missing name");
              });
      assertThat(checkpointStore.load("postgres-source")).isEmpty();
      assertThat(stream.appliedLsn()).isNull();
      assertThat(stream.flushedLsn()).isNull();
    }
  }

  @Test
  void recordsFullDumpRequiredSignalWhenCapturedRelationIsTruncated() throws Exception {
    StubStream stream =
        new StubStream(
            List.of(
                relation(
                    7,
                    "public",
                    "widgets",
                    'f',
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("id", true, 20),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("name", false, 25)),
                begin("0/16DA010", TX_TIME, 42),
                truncate(0, 7),
                commit("0/16DA018", "0/16DA020", TX_TIME)),
            java.util.Arrays.asList(
                null, null, PostgresLsn.parse("0/16DA018"), PostgresLsn.parse("0/16DA020")));
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("next-pgoutput-truncate-full-dump-signal"))) {
      PostgresSourceCheckpointStore checkpointStore = new PostgresSourceCheckpointStore(stateStore);
      PostgresTransactionStreamingSession session =
          new PostgresTransactionStreamingSession(
              "appdb",
              "test-run",
              "internal-stream",
              "postgres-source",
              List.of(widgetSchema()),
              stream,
              checkpointStore);

      Throwable failure = catchThrowable(session::readPendingTransaction);

      assertThat(failure).isInstanceOf(IllegalStateException.class);
      assertThat(stateStore.schemas().loadFullDumpRequiredSignals())
          .singleElement()
          .satisfies(
              signal -> {
                assertThat(signal.sourceId()).isEqualTo("postgres-source");
                assertThat(signal.tableId()).isEqualTo(widgetSchema().tableId());
                assertThat(signal.reason()).contains("full dump required");
                assertThat(signal.reason()).contains("TRUNCATE");
              });
      assertThat(checkpointStore.load("postgres-source")).isEmpty();
      assertThat(stream.appliedLsn()).isNull();
      assertThat(stream.flushedLsn()).isNull();
    }
  }

  @Test
  void recordsFullDumpRequiredSignalWhenPrimaryKeyUpdateIsObserved()
      throws Exception {
    StubStream stream =
        new StubStream(
            List.of(
                relation(
                    7,
                    "public",
                    "widgets",
                    'f',
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("id", true, 20),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("name", false, 25)),
                begin("0/16DA010", TX_TIME, 42),
                update(7, tuple("1", "one"), tuple("2", "two")),
                commit("0/16DA018", "0/16DA020", TX_TIME)),
            java.util.Arrays.asList(
                null, null, PostgresLsn.parse("0/16DA018"), PostgresLsn.parse("0/16DA020")));
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("next-pgoutput-primary-key-update-full-dump-signal"))) {
      PostgresSourceCheckpointStore checkpointStore = new PostgresSourceCheckpointStore(stateStore);
      PostgresTransactionStreamingSession session =
          new PostgresTransactionStreamingSession(
              "appdb",
              "test-run",
              "internal-stream",
              "postgres-source",
              List.of(widgetSchema()),
              stream,
              checkpointStore);

      Throwable failure = catchThrowable(session::readPendingTransaction);

      assertThat(failure)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Primary-key update")
          .hasMessageContaining("widgets");
      assertThat(stateStore.schemas().loadFullDumpRequiredSignals())
          .singleElement()
          .satisfies(
              signal -> {
                assertThat(signal.sourceId()).isEqualTo("postgres-source");
                assertThat(signal.tableId()).isEqualTo(widgetSchema().tableId());
                assertThat(signal.reason()).contains("full dump required");
                assertThat(signal.reason()).contains("primary-key update");
              });
      assertThat(checkpointStore.load("postgres-source")).isEmpty();
      assertThat(stream.appliedLsn()).isNull();
      assertThat(stream.flushedLsn()).isNull();
    }
  }

  @Test
  void recordsFullDumpRequiredSignalWhenUpdateOldTupleIsKeyOnly() throws Exception {
    StubStream stream =
        new StubStream(
            List.of(
                relation(
                    7,
                    "public",
                    "widgets",
                    'f',
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("id", true, 20),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("name", false, 25)),
                begin("0/16DA010", TX_TIME, 42),
                updateWithKeyOldTuple(7, tuple("1"), tuple("1", "two")),
                commit("0/16DA018", "0/16DA020", TX_TIME)),
            java.util.Arrays.asList(
                null, null, PostgresLsn.parse("0/16DA018"), PostgresLsn.parse("0/16DA020")));
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("next-pgoutput-update-key-old-tuple"))) {
      PostgresSourceCheckpointStore checkpointStore = new PostgresSourceCheckpointStore(stateStore);
      PostgresTransactionStreamingSession session =
          new PostgresTransactionStreamingSession(
              "appdb",
              "test-run",
              "internal-stream",
              "postgres-source",
              List.of(widgetSchema()),
              stream,
              checkpointStore);

      Throwable failure = catchThrowable(session::readPendingTransaction);

      assertThat(failure)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("old tuple")
          .hasMessageContaining("full dump required");
      assertThat(stateStore.schemas().loadFullDumpRequiredSignals())
          .singleElement()
          .satisfies(
              signal -> {
                assertThat(signal.sourceId()).isEqualTo("postgres-source");
                assertThat(signal.tableId()).isEqualTo(widgetSchema().tableId());
                assertThat(signal.reason()).contains("full dump required");
                assertThat(signal.reason()).contains("old tuple");
              });
      assertThat(checkpointStore.load("postgres-source")).isEmpty();
      assertThat(stream.appliedLsn()).isNull();
      assertThat(stream.flushedLsn()).isNull();
    }
  }

  @Test
  void recordsFullDumpRequiredSignalWhenUpdateOldTupleIsMissing() throws Exception {
    StubStream stream =
        new StubStream(
            List.of(
                relation(
                    7,
                    "public",
                    "widgets",
                    'f',
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("id", true, 20),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("name", false, 25)),
                begin("0/16DA010", TX_TIME, 42),
                updateWithoutOldTuple(7, tuple("1", "two")),
                commit("0/16DA018", "0/16DA020", TX_TIME)),
            java.util.Arrays.asList(
                null, null, PostgresLsn.parse("0/16DA018"), PostgresLsn.parse("0/16DA020")));
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("next-pgoutput-update-missing-old-tuple"))) {
      PostgresSourceCheckpointStore checkpointStore = new PostgresSourceCheckpointStore(stateStore);
      PostgresTransactionStreamingSession session =
          new PostgresTransactionStreamingSession(
              "appdb",
              "test-run",
              "internal-stream",
              "postgres-source",
              List.of(widgetSchema()),
              stream,
              checkpointStore);

      Throwable failure = catchThrowable(session::readPendingTransaction);

      assertThat(failure)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("old tuple")
          .hasMessageContaining("full dump required");
      assertThat(stateStore.schemas().loadFullDumpRequiredSignals())
          .singleElement()
          .satisfies(
              signal -> {
                assertThat(signal.sourceId()).isEqualTo("postgres-source");
                assertThat(signal.tableId()).isEqualTo(widgetSchema().tableId());
                assertThat(signal.reason()).contains("full dump required");
                assertThat(signal.reason()).contains("old tuple");
              });
      assertThat(checkpointStore.load("postgres-source")).isEmpty();
      assertThat(stream.appliedLsn()).isNull();
      assertThat(stream.flushedLsn()).isNull();
    }
  }

  @Test
  void recordsFullDumpRequiredSignalWhenDeleteOldTupleIsKeyOnly() throws Exception {
    StubStream stream =
        new StubStream(
            List.of(
                relation(
                    7,
                    "public",
                    "widgets",
                    'f',
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("id", true, 20),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("name", false, 25)),
                begin("0/16DA010", TX_TIME, 42),
                deleteWithKeyOldTuple(7, tuple("1")),
                commit("0/16DA018", "0/16DA020", TX_TIME)),
            java.util.Arrays.asList(
                null, null, PostgresLsn.parse("0/16DA018"), PostgresLsn.parse("0/16DA020")));
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("next-pgoutput-delete-key-old-tuple"))) {
      PostgresSourceCheckpointStore checkpointStore = new PostgresSourceCheckpointStore(stateStore);
      PostgresTransactionStreamingSession session =
          new PostgresTransactionStreamingSession(
              "appdb",
              "test-run",
              "internal-stream",
              "postgres-source",
              List.of(widgetSchema()),
              stream,
              checkpointStore);

      Throwable failure = catchThrowable(session::readPendingTransaction);

      assertThat(failure)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("old tuple")
          .hasMessageContaining("full dump required");
      assertThat(stateStore.schemas().loadFullDumpRequiredSignals())
          .singleElement()
          .satisfies(
              signal -> {
                assertThat(signal.sourceId()).isEqualTo("postgres-source");
                assertThat(signal.tableId()).isEqualTo(widgetSchema().tableId());
                assertThat(signal.reason()).contains("full dump required");
                assertThat(signal.reason()).contains("old tuple");
              });
      assertThat(checkpointStore.load("postgres-source")).isEmpty();
      assertThat(stream.appliedLsn()).isNull();
      assertThat(stream.flushedLsn()).isNull();
    }
  }

  @Test
  void ignoresMetadataEventsFromAnotherRunId() throws Exception {
    StubStream stream =
        new StubStream(
            List.of(
                relation(
                    99,
                    "dblog_meta",
                    "watermarks",
                    'd',
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("id", true, 20),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("run_id", false, 25),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("token", false, 25)),
                begin("0/20", TX_TIME, 77),
                update(99, tuple("1", null, null), tuple("1", "other-run", "lw-1")),
                commit("0/28", "0/30", TX_TIME)),
            java.util.Arrays.asList(null, null, null, PostgresLsn.parse("0/30")));
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("next-pgoutput-foreign-run"))) {
      PostgresTransactionStreamingSession session =
          new PostgresTransactionStreamingSession(
              "appdb",
              "test-run",
              "internal-stream",
              "postgres-source",
              List.of(),
              stream,
              new PostgresSourceCheckpointStore(stateStore));

      assertThat(session.readPendingTransaction()).isEmpty();
    }
  }

  @Test
  void failsClosedWhenForeignHeartbeatAppearsOnSameSourceStreamAfterOwnHeartbeat()
      throws Exception {
    Instant ownBeat = Instant.parse("2026-03-20T00:05:00Z");
    Instant foreignBeat = ownBeat.plusSeconds(60);
    StubStream stream =
        new StubStream(
            List.of(
                relation(
                    100,
                    "dblog_meta",
                    "heartbeats",
                    'd',
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("id", true, 20),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("run_id", false, 25),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("source_stream_id", false, 25),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("last_beat_at", false, 1184)),
                begin("0/31", TX_TIME, 78),
                update(
                    100,
                    tuple("1", null, null, null),
                    tuple("1", "test-run", "internal-stream", "2026-03-20T00:05:00Z")),
                update(
                    100,
                    tuple("1", "test-run", "internal-stream", "2026-03-20T00:05:00Z"),
                    tuple("1", "other-run", "internal-stream", "2026-03-20T00:06:00Z")),
                commit("0/38", "0/40", TX_TIME)),
            java.util.Arrays.asList(null, null, null, null, PostgresLsn.parse("0/40")));
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("next-pgoutput-heartbeat-same-stream"))) {
      PostgresTransactionStreamingSession session =
          new PostgresTransactionStreamingSession(
              "appdb",
              "test-run",
              "internal-stream",
              "postgres-source",
              List.of(),
              stream,
              new PostgresSourceCheckpointStore(stateStore));

      assertThatThrownBy(session::readPendingTransaction)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("same source stream");
    }
  }

  @Test
  void recordsSchemaUncertaintySignalWhenHeartbeatMetadataTimestampIsMalformed()
      throws Exception {
    StubStream stream =
        new StubStream(
            List.of(
                relation(
                    100,
                    "dblog_meta",
                    "heartbeats",
                    'd',
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("id", true, 20),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("run_id", false, 25),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("source_stream_id", false, 25),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("last_beat_at", false, 1184)),
                begin("0/31", TX_TIME, 78),
                update(
                    100,
                    tuple("1", null, null, null),
                    tuple("1", "test-run", "internal-stream", "not-a-timestamp")),
                commit("0/38", "0/40", TX_TIME)),
            java.util.Arrays.asList(null, null, null, PostgresLsn.parse("0/40")));
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("next-pgoutput-heartbeat-malformed-diagnostic"))) {
      PostgresSourceCheckpointStore checkpointStore = new PostgresSourceCheckpointStore(stateStore);
      PostgresTransactionStreamingSession session =
          new PostgresTransactionStreamingSession(
              "appdb",
              "test-run",
              "internal-stream",
              "postgres-source",
              List.of(),
              stream,
              checkpointStore);

      Throwable failure = catchThrowable(session::readPendingTransaction);

      assertThat(failure)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("parseable heartbeat timestamp")
          .hasMessageContaining("not-a-timestamp")
          .hasCauseInstanceOf(DateTimeParseException.class);
      assertThat(stateStore.schemas().loadSchemaUncertaintySignals())
          .singleElement()
          .satisfies(
              signal -> {
                assertThat(signal.sourceId()).isEqualTo("postgres-source");
                assertThat(signal.tableId()).isEqualTo(HeartbeatMetadata.tableIdFor("appdb"));
                assertThat(signal.reason()).contains("parseable heartbeat timestamp");
                assertThat(signal.reason()).contains("not-a-timestamp");
              });
      assertThat(checkpointStore.load("postgres-source")).isEmpty();
      assertThat(stream.appliedLsn()).isNull();
      assertThat(stream.flushedLsn()).isNull();
    }
  }

  @Test
  void preservesOriginalSchemaUncertaintySignalWhenSignalPersistenceFails()
      throws Exception {
    StubStream stream =
        new StubStream(
            List.of(
                relation(
                    100,
                    "dblog_meta",
                    "heartbeats",
                    'd',
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("id", true, 20),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("run_id", false, 25),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("source_stream_id", false, 25),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("last_beat_at", false, 1184)),
                begin("0/31", TX_TIME, 78),
                update(
                    100,
                    tuple("1", null, null, null),
                    tuple("1", "test-run", "internal-stream", "not-a-timestamp")),
                commit("0/38", "0/40", TX_TIME)),
            java.util.Arrays.asList(null, null, null, PostgresLsn.parse("0/40")));
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("next-pgoutput-schema-signal-save-failure"))) {
      RuntimeException persistenceFailure =
          new IllegalStateException("injected schema uncertainty write failure");
      PostgresSourceCheckpointStore checkpointStore =
          new PostgresSourceCheckpointStore(
              ThrowingSchemaSignalRuntimeStateStore.failSchemaUncertaintySignal(
                  stateStore, persistenceFailure));
      PostgresTransactionStreamingSession session =
          new PostgresTransactionStreamingSession(
              "appdb",
              "test-run",
              "internal-stream",
              "postgres-source",
              List.of(),
              stream,
              checkpointStore);

      Throwable failure = catchThrowable(session::readPendingTransaction);

      assertThat(failure)
          .isInstanceOf(SourceSchemaUncertaintyException.class)
          .hasMessageContaining("parseable heartbeat timestamp")
          .hasMessageContaining("not-a-timestamp")
          .hasCauseInstanceOf(DateTimeParseException.class);
      assertThat(failure.getSuppressed()).containsExactly(persistenceFailure);
      assertThat(stateStore.schemas().loadSchemaUncertaintySignals()).isEmpty();
      assertThat(checkpointStore.load("postgres-source")).isEmpty();
      assertThat(stream.appliedLsn()).isNull();
      assertThat(stream.flushedLsn()).isNull();
    }
  }

  @Test
  void recordsSchemaUncertaintySignalWhenForeignHeartbeatConflictsOnSameSourceStream()
      throws Exception {
    Instant ownBeat = Instant.parse("2026-03-20T00:05:00Z");
    Instant foreignBeat = ownBeat.plusSeconds(60);
    StubStream stream =
        new StubStream(
            List.of(
                relation(
                    100,
                    "dblog_meta",
                    "heartbeats",
                    'd',
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("id", true, 20),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("run_id", false, 25),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("source_stream_id", false, 25),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("last_beat_at", false, 1184)),
                begin("0/31", TX_TIME, 78),
                update(
                    100,
                    tuple("1", null, null, null),
                    tuple("1", "test-run", "internal-stream", ownBeat.toString())),
                update(
                    100,
                    tuple("1", "test-run", "internal-stream", ownBeat.toString()),
                    tuple("1", "other-run", "internal-stream", foreignBeat.toString())),
                commit("0/38", "0/40", TX_TIME)),
            java.util.Arrays.asList(null, null, null, null, PostgresLsn.parse("0/40")));
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("next-pgoutput-heartbeat-conflict-diagnostic"))) {
      PostgresSourceCheckpointStore checkpointStore = new PostgresSourceCheckpointStore(stateStore);
      PostgresTransactionStreamingSession session =
          new PostgresTransactionStreamingSession(
              "appdb",
              "test-run",
              "internal-stream",
              "postgres-source",
              List.of(),
              stream,
              checkpointStore);

      Throwable failure = catchThrowable(session::readPendingTransaction);

      assertThat(failure)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("same source stream");
      assertThat(stateStore.schemas().loadSchemaUncertaintySignals())
          .singleElement()
          .satisfies(
              signal -> {
                assertThat(signal.sourceId()).isEqualTo("postgres-source");
                assertThat(signal.tableId()).isEqualTo(HeartbeatMetadata.tableIdFor("appdb"));
                assertThat(signal.reason()).contains("same source stream");
              });
      assertThat(checkpointStore.load("postgres-source")).isEmpty();
      assertThat(stream.appliedLsn()).isNull();
      assertThat(stream.flushedLsn()).isNull();
    }
  }

  @Test
  void failsClosedWhenOwnHeartbeatRunIdAppearsOnUnexpectedSourceStream() throws Exception {
    // Same runId but a different source_stream_id indicates a configuration / stream-rotation
    // misconfiguration where the runtime is somehow bound to an unexpected live stream.
    // Per docs/IMPLEMENTATION.md §7.1 the runtime must fail closed in that case rather than
    // mark itself as having confirmed its own heartbeat and resume.
    StubStream stream =
        new StubStream(
            List.of(
                relation(
                    100,
                    "dblog_meta",
                    "heartbeats",
                    'd',
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("id", true, 20),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("run_id", false, 25),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("source_stream_id", false, 25),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("last_beat_at", false, 1184)),
                begin("0/31", TX_TIME, 79),
                update(
                    100,
                    tuple("1", null, null, null),
                    tuple("1", "test-run", "unexpected-stream", "2026-03-20T00:05:00Z")),
                commit("0/38", "0/40", TX_TIME)),
            java.util.Arrays.asList(null, null, null, PostgresLsn.parse("0/40")));
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("next-pgoutput-heartbeat-unexpected-stream"))) {
      PostgresTransactionStreamingSession session =
          new PostgresTransactionStreamingSession(
              "appdb",
              "test-run",
              "internal-stream",
              "postgres-source",
              List.of(),
              stream,
              new PostgresSourceCheckpointStore(stateStore));

      assertThatThrownBy(session::readPendingTransaction)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("unexpected source stream");
    }
  }

  @Test
  void decodesTemporalAndUuidColumnsFromTupleStrings() throws Exception {
    UUID uuid = UUID.fromString("11111111-1111-1111-1111-111111111111");
    Instant updatedAt = Instant.parse("2026-03-20T10:15:30Z");
    StubStream stream =
        new StubStream(
            List.of(
                relation(
                    7,
                    "public",
                    "typed_values",
                    'f',
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("id", true, 20),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("event_date", false, 1082),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("event_time", false, 1083),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("updated_at", false, 1184),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("entity_uuid", false, 2950)),
                begin("0/16DA010", TX_TIME, 42),
                insert(
                    7,
                    "1",
                    "2026-03-20",
                    "10:15:30",
                    "2026-03-20T10:15:30Z",
                    uuid.toString()),
                commit("0/16DA018", "0/16DA020", TX_TIME)),
            java.util.Arrays.asList(null, null, null, PostgresLsn.parse("0/16DA020")));
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("next-pgoutput-typed-scalars"))) {
      PostgresTransactionStreamingSession session =
          new PostgresTransactionStreamingSession(
              "appdb",
              "test-run",
              "internal-stream",
              "postgres-source",
              List.of(typedScalarSchema()),
              stream,
              new PostgresSourceCheckpointStore(stateStore));

      PostgresPgoutputTransaction transaction = session.readPendingTransaction().orElseThrow();

      assertThat(transaction.events()).hasSize(1);
      assertThat(transaction.events().getFirst().afterRow().asMap())
          .containsEntry("id", 1L)
          .containsEntry("event_date", LocalDate.parse("2026-03-20"))
          .containsEntry("event_time", LocalTime.parse("10:15:30"))
          .containsEntry("updated_at", updatedAt)
          .containsEntry("entity_uuid", uuid);
    }
  }

  @Test
  void skipsUnsupportedNonPrimaryColumnsButPreservesUnsupportedPrimaryKeyTupleValues()
      throws Exception {
    StubStream stream =
        new StubStream(
            List.of(
                relation(
                    7,
                    "public",
                    "weird_keys",
                    'f',
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("weird_id", true, 25),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("ignored_payload", false, 25),
                    new PostgresPgoutputDecoderTestsHelper.RelationColumnSpec("display_name", false, 25)),
                begin("0/16DA010", TX_TIME, 42),
                insert(7, "pk-1", "ignored", "alice"),
                commit("0/16DA018", "0/16DA020", TX_TIME)),
            java.util.Arrays.asList(null, null, null, PostgresLsn.parse("0/16DA020")));
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("next-pgoutput-unsupported-pk"))) {
      PostgresTransactionStreamingSession session =
          new PostgresTransactionStreamingSession(
              "appdb",
              "test-run",
              "internal-stream",
              "postgres-source",
              List.of(unsupportedPrimaryKeySchema()),
              stream,
              new PostgresSourceCheckpointStore(stateStore));

      PostgresPgoutputTransaction transaction = session.readPendingTransaction().orElseThrow();

      assertThat(transaction.events()).hasSize(1);
      assertThat(transaction.events().getFirst().primaryKey().asMap()).containsExactlyEntriesOf(
          java.util.Map.of("weird_id", "pk-1"));
      assertThat(transaction.events().getFirst().afterRow().asMap())
          .containsEntry("weird_id", "pk-1")
          .containsEntry("display_name", "alice")
          .doesNotContainKey("ignored_payload");
    }
  }

  private static TableSchema widgetSchema() {
    return TableSchema.create(
        new TableId("appdb", "public", "widgets"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true)),
        TX_TIME);
  }

  private static TableSchema typedScalarSchema() {
    return TableSchema.create(
        new TableId("appdb", "public", "typed_values"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("event_date", "date", NeutralColumnType.DATE, false, true),
            new ColumnDefinition("event_time", "time", NeutralColumnType.TIME, false, true),
            new ColumnDefinition("updated_at", "timestamptz", NeutralColumnType.TIMESTAMP, false, true),
            new ColumnDefinition("entity_uuid", "uuid", NeutralColumnType.UUID, false, true)),
        TX_TIME);
  }

  private static TableSchema unsupportedPrimaryKeySchema() {
    return TableSchema.create(
        new TableId("appdb", "public", "weird_keys"),
        List.of(
            new ColumnDefinition("weird_id", "opaque", NeutralColumnType.UNSUPPORTED, true, false),
            new ColumnDefinition("ignored_payload", "opaque", NeutralColumnType.UNSUPPORTED, false, true),
            new ColumnDefinition("display_name", "text", NeutralColumnType.STRING, false, true)),
        TX_TIME);
  }

  private static byte[] relation(
      int relationId,
      String schemaName,
      String tableName,
      char replicaIdentity,
      PostgresPgoutputDecoderTestsHelper.RelationColumnSpec... columns)
      throws Exception {
    return PostgresPgoutputDecoderTestsHelper.relation(relationId, schemaName, tableName, replicaIdentity, columns);
  }

  private static byte[] begin(String finalLsn, Instant timestamp, int transactionId)
      throws Exception {
    return PostgresPgoutputDecoderTestsHelper.begin(finalLsn, timestamp, transactionId);
  }

  private static byte[] insert(int relationId, String... values) throws Exception {
    return PostgresPgoutputDecoderTestsHelper.insert(relationId, values);
  }

  private static byte[] update(
      int relationId, String[] oldValues, String[] newValues) throws Exception {
    return PostgresPgoutputDecoderTestsHelper.update(relationId, oldValues, newValues);
  }

  private static byte[] updateWithKeyOldTuple(
      int relationId, String[] oldValues, String[] newValues) throws Exception {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(buffer)) {
      out.writeByte('U');
      out.writeInt(relationId);
      out.writeByte('K');
      writeTuple(out, oldValues);
      out.writeByte('N');
      writeTuple(out, newValues);
    }
    return buffer.toByteArray();
  }

  private static byte[] updateWithoutOldTuple(int relationId, String[] newValues)
      throws Exception {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(buffer)) {
      out.writeByte('U');
      out.writeInt(relationId);
      out.writeByte('N');
      writeTuple(out, newValues);
    }
    return buffer.toByteArray();
  }

  private static byte[] delete(int relationId, String[] oldValues) throws Exception {
    return PostgresPgoutputDecoderTestsHelper.delete(relationId, oldValues);
  }

  private static byte[] deleteWithKeyOldTuple(int relationId, String[] oldValues)
      throws Exception {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(buffer)) {
      out.writeByte('D');
      out.writeInt(relationId);
      out.writeByte('K');
      writeTuple(out, oldValues);
    }
    return buffer.toByteArray();
  }

  private static byte[] truncate(int options, int... relationIds) throws Exception {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(buffer)) {
      out.writeByte('T');
      out.writeInt(relationIds.length);
      out.writeByte(options);
      for (int relationId : relationIds) {
        out.writeInt(relationId);
      }
    }
    return buffer.toByteArray();
  }

  private static byte[] commit(String commitLsn, String endLsn, Instant timestamp)
      throws Exception {
    return PostgresPgoutputDecoderTestsHelper.commit(commitLsn, endLsn, timestamp, 0);
  }

  private static void writeTuple(DataOutputStream out, String[] values) throws Exception {
    out.writeShort(values.length);
    for (String value : values) {
      if (value == null) {
        out.writeByte('n');
        continue;
      }
      out.writeByte('t');
      byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
      out.writeInt(bytes.length);
      out.write(bytes);
    }
  }

  private static String[] tuple(String... values) {
    return values;
  }

  private static final class StubStream implements PostgresPgoutputStream {
    private final Deque<byte[]> messages;
    private final List<PostgresLsn> receiveLsns;
    private int receiveLsnIndex;
    private PostgresLsn currentReceiveLsn;
    private PostgresLsn appliedLsn;
    private PostgresLsn flushedLsn;

    private StubStream(List<byte[]> messages, List<PostgresLsn> receiveLsns) {
      this.messages = new ArrayDeque<>(messages);
      this.receiveLsns = new java.util.ArrayList<>(receiveLsns);
    }

    @Override
    public Optional<ByteBuffer> readPending() throws SQLException {
      byte[] message = messages.pollFirst();
      currentReceiveLsn =
          receiveLsnIndex < receiveLsns.size() ? receiveLsns.get(receiveLsnIndex++) : null;
      return message == null ? Optional.empty() : Optional.of(ByteBuffer.wrap(message));
    }

    @Override
    public Optional<PostgresLsn> lastReceiveLsn() {
      return Optional.ofNullable(currentReceiveLsn);
    }

    @Override
    public void setAppliedLsn(PostgresLsn lsn) {
      appliedLsn = lsn;
    }

    @Override
    public void setFlushedLsn(PostgresLsn lsn) {
      flushedLsn = lsn;
    }

    @Override
    public void forceUpdateStatus() {}

    @Override
    public void close() {}

    private PostgresLsn appliedLsn() {
      return appliedLsn;
    }

    private PostgresLsn flushedLsn() {
      return flushedLsn;
    }
  }
}
