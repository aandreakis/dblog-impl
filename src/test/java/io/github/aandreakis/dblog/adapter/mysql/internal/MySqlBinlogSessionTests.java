package io.github.aandreakis.dblog.adapter.mysql.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.github.aandreakis.dblog.adapter.mysql.MySqlBinlogTransaction;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourcePosition;
import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class MySqlBinlogSessionTests {
  private static final Instant TX_TIME = Instant.parse("2026-03-21T00:00:00Z");

  @Test
  void decodesCommittedUserTableTransactionFromMessageStream() throws Exception {
    StubStream stream =
        new StubStream(
            List.of(
                new MySqlBinlogMessage.TableMap(7L, "appdb", "widgets", pos(100L), TX_TIME),
                new MySqlBinlogMessage.Gtid("uuid:1", pos(101L), TX_TIME),
                new MySqlBinlogMessage.WriteRows(
                    7L, List.<Object[]>of(row(1L, "one")), pos(102L), TX_TIME),
                new MySqlBinlogMessage.UpdateRows(
                    7L,
                    List.of(new MySqlBinlogMessage.RowChange(row(1L, "one"), row(1L, "two"))),
                    pos(103L),
                    TX_TIME),
                new MySqlBinlogMessage.DeleteRows(
                    7L, List.<Object[]>of(row(1L, "two")), pos(104L), TX_TIME),
                new MySqlBinlogMessage.Commit("xid-42", pos(105L), TX_TIME)));
    MySqlBinlogSession session =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "mysql-source",
            List.of(widgetSchema()),
            stream);

    MySqlBinlogTransaction transaction = session.readPendingTransaction().orElseThrow();

    assertThat(transaction.transactionId()).isEqualTo("uuid:1");
    assertThat(transaction.gtid()).isEqualTo("uuid:1");
    assertThat(transaction.checkpointPosition()).isEqualTo(pos(105L));
    assertThat(transaction.events()).hasSize(3);
    assertThat(transaction.events())
        .extracting(event -> event.operationType().name())
        .containsExactly("INSERT", "UPDATE", "DELETE");
    assertThat(transaction.events())
        .extracting(event -> event.captureOrigin().name())
        .containsOnly("LOG");
    assertThat(transaction.events().get(0).tableId()).isEqualTo(widgetSchema().tableId());
    assertThat(transaction.events().get(0).afterRow().asMap()).containsEntry("id", 1L).containsEntry("name", "one");
    assertThat(transaction.events().get(1).beforeRow().asMap()).containsEntry("name", "one");
    assertThat(transaction.events().get(1).afterRow().asMap()).containsEntry("name", "two");
    assertThat(transaction.events().get(2).beforeRow().asMap()).containsEntry("name", "two");
  }

  @Test
  void ignoresExtraTableMapColumnsWhenSelectedContractStillMatches() throws Exception {
    StubStream stream =
        new StubStream(
            List.of(
                new MySqlBinlogMessage.TableMap(
                    7L,
                    "appdb",
                    "widgets",
                    null,
                    null,
                    null,
                    List.of("id", "name", "description"),
                    List.of(0),
                    null,
                    List.of(),
                    pos(100L),
                    TX_TIME),
                new MySqlBinlogMessage.Gtid("uuid:extra-column-table-map", pos(101L), TX_TIME),
                new MySqlBinlogMessage.WriteRows(
                    7L,
                    List.<Object[]>of(new Object[] {1L, "one", "ignored by contract"}),
                    pos(102L),
                    TX_TIME),
                new MySqlBinlogMessage.Commit("xid-extra-column-table-map", pos(103L), TX_TIME)));
    MySqlBinlogSession session =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "mysql-source",
            List.of(widgetSchema()),
            stream);

    MySqlBinlogTransaction transaction = session.readPendingTransaction().orElseThrow();

    assertThat(transaction.events()).hasSize(1);
    assertThat(transaction.events().getFirst().primaryKey().asMap())
        .containsExactlyEntriesOf(java.util.Map.of("id", 1L));
    assertThat(transaction.events().getFirst().afterRow().asMap())
        .containsEntry("id", 1L)
        .containsEntry("name", "one")
        .doesNotContainKey("description");
  }

  @Test
  void preservesNullableUserColumnValuesWhenDecodingCommittedTransactions() throws Exception {
    StubStream stream =
        new StubStream(
            List.of(
                new MySqlBinlogMessage.TableMap(7L, "appdb", "widgets", pos(100L), TX_TIME),
                new MySqlBinlogMessage.Gtid("uuid:nulls", pos(101L), TX_TIME),
                new MySqlBinlogMessage.WriteRows(
                    7L, List.<Object[]>of(row(1L, null)), pos(102L), TX_TIME),
                new MySqlBinlogMessage.Commit("xid-null", pos(103L), TX_TIME)));
    MySqlBinlogSession session =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "mysql-source",
            List.of(widgetSchema()),
            stream);

    MySqlBinlogTransaction transaction = session.readPendingTransaction().orElseThrow();

    assertThat(transaction.events()).hasSize(1);
    assertThat(transaction.events().getFirst().afterRow().asMap())
        .containsEntry("id", 1L)
        .containsEntry("name", null);
  }

  @Test
  void decodesJsonColumnsFromByteArraysIntoJsonText() throws Exception {
    StubStream stream =
        new StubStream(
            List.of(
                new MySqlBinlogMessage.TableMap(7L, "appdb", "typed_values", pos(100L), TX_TIME),
                new MySqlBinlogMessage.Gtid("uuid:json", pos(101L), TX_TIME),
                new MySqlBinlogMessage.WriteRows(
                    7L,
                    List.<Object[]>of(
                        new Object[] {
                          1L, "{\"tag\":\"mid\",\"value\":123}".getBytes(StandardCharsets.UTF_8)
                        }),
                    pos(102L),
                    TX_TIME),
                new MySqlBinlogMessage.Commit("xid-json", pos(103L), TX_TIME)));
    MySqlBinlogSession session =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "mysql-source",
            List.of(jsonSchema()),
            stream);

    MySqlBinlogTransaction transaction = session.readPendingTransaction().orElseThrow();

    assertThat(transaction.events()).hasSize(1);
    assertThat(transaction.events().getFirst().afterRow().asMap())
        .containsEntry("id", 1L)
        .containsEntry("json_value", "{\"tag\":\"mid\",\"value\":123}");
  }

  @Test
  void surfacesMetadataTableUpdatesAsInternalWatermarkAndHeartbeatEvents() throws Exception {
    Instant beat = Instant.parse("2026-03-21T00:01:00Z");
    StubStream stream =
        new StubStream(
            List.of(
                new MySqlBinlogMessage.TableMap(99L, "dblog_meta", "watermarks", pos(200L), TX_TIME),
                new MySqlBinlogMessage.TableMap(100L, "dblog_meta", "heartbeats", pos(201L), TX_TIME),
                new MySqlBinlogMessage.Gtid("uuid:2", pos(202L), TX_TIME),
                new MySqlBinlogMessage.UpdateRows(
                    99L,
                    List.of(
                        new MySqlBinlogMessage.RowChange(
                            new Object[] {1L, null, null},
                            new Object[] {1L, "test-run", "lw-1"})),
                    pos(203L),
                    TX_TIME),
                new MySqlBinlogMessage.UpdateRows(
                    100L,
                    List.of(
                        new MySqlBinlogMessage.RowChange(
                            new Object[] {1L, null, null},
                            new Object[] {1L, "test-run", Timestamp.from(beat)})),
                    pos(204L),
                    TX_TIME),
                new MySqlBinlogMessage.Commit("xid-77", pos(205L), TX_TIME)));
    MySqlBinlogSession session =
        new MySqlBinlogSession("test-run", "internal-stream", "mysql-source", List.of(), stream);

    MySqlBinlogTransaction transaction = session.readPendingTransaction().orElseThrow();

    assertThat(transaction.events()).hasSize(2);
    assertThat(transaction.events().get(0).tableId())
        .isEqualTo(io.github.aandreakis.dblog.core.reconcile.WatermarkMetadata.tableIdFor("mysql-source"));
    assertThat(transaction.events().get(0).operationType()).isEqualTo(OperationType.WATERMARK);
    assertThat(transaction.events().get(0).captureOrigin()).isEqualTo(CaptureOrigin.LOG);
    assertThat(transaction.events().get(0).afterRow().asMap()).containsEntry("token", "lw-1");

    assertThat(transaction.events().get(1).tableId())
        .isEqualTo(io.github.aandreakis.dblog.core.reconcile.HeartbeatMetadata.tableIdFor("mysql-source"));
    assertThat(transaction.events().get(1).operationType()).isEqualTo(OperationType.HEARTBEAT);
    assertThat(transaction.events().get(1).captureOrigin()).isEqualTo(CaptureOrigin.LOG);
    assertThat(transaction.events().get(1).afterRow().asMap())
        .containsEntry("last_beat_at", beat);
  }

  @Test
  void ignoresMetadataEventsFromAnotherRunId() throws Exception {
    Instant beat = Instant.parse("2026-03-21T00:01:00Z");
    StubStream stream =
        new StubStream(
            List.of(
                new MySqlBinlogMessage.TableMap(99L, "dblog_meta", "watermarks", pos(200L), TX_TIME),
                new MySqlBinlogMessage.TableMap(100L, "dblog_meta", "heartbeats", pos(201L), TX_TIME),
                new MySqlBinlogMessage.Gtid("uuid:2", pos(202L), TX_TIME),
                new MySqlBinlogMessage.UpdateRows(
                    99L,
                    List.of(
                        new MySqlBinlogMessage.RowChange(
                            new Object[] {1L, null, null},
                            new Object[] {1L, "other-run", "lw-1"})),
                    pos(203L),
                    TX_TIME),
                new MySqlBinlogMessage.UpdateRows(
                    100L,
                    List.of(
                        new MySqlBinlogMessage.RowChange(
                            new Object[] {1L, null, null},
                            new Object[] {1L, "other-run", Timestamp.from(beat)})),
                    pos(204L),
                    TX_TIME),
                new MySqlBinlogMessage.Commit("xid-77", pos(205L), TX_TIME)));
    MySqlBinlogSession session =
        new MySqlBinlogSession("test-run", "internal-stream", "mysql-source", List.of(), stream);

    assertThat(session.readPendingTransaction()).isEmpty();
  }

  @Test
  void failsClosedWhenForeignHeartbeatAppearsOnSameSourceStreamAfterOwnHeartbeat()
      throws Exception {
    Instant ownBeat = Instant.parse("2026-03-21T00:01:00Z");
    Instant foreignBeat = ownBeat.plusSeconds(60);
    StubStream stream =
        new StubStream(
            List.of(
                new MySqlBinlogMessage.TableMap(100L, "dblog_meta", "heartbeats", pos(240L), TX_TIME),
                new MySqlBinlogMessage.Gtid("uuid:2c", pos(241L), TX_TIME),
                new MySqlBinlogMessage.UpdateRows(
                    100L,
                    List.of(
                        new MySqlBinlogMessage.RowChange(
                            new Object[] {1L, null, null, null},
                            new Object[] {1L, "test-run", "internal-stream", Timestamp.from(ownBeat)}),
                        new MySqlBinlogMessage.RowChange(
                            new Object[] {1L, "test-run", "internal-stream", Timestamp.from(ownBeat)},
                            new Object[] {1L, "other-run", "internal-stream", Timestamp.from(foreignBeat)})),
                    pos(242L),
                    TX_TIME)));
    MySqlBinlogSession session =
        new MySqlBinlogSession("test-run", "internal-stream", "mysql-source", List.of(), stream);

    assertThatThrownBy(session::readPendingTransaction)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("same source stream");
  }

  @Test
  void buffersTheEntireCommittedTransactionInHeapBeforeEmission() throws Exception {
    // Pins the intentional non-goal in docs/SPEC.md §4: "spill-to-disk or per-batch streaming
    // of very large single transactions" is NOT supported. The adapter session buffers every
    // event of a committed transaction and surfaces them as one output batch. This test
    // characterises that contract so future changes can't silently switch to streaming
    // partial transactions (which would violate at-least-once and break the watermark
    // single-consumer contract).
    int eventCount = 500;
    java.util.List<MySqlBinlogMessage> messages = new java.util.ArrayList<>();
    messages.add(new MySqlBinlogMessage.TableMap(7L, "appdb", "widgets", pos(100L), TX_TIME));
    messages.add(new MySqlBinlogMessage.Gtid("uuid:large", pos(101L), TX_TIME));
    for (int i = 0; i < eventCount; i++) {
      messages.add(
          new MySqlBinlogMessage.WriteRows(
              7L,
              java.util.List.<Object[]>of(row((long) (i + 1), "row-" + i)),
              pos(200L + i),
              TX_TIME));
    }
    messages.add(new MySqlBinlogMessage.Commit("xid-large", pos(700L + eventCount), TX_TIME));

    StubStream stream = new StubStream(messages);
    MySqlBinlogSession session =
        new MySqlBinlogSession(
            "test-run", "internal-stream", "mysql-source", List.of(widgetSchema()), stream);

    MySqlBinlogTransaction transaction = session.readPendingTransaction().orElseThrow();

    // Single-batch emission: every event for the committed transaction is present in a single
    // transaction object. The runtime loop later calls `sink.appendEvents(transaction.events())`
    // exactly once for this commit — the adapter never surfaces a partial transaction.
    assertThat(transaction.events()).hasSize(eventCount);
    assertThat(transaction.events())
        .allMatch(event -> event.operationType() == OperationType.INSERT);
    assertThat(transaction.events().get(0).afterRow().asMap()).containsEntry("id", 1L);
    assertThat(transaction.events().get(eventCount - 1).afterRow().asMap())
        .containsEntry("id", (long) eventCount);
    // Checkpoint reflects the COMMIT, not any individual row event.
    assertThat(transaction.checkpointPosition().binlogPosition()).isEqualTo(700L + eventCount);
  }

  @Test
  void failsClosedWhenOwnHeartbeatRunIdAppearsOnUnexpectedSourceStream() throws Exception {
    // The MySQL adapter must refuse to mark its own heartbeat as confirmed if the
    // accompanying source_stream_id does not match the current runtime stream identity.
    // This catches stream-rotation / misconfiguration where the same run_id is somehow
    // observed on a different live stream.
    Instant beat = Instant.parse("2026-03-21T00:05:00Z");
    StubStream stream =
        new StubStream(
            List.of(
                new MySqlBinlogMessage.TableMap(100L, "dblog_meta", "heartbeats", pos(300L), TX_TIME),
                new MySqlBinlogMessage.Gtid("uuid:unexpected-stream", pos(301L), TX_TIME),
                new MySqlBinlogMessage.UpdateRows(
                    100L,
                    List.of(
                        new MySqlBinlogMessage.RowChange(
                            new Object[] {1L, null, null, null},
                            new Object[] {1L, "test-run", "unexpected-stream", Timestamp.from(beat)})),
                    pos(302L),
                    TX_TIME)));
    MySqlBinlogSession session =
        new MySqlBinlogSession("test-run", "internal-stream", "mysql-source", List.of(), stream);

    assertThatThrownBy(session::readPendingTransaction)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("unexpected source stream");
  }

  @Test
  void ignoresQueryEventsFromUnrelatedDatabasesAndContinuesStreaming() throws Exception {
    StubStream stream =
        new StubStream(
            List.of(
                new MySqlBinlogMessage.Query("mysql", "TRUNCATE TABLE time_zone", pos(90L), TX_TIME),
                new MySqlBinlogMessage.TableMap(7L, "appdb", "widgets", pos(100L), TX_TIME),
                new MySqlBinlogMessage.Gtid("uuid:1", pos(101L), TX_TIME),
                new MySqlBinlogMessage.WriteRows(
                    7L, List.<Object[]>of(row(1L, "one")), pos(102L), TX_TIME),
                new MySqlBinlogMessage.Commit("xid-42", pos(103L), TX_TIME)));
    MySqlBinlogSession session =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "mysql-source",
            List.of(widgetSchema()),
            stream);

    MySqlBinlogTransaction transaction = session.readPendingTransaction().orElseThrow();

    assertThat(transaction.events()).hasSize(1);
    assertThat(transaction.events().getFirst().afterRow().asMap())
        .containsEntry("id", 1L)
        .containsEntry("name", "one");
  }

  @Test
  void failsClosedOnUncapturedTableDdlInCapturedDatabaseAsDocumentedLimitation() {
    // MySQL Query events expose only the session default database and raw SQL text. DBLog
    // intentionally avoids DDL parsing, so row/schema-affecting DDL in a captured database is
    // treated as relevant even when the SQL targets an uncaptured table.
    StubStream stream =
        new StubStream(
            List.of(
                new MySqlBinlogMessage.Query(
                    "appdb",
                    "ALTER TABLE `appdb`.`untracked` ADD COLUMN x INT",
                    pos(90L),
                    TX_TIME),
                new MySqlBinlogMessage.TableMap(7L, "appdb", "widgets", pos(100L), TX_TIME),
                new MySqlBinlogMessage.Gtid("uuid:after-untracked-ddl", pos(101L), TX_TIME),
                new MySqlBinlogMessage.WriteRows(
                    7L, List.<Object[]>of(row(1L, "after-untracked-ddl")), pos(102L), TX_TIME),
                new MySqlBinlogMessage.Commit("xid-after-untracked-ddl", pos(103L), TX_TIME)));
    MySqlBinlogSession session =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "mysql-source",
            List.of(widgetSchema()),
            stream);

    Throwable failure = catchThrowable(session::readPendingTransaction);

    assertThat(failure)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("row-state-affecting or schema-affecting DDL")
        .hasMessageContaining("full dump required")
        .hasMessageContaining("ALTER TABLE `appdb`.`untracked`");
  }

  @Test
  void ignoresDblogMetadataBootstrapDdlLeakedIntoBinlogAndContinuesStreaming() throws Exception {
    // DBLog's own CREATE DATABASE/TABLE statements reach the binlog when the bootstrap
    // connection lacks SESSION_VARIABLES_ADMIN. The Query event carries the user's default
    // database (e.g. "appdb") rather than "dblog_meta", so the fast path cannot skip it; the
    // SQL-pattern fallback in shouldIgnoreQuery must. If this test ever regresses, smokeCheck
    // fails closed with "does not yet support DDL/session query decoding".
    StubStream stream =
        new StubStream(
            List.of(
                new MySqlBinlogMessage.Query(
                    "appdb",
                    "CREATE DATABASE IF NOT EXISTS `dblog_meta`",
                    pos(80L),
                    TX_TIME),
                new MySqlBinlogMessage.Query(
                    "appdb",
                    "CREATE TABLE IF NOT EXISTS `dblog_meta`.`watermarks` (`id` BIGINT PRIMARY KEY, `run_id` VARCHAR(255) NULL, `token` VARCHAR(512) NULL)",
                    pos(81L),
                    TX_TIME),
                new MySqlBinlogMessage.Query(
                    "appdb",
                    "CREATE TABLE IF NOT EXISTS `dblog_meta`.`heartbeats` (`id` BIGINT PRIMARY KEY, `run_id` VARCHAR(255) NULL, `source_stream_id` VARCHAR(255) NULL, `last_beat_at` TIMESTAMP NULL)",
                    pos(82L),
                    TX_TIME),
                new MySqlBinlogMessage.TableMap(7L, "appdb", "widgets", pos(100L), TX_TIME),
                new MySqlBinlogMessage.Gtid("uuid:after-ddl", pos(101L), TX_TIME),
                new MySqlBinlogMessage.WriteRows(
                    7L, List.<Object[]>of(row(1L, "one")), pos(102L), TX_TIME),
                new MySqlBinlogMessage.Commit("xid-bootstrap", pos(103L), TX_TIME)));
    MySqlBinlogSession session =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "mysql-source",
            List.of(widgetSchema()),
            stream);

    MySqlBinlogTransaction transaction = session.readPendingTransaction().orElseThrow();

    assertThat(transaction.events()).hasSize(1);
    assertThat(transaction.events().getFirst().afterRow().asMap())
        .containsEntry("id", 1L)
        .containsEntry("name", "one");
  }

  @Test
  void ignoresDblogMetadataDmlQueryEventsAndContinuesStreaming() throws Exception {
    // Statement-format metadata writes can arrive as Query events with dblog_meta as the
    // default database. These exact DBLog-owned watermark/heartbeat mutations are safe to skip,
    // but arbitrary metadata-database DDL still fails closed in the tests below.
    StubStream stream =
        new StubStream(
            List.of(
                new MySqlBinlogMessage.Query(
                    "dblog_meta",
                    "INSERT INTO `dblog_meta`.`watermarks` (`id`, `run_id`, `token`) VALUES (1, NULL, NULL) ON DUPLICATE KEY UPDATE `id` = `id`",
                    pos(80L),
                    TX_TIME),
                new MySqlBinlogMessage.Query(
                    "dblog_meta",
                    "UPDATE `dblog_meta`.`watermarks` SET `run_id` = 'test-run', `token` = 'lw-1' WHERE `id` = 1",
                    pos(81L),
                    TX_TIME),
                new MySqlBinlogMessage.Query(
                    "dblog_meta",
                    "INSERT INTO `dblog_meta`.`heartbeats` (`id`, `run_id`, `source_stream_id`, `last_beat_at`) VALUES (1, NULL, NULL, NULL) ON DUPLICATE KEY UPDATE `id` = `id`",
                    pos(82L),
                    TX_TIME),
                new MySqlBinlogMessage.Query(
                    "dblog_meta",
                    "UPDATE `dblog_meta`.`heartbeats` SET `run_id` = 'test-run', `source_stream_id` = 'internal-stream', `last_beat_at` = '2026-03-21 00:01:00' WHERE `id` = 1",
                    pos(83L),
                    TX_TIME),
                new MySqlBinlogMessage.TableMap(7L, "appdb", "widgets", pos(100L), TX_TIME),
                new MySqlBinlogMessage.Gtid("uuid:after-metadata-dml", pos(101L), TX_TIME),
                new MySqlBinlogMessage.WriteRows(
                    7L, List.<Object[]>of(row(1L, "after-metadata-dml")), pos(102L), TX_TIME),
                new MySqlBinlogMessage.Commit("xid-after-metadata-dml", pos(103L), TX_TIME)));
    MySqlBinlogSession session =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "mysql-source",
            List.of(widgetSchema()),
            stream);

    MySqlBinlogTransaction transaction = session.readPendingTransaction().orElseThrow();

    assertThat(transaction.events()).hasSize(1);
    assertThat(transaction.events().getFirst().afterRow().asMap())
        .containsEntry("id", 1L)
        .containsEntry("name", "after-metadata-dml");
  }

  @Test
  void failsClosedOnUserTableDdlShadowingDblogMetadataNames() throws Exception {
    // The fallback regex must not match user tables whose names contain "dblog_meta" or
    // whose table name is "watermarks" / "heartbeats" under a different schema. A user
    // ALTER/CREATE on such a table should still fail closed because DBLog does not decode
    // generic DDL.
    StubStream stream =
        new StubStream(
            List.of(
                new MySqlBinlogMessage.Query(
                    "appdb",
                    "CREATE TABLE `appdb`.`watermarks` (`id` BIGINT PRIMARY KEY)",
                    pos(80L),
                    TX_TIME),
                new MySqlBinlogMessage.Commit("xid-user-ddl", pos(81L), TX_TIME)));
    MySqlBinlogSession session =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "mysql-source",
            List.of(widgetSchema()),
            stream);

    assertThatThrownBy(session::readPendingTransaction)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("row-state-affecting or schema-affecting DDL");
  }

  @Test
  void ignoresBenignAdministrativeDdlOnCapturedDatabaseAndContinuesStreaming() throws Exception {
    // Routine operator/DBA activity on the captured database — privilege changes, stored
    // routines, indexes, table maintenance, server housekeeping — arrives in the binlog with
    // databaseName set to the captured database. DBLog must skip these rather than fail
    // closed, otherwise any DBA action kills the runtime.
    StubStream stream =
        new StubStream(
            List.of(
                new MySqlBinlogMessage.Query(
                    "appdb", "GRANT SELECT ON `appdb`.* TO 'reporting'@'%'", pos(80L), TX_TIME),
                new MySqlBinlogMessage.Query(
                    "appdb", "REVOKE INSERT ON `appdb`.* FROM 'reporting'@'%'", pos(81L), TX_TIME),
                new MySqlBinlogMessage.Query(
                    "appdb", "CREATE USER 'audit'@'%' IDENTIFIED BY 'secret'", pos(82L), TX_TIME),
                new MySqlBinlogMessage.Query(
                    "appdb", "DROP USER 'old-service'@'%'", pos(83L), TX_TIME),
                new MySqlBinlogMessage.Query(
                    "appdb",
                    // Real MySQL binlog CREATE PROCEDURE statements contain embedded
                    // newlines (the server re-emits the parsed body). The allowlist regex
                    // must match across lines.
                    "CREATE DEFINER=`root`@`localhost` PROCEDURE `housekeeping`()\nBEGIN\n  SELECT 1;\nEND",
                    pos(84L),
                    TX_TIME),
                new MySqlBinlogMessage.Query(
                    "appdb", "DROP PROCEDURE IF EXISTS `appdb`.`housekeeping`", pos(85L), TX_TIME),
                new MySqlBinlogMessage.Query(
                    "appdb", "DROP FUNCTION IF EXISTS `appdb`.`my_fn`", pos(86L), TX_TIME),
                new MySqlBinlogMessage.Query(
                    "appdb", "DROP TRIGGER IF EXISTS `appdb`.`my_trigger`", pos(87L), TX_TIME),
                new MySqlBinlogMessage.Query(
                    "appdb", "DROP VIEW IF EXISTS `appdb`.`my_view`", pos(88L), TX_TIME),
                new MySqlBinlogMessage.Query(
                    "appdb",
                    "CREATE UNIQUE INDEX widgets_name_idx ON `appdb`.`widgets` (name)",
                    pos(89L),
                    TX_TIME),
                new MySqlBinlogMessage.Query(
                    "appdb", "DROP INDEX widgets_name_idx ON `appdb`.`widgets`", pos(90L), TX_TIME),
                new MySqlBinlogMessage.Query(
                    "appdb", "ANALYZE TABLE `appdb`.`widgets`", pos(91L), TX_TIME),
                new MySqlBinlogMessage.Query(
                    "appdb", "OPTIMIZE NO_WRITE_TO_BINLOG TABLE `appdb`.`widgets`", pos(92L), TX_TIME),
                new MySqlBinlogMessage.Query(
                    "appdb", "FLUSH PRIVILEGES", pos(93L), TX_TIME),
                new MySqlBinlogMessage.Query(
                    "appdb", "SET @@global.read_only = OFF", pos(94L), TX_TIME),
                new MySqlBinlogMessage.TableMap(7L, "appdb", "widgets", pos(100L), TX_TIME),
                new MySqlBinlogMessage.Gtid("uuid:after-admin-ddl", pos(101L), TX_TIME),
                new MySqlBinlogMessage.WriteRows(
                    7L, List.<Object[]>of(row(1L, "after-admin-ddl")), pos(102L), TX_TIME),
                new MySqlBinlogMessage.Commit("xid-after-admin-ddl", pos(103L), TX_TIME)));
    MySqlBinlogSession session =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "mysql-source",
            List.of(widgetSchema()),
            stream);

    MySqlBinlogTransaction transaction = session.readPendingTransaction().orElseThrow();

    assertThat(transaction.events()).hasSize(1);
    assertThat(transaction.events().getFirst().afterRow().asMap())
        .containsEntry("id", 1L)
        .containsEntry("name", "after-admin-ddl");
  }

  @Test
  void failsClosedOnTruncateOfCapturedTableWithActionableMessage() throws Exception {
    // TRUNCATE nukes all rows in one shot and MySQL records it as a QUERY event rather than
    // row deletes. Silently skipping would leave the target with rows the source no longer
    // has. DBLog must fail closed so the operator can re-bootstrap.
    StubStream stream =
        new StubStream(
            List.of(
                new MySqlBinlogMessage.Query(
                    "appdb", "TRUNCATE TABLE `appdb`.`widgets`", pos(80L), TX_TIME)));
    MySqlBinlogSession session =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "mysql-source",
            List.of(widgetSchema()),
            stream);

    assertThatThrownBy(session::readPendingTransaction)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("row-state-affecting or schema-affecting DDL")
        .hasMessageContaining("submit a fresh ALL_TABLES dump")
        .hasMessageContaining("TRUNCATE TABLE");
  }

  @Test
  void failsClosedOnQualifiedCapturedTableDdlWhenDefaultDatabaseIsDblogMeta() {
    // MySQL Query events carry both SQL text and the session default database. A user can run
    // USE dblog_meta and still issue fully qualified DDL against a captured table. The default
    // database alone must not cause DBLog to skip that row-state-affecting statement.
    StubStream stream =
        new StubStream(
            List.of(
                new MySqlBinlogMessage.Query(
                    "dblog_meta", "TRUNCATE TABLE `appdb`.`widgets`", pos(80L), TX_TIME)));
    MySqlBinlogSession session =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "mysql-source",
            List.of(widgetSchema()),
            stream);

    assertThatThrownBy(session::readPendingTransaction)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("row-state-affecting or schema-affecting DDL")
        .hasMessageContaining("TRUNCATE TABLE");
  }

  @Test
  void failsClosedOnDestructiveMetadataTableDdlWhenDefaultDatabaseIsDblogMeta() {
    List<String> statements =
        List.of(
            "ALTER TABLE `dblog_meta`.`watermarks` ADD COLUMN operator_note TEXT",
            "DROP TABLE `dblog_meta`.`watermarks`",
            "TRUNCATE TABLE `dblog_meta`.`watermarks`",
            "ALTER TABLE `dblog_meta`.`heartbeats` ADD COLUMN operator_note TEXT",
            "DROP TABLE `dblog_meta`.`heartbeats`",
            "TRUNCATE TABLE `dblog_meta`.`heartbeats`");

    for (String statement : statements) {
      StubStream stream =
          new StubStream(
              List.of(new MySqlBinlogMessage.Query("dblog_meta", statement, pos(80L), TX_TIME)));
      MySqlBinlogSession session =
          new MySqlBinlogSession(
              "test-run",
              "internal-stream",
              "mysql-source",
              List.of(widgetSchema()),
              stream);

      assertThatThrownBy(session::readPendingTransaction)
          .as(statement)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("row-state-affecting or schema-affecting DDL");
    }
  }

  @Test
  void ignoresDblogOwnMetadataDmlWhenItArrivesAsAQueryEventUnderStatementBinlog()
      throws Exception {
    // When binlog_format=STATEMENT, DBLog's own metadata writes arrive as Query events rather
    // than Row events. The session must ignore these (they are DBLog-internal) and continue
    // decoding subsequent user transactions without throwing. Shapes below match the literal
    // SQL generated by JdbcMySqlWatermarkTableHelper and JdbcMySqlHeartbeatTableHelper.
    List<String> dblogGeneratedStatements =
        List.of(
            "INSERT INTO `dblog_meta`.`watermarks` (`id`, `run_id`, `token`) VALUES (?, ?, ?)"
                + " ON DUPLICATE KEY UPDATE `id` = `id`",
            "UPDATE `dblog_meta`.`watermarks` SET `run_id` = ?, `token` = ? WHERE `id` = ?",
            "INSERT INTO `dblog_meta`.`heartbeats`"
                + " (`id`, `run_id`, `source_stream_id`, `last_beat_at`) VALUES (?, ?, ?, ?)"
                + " ON DUPLICATE KEY UPDATE `id` = `id`",
            "UPDATE `dblog_meta`.`heartbeats` SET `last_beat_at` = ? WHERE `id` = ?");

    for (String statement : dblogGeneratedStatements) {
      StubStream stream =
          new StubStream(
              List.of(
                  new MySqlBinlogMessage.Query("dblog_meta", statement, pos(80L), TX_TIME),
                  new MySqlBinlogMessage.TableMap(7L, "appdb", "widgets", pos(100L), TX_TIME),
                  new MySqlBinlogMessage.Gtid("uuid:after-meta-dml", pos(101L), TX_TIME),
                  new MySqlBinlogMessage.WriteRows(
                      7L, List.<Object[]>of(row(1L, "post-meta-dml")), pos(102L), TX_TIME),
                  new MySqlBinlogMessage.Commit("xid-post-meta-dml", pos(103L), TX_TIME)));
      MySqlBinlogSession session =
          new MySqlBinlogSession(
              "test-run",
              "internal-stream",
              "mysql-source",
              List.of(widgetSchema()),
              stream);

      MySqlBinlogTransaction transaction =
          session.readPendingTransaction().orElseThrow(() -> new AssertionError(statement));
      assertThat(transaction.events()).as(statement).hasSize(1);
      assertThat(transaction.events().getFirst().afterRow().asMap())
          .as(statement)
          .containsEntry("id", 1L)
          .containsEntry("name", "post-meta-dml");
    }
  }

  @Test
  void documentsOnlineAddColumnFailsClosedWhileLiveStreaming() throws Exception {
    // MySQL Query events expose raw DDL text before any later TABLE_MAP metadata. Even a pure
    // additive ADD COLUMN on a captured table is not auto-accepted while DBLog is live; the
    // adapter deliberately fails closed instead of parsing DDL and guessing schema continuity.
    StubStream stream =
        new StubStream(
            List.of(
                new MySqlBinlogMessage.Query(
                    "appdb",
                    "ALTER TABLE `appdb`.`widgets` ADD COLUMN description TEXT",
                    pos(80L),
                    TX_TIME)));
    MySqlBinlogSession session =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "mysql-source",
            List.of(widgetSchema()),
            stream);

    assertThatThrownBy(session::readPendingTransaction)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("row-state-affecting or schema-affecting DDL")
        .hasMessageContaining("ALTER TABLE")
        .hasMessageContaining("ADD COLUMN");
  }

  @Test
  void unsupportedDdlRecoveryMessageAccountsForRestartReplayFromCheckpoint() {
    // Restarting with the same persisted state resumes from the pre-DDL checkpoint and replays
    // this unsupported Query event before the operator can submit a replacement dump. The
    // recovery text must call out the state/reset step explicitly.
    StubStream stream =
        new StubStream(
            List.of(
                new MySqlBinlogMessage.Query(
                    "appdb",
                    "ALTER TABLE `appdb`.`widgets` DROP COLUMN name",
                    pos(80L),
                    TX_TIME)));
    MySqlBinlogSession session =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "mysql-source",
            List.of(widgetSchema()),
            stream);

    Throwable failure = catchThrowable(session::readPendingTransaction);

    assertThat(failure)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("persisted runtime state")
        .hasMessageContaining("<state-path>.mv.db")
        .hasMessageContaining("checkpoint")
        .hasMessageContaining("fresh ALL_TABLES dump")
        .hasMessageNotContaining("then restart and submit a fresh ALL_TABLES dump");
  }

  @Test
  void decodesTemporalAndUuidColumnsThroughTheCommittedTransactionPath() throws Exception {
    UUID uuid = UUID.fromString("11111111-1111-1111-1111-111111111111");
    Instant happenedAt = Instant.parse("2026-03-21T10:15:30Z");
    StubStream stream =
        new StubStream(
            List.of(
                new MySqlBinlogMessage.TableMap(7L, "appdb", "typed_values", pos(100L), TX_TIME),
                new MySqlBinlogMessage.Gtid("uuid:typed", pos(101L), TX_TIME),
                new MySqlBinlogMessage.WriteRows(
                    7L,
                    List.<Object[]>of(
                        new Object[] {
                          1L,
                          java.sql.Date.valueOf(LocalDate.parse("2026-03-21")),
                          Time.valueOf(LocalTime.parse("10:15:30")),
                          Timestamp.from(happenedAt),
                          uuid.toString()
                        }),
                    pos(102L),
                    TX_TIME),
                new MySqlBinlogMessage.Commit("xid-typed", pos(103L), TX_TIME)));
    MySqlBinlogSession session =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "mysql-source",
            List.of(typedScalarSchema()),
            stream);

    MySqlBinlogTransaction transaction = session.readPendingTransaction().orElseThrow();

    assertThat(transaction.events()).hasSize(1);
    assertThat(transaction.events().getFirst().afterRow().asMap())
        .containsEntry("id", 1L)
        .containsEntry("event_date", LocalDate.parse("2026-03-21"))
        .containsEntry("event_time", LocalTime.parse("10:15:30"))
        .containsEntry("updated_at", happenedAt)
        .containsEntry("entity_uuid", uuid);
  }

  @Test
  void documentsMysqlDatetimeLivePathLimitation() throws Exception {
    // mysql-binlog-connector-java surfaces MySQL DATETIME(6) row values as java.util.Date in
    // this runtime path. That leaves DBLog with millisecond precision and no way to distinguish
    // DATETIME's zone-less wall-clock semantics from TIMESTAMP's instant semantics inside the
    // neutral normalizer. This is a documented adapter limitation, not supported parity with the
    // JDBC dump path, which keeps MySQL DATETIME as LocalDateTime.
    LocalDateTime sourceWallClock = LocalDateTime.parse("2026-04-26T10:55:00.123456");
    Instant binlogDecodedInstant = Instant.parse("2026-04-26T10:55:00.123Z");
    StubStream stream =
        new StubStream(
            List.of(
                new MySqlBinlogMessage.TableMap(7L, "appdb", "datetime_values", pos(100L), TX_TIME),
                new MySqlBinlogMessage.Gtid("uuid:datetime-limit", pos(101L), TX_TIME),
                new MySqlBinlogMessage.WriteRows(
                    7L,
                    List.<Object[]>of(
                        new Object[] {
                          1L, new java.util.Date(binlogDecodedInstant.toEpochMilli())
                        }),
                    pos(102L),
                    TX_TIME),
                new MySqlBinlogMessage.Commit("xid-datetime-limit", pos(103L), TX_TIME)));
    MySqlBinlogSession session =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "mysql-source",
            List.of(datetimeSchema()),
            stream);

    MySqlBinlogTransaction transaction = session.readPendingTransaction().orElseThrow();

    Object occurredAt = transaction.events().getFirst().afterRow().asMap().get("occurred_at");
    assertThat(occurredAt).isEqualTo(binlogDecodedInstant);
    assertThat(occurredAt).isNotEqualTo(sourceWallClock);
  }

  @Test
  void skipsUnsupportedNonPrimaryColumnsButPreservesUnsupportedPrimaryKeyValues() throws Exception {
    StubStream stream =
        new StubStream(
            List.of(
                new MySqlBinlogMessage.TableMap(7L, "appdb", "weird_keys", pos(100L), TX_TIME),
                new MySqlBinlogMessage.Gtid("uuid:unsupported-pk", pos(101L), TX_TIME),
                new MySqlBinlogMessage.WriteRows(
                    7L,
                    List.<Object[]>of(new Object[] {"pk-1", "ignored", "alice"}),
                    pos(102L),
                    TX_TIME),
                new MySqlBinlogMessage.Commit("xid-unsupported-pk", pos(103L), TX_TIME)));
    MySqlBinlogSession session =
        new MySqlBinlogSession(
            "test-run",
            "internal-stream",
            "mysql-source",
            List.of(unsupportedPrimaryKeySchema()),
            stream);

    MySqlBinlogTransaction transaction = session.readPendingTransaction().orElseThrow();

    assertThat(transaction.events()).hasSize(1);
    assertThat(transaction.events().getFirst().primaryKey().asMap()).containsExactlyEntriesOf(
        java.util.Map.of("weird_id", "pk-1"));
    assertThat(transaction.events().getFirst().afterRow().asMap())
        .containsEntry("weird_id", "pk-1")
        .containsEntry("display_name", "alice")
        .doesNotContainKey("ignored_payload");
  }

  private static TableSchema widgetSchema() {
    return TableSchema.create(
        new TableId("mysql-source", "appdb", "widgets"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
        Instant.parse("2026-03-21T00:00:00Z"));
  }

  private static TableSchema jsonSchema() {
    return TableSchema.create(
        new TableId("mysql-source", "appdb", "typed_values"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("json_value", "json", NeutralColumnType.JSON, false, true)),
        Instant.parse("2026-03-21T00:00:00Z"));
  }

  private static TableSchema typedScalarSchema() {
    return TableSchema.create(
        new TableId("mysql-source", "appdb", "typed_values"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("event_date", "date", NeutralColumnType.DATE, false, true),
            new ColumnDefinition("event_time", "time", NeutralColumnType.TIME, false, true),
            new ColumnDefinition("updated_at", "timestamp", NeutralColumnType.TIMESTAMP, false, true),
            new ColumnDefinition("entity_uuid", "uuid", NeutralColumnType.UUID, false, true)),
        Instant.parse("2026-03-21T00:00:00Z"));
  }

  private static TableSchema datetimeSchema() {
    return TableSchema.create(
        new TableId("mysql-source", "appdb", "datetime_values"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition(
                "occurred_at", "datetime(6)", NeutralColumnType.TIMESTAMP, false, true)),
        Instant.parse("2026-03-21T00:00:00Z"));
  }

  private static TableSchema unsupportedPrimaryKeySchema() {
    return TableSchema.create(
        new TableId("mysql-source", "appdb", "weird_keys"),
        List.of(
            new ColumnDefinition("weird_id", "opaque", NeutralColumnType.UNSUPPORTED, true, false),
            new ColumnDefinition("ignored_payload", "opaque", NeutralColumnType.UNSUPPORTED, false, true),
            new ColumnDefinition(
                "display_name", "varchar(255)", NeutralColumnType.STRING, false, true)),
        Instant.parse("2026-03-21T00:00:00Z"));
  }

  private static Object[] row(long id, String name) {
    return new Object[] {id, name};
  }

  private static MySqlSourcePosition pos(long position) {
    return new MySqlSourcePosition("mysql-bin.000001", position, null);
  }

  private static final class StubStream implements MySqlBinlogStream {
    private final Deque<MySqlBinlogMessage> messages;

    private StubStream(List<MySqlBinlogMessage> messages) {
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
