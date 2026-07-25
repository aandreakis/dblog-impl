package io.github.aandreakis.dblog.sink.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.testsupport.ChangeEventTestFixtures;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TimeZone;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class JdbcApplyChangeEventSinkTests {
  @Test
  void filtersPureControlEventsWithoutOpeningAConnection() throws Exception {
    java.util.concurrent.atomic.AtomicInteger openCalls = new java.util.concurrent.atomic.AtomicInteger();

    try (JdbcApplyChangeEventSink sink =
        new JdbcApplyChangeEventSink(
            JdbcApplyTargetDialect.POSTGRES,
            "org.h2.Driver",
            "jdbc:h2:mem:control-events-only",
            () -> {
              openCalls.incrementAndGet();
              return mock(Connection.class);
            },
            TargetTableResolver.identity(),
            staticTargetSchema(sampleOrdersTargetMetadata("app", "demo", "sample_orders")))) {
      sink.appendEvents(
          List.of(
              controlEvent(new TableId("app", "demo", "sample_orders"), OperationType.WATERMARK),
              controlEvent(new TableId("app", "demo", "sample_orders"), OperationType.HEARTBEAT)));
    }

    assertThat(openCalls).hasValue(0);
  }

  @Test
  void buildsPostgresPreparedStatementsWithOnConflictApplyShape() throws Exception {
    Connection connection = mockBatchCapableConnection();
    PreparedStatement upsertStatement = mock(PreparedStatement.class);
    PreparedStatement deleteStatement = mock(PreparedStatement.class);
    when(connection.getAutoCommit()).thenReturn(true);
    when(upsertStatement.getConnection()).thenReturn(connection);
    when(deleteStatement.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(upsertStatement, deleteStatement);
    when(upsertStatement.executeBatch()).thenReturn(new int[] {1, 1});
    when(deleteStatement.executeBatch()).thenReturn(new int[] {1});

    try (JdbcApplyChangeEventSink sink =
        new JdbcApplyChangeEventSink(
            JdbcApplyTargetDialect.POSTGRES,
            "org.h2.Driver",
            "jdbc:h2:mem:postgres-sql-shape",
            () -> connection,
            TargetTableResolver.identity(),
            staticTargetSchema(sampleOrdersTargetMetadata("app", "demo", "sample_orders")))) {
      sink.appendEvents(
          List.of(
              upsertEvent(new TableId("app", "demo", "sample_orders"), CaptureOrigin.LOG, 1L, "alice", "NEW"),
              upsertEvent(new TableId("app", "demo", "sample_orders"), CaptureOrigin.SELECT, 1L, "alice", "SHIPPED"),
              deleteEvent(new TableId("app", "demo", "sample_orders"), 1L)));
    }

    org.mockito.ArgumentCaptor<String> sqlCaptor = org.mockito.ArgumentCaptor.forClass(String.class);
    verify(connection, times(2)).prepareStatement(sqlCaptor.capture());
    assertThat(sqlCaptor.getAllValues().get(0))
        .contains("INSERT INTO \"demo\".\"sample_orders\"")
        .contains("ON CONFLICT (\"id\") DO UPDATE SET")
        .contains("\"customer_name\" = EXCLUDED.\"customer_name\"");
    assertThat(sqlCaptor.getAllValues().get(1))
        .isEqualTo("DELETE FROM \"demo\".\"sample_orders\" WHERE \"id\" = ?");
  }

  @Test
  void appliesUpsertsAndDeletesUsingMySqlDialect() throws Exception {
    Connection connection = mockBatchCapableConnection();
    PreparedStatement upsertStatement = mock(PreparedStatement.class);
    PreparedStatement deleteStatement = mock(PreparedStatement.class);
    when(connection.getAutoCommit()).thenReturn(true);
    when(upsertStatement.getConnection()).thenReturn(connection);
    when(deleteStatement.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(upsertStatement, deleteStatement);
    when(upsertStatement.executeBatch()).thenReturn(new int[] {1, 1});
    when(deleteStatement.executeBatch()).thenReturn(new int[] {1});

    try (JdbcApplyChangeEventSink sink =
        new JdbcApplyChangeEventSink(
            JdbcApplyTargetDialect.MYSQL,
            "org.h2.Driver",
            "jdbc:h2:mem:mysql-sql-shape",
            () -> connection,
            TargetTableResolver.identity(),
            staticTargetSchema(sampleOrdersTargetMetadata("mysql_source", "app", "sample_orders")))) {
      sink.appendEvents(
          List.of(
              upsertEvent(new TableId("mysql_source", "app", "sample_orders"), CaptureOrigin.LOG, 1L, "bob", "NEW"),
              upsertEvent(new TableId("mysql_source", "app", "sample_orders"), CaptureOrigin.LOG, 1L, "bob", "DONE"),
              deleteEvent(new TableId("mysql_source", "app", "sample_orders"), 1L)));
    }

    org.mockito.ArgumentCaptor<String> sqlCaptor = org.mockito.ArgumentCaptor.forClass(String.class);
    verify(connection, times(2)).prepareStatement(sqlCaptor.capture());
    assertThat(sqlCaptor.getAllValues().get(0))
        .contains("INSERT INTO `app`.`sample_orders`")
        .contains("VALUES (?, ?, ?, ?) AS new_row ON DUPLICATE KEY UPDATE")
        .contains("`customer_name` = new_row.`customer_name`")
        .contains("`status` = new_row.`status`");
    assertThat(sqlCaptor.getAllValues().get(1))
        .isEqualTo("DELETE FROM `app`.`sample_orders` WHERE `id` = ?");
  }

  @Test
  void batchesContiguousCompatibleStatementsThroughPreparedStatement() throws Exception {
    Connection connection = mockBatchCapableConnection();
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(connection.getAutoCommit()).thenReturn(true);
    when(preparedStatement.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(org.mockito.ArgumentMatchers.anyString()))
        .thenReturn(preparedStatement);
    when(preparedStatement.executeBatch()).thenReturn(new int[] {1, 1});

    try (JdbcApplyChangeEventSink sink =
        new JdbcApplyChangeEventSink(
            JdbcApplyTargetDialect.POSTGRES,
            "org.h2.Driver",
            "jdbc:h2:mem:prepared-batch",
            () -> connection,
            TargetTableResolver.identity(),
            staticTargetSchema(sampleOrdersTargetMetadata("app", "demo", "sample_orders")))) {
      sink.appendEvents(
          List.of(
              upsertEvent(new TableId("app", "demo", "sample_orders"), CaptureOrigin.LOG, 1L, "alice", "NEW"),
              upsertEvent(new TableId("app", "demo", "sample_orders"), CaptureOrigin.LOG, 2L, "bob", "NEW")));
    }

    verify(connection).setAutoCommit(false);
    verify(connection).commit();
    verify(connection).setAutoCommit(true);
    verify(connection, times(1)).prepareStatement(org.mockito.ArgumentMatchers.anyString());
    verify(preparedStatement, times(2)).addBatch();
    verify(preparedStatement, times(1)).executeBatch();
  }

  @Test
  void collapsesDuplicatePrimaryKeysWithinOneBatchKeepingTheLastEvent() throws Exception {
    // During a watermark window the same PK can legitimately appear in one sink batch as
    // a LOG-origin binlog event and as a SELECT-origin refresh row. Postgres 15+ rejects
    // an INSERT...ON CONFLICT DO UPDATE where the same conflict target row would be
    // affected twice ("ON CONFLICT DO UPDATE command cannot affect row a second time").
    // The sink collapses duplicates and keeps the last event (spec §14 last-write-wins).
    Connection connection = mockBatchCapableConnection();
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(connection.getAutoCommit()).thenReturn(true);
    when(preparedStatement.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    when(preparedStatement.executeBatch()).thenReturn(new int[] {1, 1});

    TableId tableId = new TableId("app", "demo", "sample_orders");
    Instant earlier = Instant.parse("2026-03-28T12:00:00Z");
    Instant later = Instant.parse("2026-03-28T12:00:05Z");

    try (JdbcApplyChangeEventSink sink =
        new JdbcApplyChangeEventSink(
            JdbcApplyTargetDialect.POSTGRES,
            "org.h2.Driver",
            "jdbc:h2:mem:pk-dedup-pair",
            () -> connection,
            TargetTableResolver.identity(),
            staticTargetSchema(sampleOrdersTargetMetadata("app", "demo", "sample_orders")))) {
      sink.appendEvents(
          List.of(
              upsertEvent(tableId, CaptureOrigin.LOG, 1L, "alice", "NEW", earlier),
              upsertEvent(tableId, CaptureOrigin.LOG, 2L, "bob", "NEW", earlier),
              upsertEvent(tableId, CaptureOrigin.SELECT, 1L, "alice", "SHIPPED", later),
              upsertEvent(tableId, CaptureOrigin.SELECT, 2L, "bob", "DONE", later)));
    }

    // Two unique PKs survive the collapse: PK=1 and PK=2, each with the later values.
    verify(preparedStatement, times(2)).addBatch();
    verify(preparedStatement, times(1)).executeBatch();
    // The last event's status values are what get bound.
    verify(preparedStatement).setString(3, "SHIPPED");
    verify(preparedStatement).setString(3, "DONE");
    // The earlier "NEW" status values from the LOG events must not appear in this batch.
    org.mockito.Mockito.verify(preparedStatement, org.mockito.Mockito.never())
        .setString(3, "NEW");
  }

  @Test
  void collapsesTripleEventsForSamePrimaryKeyKeepingTheLastValues() throws Exception {
    Connection connection = mockBatchCapableConnection();
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(connection.getAutoCommit()).thenReturn(true);
    when(preparedStatement.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    when(preparedStatement.executeBatch()).thenReturn(new int[] {1});

    TableId tableId = new TableId("app", "demo", "sample_orders");

    try (JdbcApplyChangeEventSink sink =
        new JdbcApplyChangeEventSink(
            JdbcApplyTargetDialect.POSTGRES,
            "org.h2.Driver",
            "jdbc:h2:mem:pk-dedup-triple",
            () -> connection,
            TargetTableResolver.identity(),
            staticTargetSchema(sampleOrdersTargetMetadata("app", "demo", "sample_orders")))) {
      sink.appendEvents(
          List.of(
              upsertEvent(tableId, CaptureOrigin.LOG, 1L, "alice-v1", "NEW"),
              upsertEvent(tableId, CaptureOrigin.LOG, 1L, "alice-v2", "PROCESSING"),
              upsertEvent(tableId, CaptureOrigin.SELECT, 1L, "alice-v3", "SHIPPED")));
    }

    verify(preparedStatement, times(1)).addBatch();
    verify(preparedStatement, times(1)).executeBatch();
    verify(preparedStatement).setString(2, "alice-v3");
    verify(preparedStatement).setString(3, "SHIPPED");
    org.mockito.Mockito.verify(preparedStatement, org.mockito.Mockito.never())
        .setString(2, "alice-v1");
    org.mockito.Mockito.verify(preparedStatement, org.mockito.Mockito.never())
        .setString(2, "alice-v2");
  }

  @Test
  void doesNotCollapseDistinctPrimaryKeysWithinOneBatch() throws Exception {
    Connection connection = mockBatchCapableConnection();
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(connection.getAutoCommit()).thenReturn(true);
    when(preparedStatement.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    when(preparedStatement.executeBatch()).thenReturn(new int[] {1, 1, 1});

    TableId tableId = new TableId("app", "demo", "sample_orders");

    try (JdbcApplyChangeEventSink sink =
        new JdbcApplyChangeEventSink(
            JdbcApplyTargetDialect.POSTGRES,
            "org.h2.Driver",
            "jdbc:h2:mem:pk-dedup-unique",
            () -> connection,
            TargetTableResolver.identity(),
            staticTargetSchema(sampleOrdersTargetMetadata("app", "demo", "sample_orders")))) {
      sink.appendEvents(
          List.of(
              upsertEvent(tableId, CaptureOrigin.LOG, 1L, "alice", "NEW"),
              upsertEvent(tableId, CaptureOrigin.LOG, 2L, "bob", "NEW"),
              upsertEvent(tableId, CaptureOrigin.LOG, 3L, "carol", "NEW")));
    }

    verify(preparedStatement, times(3)).addBatch();
    verify(preparedStatement, times(1)).executeBatch();
  }

  @Test
  void bindsArrayBackedOrderedRowsWithoutRepeatedMapShapeAssumptions() throws Exception {
    Connection connection = mockBatchCapableConnection();
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(connection.getAutoCommit()).thenReturn(true);
    when(preparedStatement.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    when(preparedStatement.executeBatch()).thenReturn(new int[] {1});

    TableId tableId = new TableId("app", "demo", "sample_orders");
    ChangeEvent orderedEvent =
        ChangeEventTestFixtures.fromRowMaps(
            tableId,
            OperationType.UPDATE,
            CaptureOrigin.LOG,
            Map.of("id", 1L),
            null,
            orderedMap("id", 1L, "customer_name", "alice", "status", "NEW", "updated_at",
                Instant.parse("2026-03-28T12:00:00Z")),
            new OpaqueSourcePosition("pos:1"),
            "tx-1",
            null);

    try (JdbcApplyChangeEventSink sink =
        new JdbcApplyChangeEventSink(
            JdbcApplyTargetDialect.POSTGRES,
            "org.h2.Driver",
            "jdbc:h2:mem:ordered-row-binding",
            () -> connection,
            TargetTableResolver.identity(),
            staticTargetSchema(sampleOrdersTargetMetadata("app", "demo", "sample_orders")))) {
      sink.appendEvents(List.of(orderedEvent));
    }

    verify(preparedStatement).setLong(1, 1L);
    verify(preparedStatement).setString(2, "alice");
    verify(preparedStatement).setString(3, "NEW");
    verify(preparedStatement).setObject(4, java.time.OffsetDateTime.ofInstant(
        Instant.parse("2026-03-28T12:00:00Z"), java.time.ZoneOffset.UTC));
    verify(preparedStatement).executeBatch();
  }

  @Test
  void closeSurfacesJdbcResourceCleanupFailures() {
    JdbcApplyChangeEventSink sink =
        new JdbcApplyChangeEventSink(
            JdbcApplyTargetDialect.POSTGRES,
            "org.h2.Driver",
            "jdbc:h2:mem:close-failure",
            new JdbcApplyChangeEventSink.SqlConnectionSource() {
              @Override
              public Connection open() {
                return mock(Connection.class);
              }

              @Override
              public void close() {
                throw new RuntimeException("close boom");
              }
            },
            TargetTableResolver.identity(),
            staticTargetSchema(sampleOrdersTargetMetadata("app", "demo", "sample_orders")));

    assertThatThrownBy(sink::close)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Failed to close JDBC apply sink")
        .hasRootCauseMessage("close boom");
  }

  @Test
  void allowsNullPayloadValuesInUpsertEvents() throws Exception {
    Connection connection = mockBatchCapableConnection();
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(connection.getAutoCommit()).thenReturn(true);
    when(preparedStatement.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    when(preparedStatement.executeBatch()).thenReturn(new int[] {1});

    try (JdbcApplyChangeEventSink sink =
        new JdbcApplyChangeEventSink(
            JdbcApplyTargetDialect.POSTGRES,
            "org.h2.Driver",
            "jdbc:h2:mem:null-upsert",
            () -> connection,
            TargetTableResolver.identity(),
            staticTargetSchema(sampleOrdersTargetMetadata("app", "demo", "sample_orders")))) {
      Throwable failure =
          catchThrowable(
              () ->
                  sink.appendEvents(
                      List.of(
                          upsertEvent(
                              new TableId("app", "demo", "sample_orders"),
                              CaptureOrigin.SELECT,
                              1L,
                              "alice",
                              "NEW",
                              null))));
      assertThat(failure).isNull();
    }

    verify(preparedStatement).setObject(4, null);
    verify(preparedStatement).executeBatch();
  }

  @Test
  void bindsFractionalTimeThroughTheJdbc42LocalTimePath() throws Exception {
    Connection connection = mockBatchCapableConnection();
    PreparedStatement statement = mock(PreparedStatement.class);
    when(connection.getAutoCommit()).thenReturn(true);
    when(statement.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(statement);
    when(statement.executeBatch()).thenReturn(new int[] {1});

    TimeZone originalDefault = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Berlin"));
    try {
    TableId tableId = new TableId("source", "demo", "temporal_orders");
    JdbcApplyTargetSchemaInspector.TargetTableMetadata metadata =
        new JdbcApplyTargetSchemaInspector.TargetTableMetadata(
            tableId,
            List.of(
                targetColumn("id", "bigint", NeutralColumnType.INTEGER, true, 1),
                targetColumn("event_date", "date", NeutralColumnType.DATE, false, 0),
                targetColumn("event_time", "time", NeutralColumnType.TIME, false, 0),
                targetColumn("updated_at", "timestamp", NeutralColumnType.TIMESTAMP, false, 0)));

    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>();
    primaryKey.put("id", 1L);
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put("id", 1L);
    LocalDate dateValue = LocalDate.parse("2026-03-29");
    LocalTime timeValue = LocalTime.parse("12:34:56.789012");
    // 02:30 on 2026-03-29 does not exist in Europe/Berlin — the clocks jump 02:00 -> 03:00.
    // Timestamp.valueOf resolves such a wall clock through the JVM default zone and silently
    // slides it forward an hour; binding the LocalDateTime itself sends the literal wall clock.
    LocalDateTime timestampValue = LocalDateTime.parse("2026-03-29T02:30:00");
    afterRow.put("event_date", dateValue);
    afterRow.put("event_time", timeValue);
    afterRow.put("updated_at", timestampValue);

    try (JdbcApplyChangeEventSink sink =
        new JdbcApplyChangeEventSink(
            JdbcApplyTargetDialect.MYSQL,
            "org.h2.Driver",
            "jdbc:h2:mem:mysql-temporal-bindings",
            () -> connection,
            TargetTableResolver.identity(),
            staticTargetSchema(metadata))) {
      sink.appendEvents(
          List.of(
              ChangeEventTestFixtures.fromRowMaps(
                  tableId,
                  OperationType.UPDATE,
                  CaptureOrigin.LOG,
                  primaryKey,
                  null,
                  afterRow,
                  new OpaqueSourcePosition("pos:1"),
                  "tx-1",
                  null)));
    }

    verify(statement).setDate(2, Date.valueOf(dateValue));
    verify(statement).setObject(3, timeValue);
    verify(statement).setObject(4, timestampValue);
    } finally {
      TimeZone.setDefault(originalDefault);
    }
  }

  @Test
  void doesNotToggleAutoCommitWhenConnectionIsAlreadyTransactional() throws Exception {
    Connection connection = mockBatchCapableConnection();
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(connection.getAutoCommit()).thenReturn(false);
    when(preparedStatement.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    when(preparedStatement.executeBatch()).thenReturn(new int[] {1});

    try (JdbcApplyChangeEventSink sink =
        new JdbcApplyChangeEventSink(
            JdbcApplyTargetDialect.POSTGRES,
            "org.h2.Driver",
            "jdbc:h2:mem:transactional-connection",
            () -> connection,
            TargetTableResolver.identity(),
            staticTargetSchema(sampleOrdersTargetMetadata("app", "demo", "sample_orders")))) {
      sink.appendEvents(
          List.of(
              upsertEvent(
                  new TableId("app", "demo", "sample_orders"),
                  CaptureOrigin.LOG,
                  1L,
                  "alice",
                  "NEW")));
    }

    verify(connection, times(0)).setAutoCommit(false);
    verify(connection, times(0)).setAutoCommit(true);
    verify(connection).commit();
  }

  @Test
  void failsWhenUpsertEventHasNoAfterRow() throws Exception {
    Connection connection = mockBatchCapableConnection();
    when(connection.getAutoCommit()).thenReturn(true);
    ChangeEvent invalid =
        ChangeEventTestFixtures.fromRowMaps(
            new TableId("app", "demo", "sample_orders"),
            OperationType.UPDATE,
            CaptureOrigin.LOG,
            Map.of("id", 1L),
            null,
            null,
            new OpaqueSourcePosition("pos:1"),
            "tx-1",
            null);

    try (JdbcApplyChangeEventSink sink =
        new JdbcApplyChangeEventSink(
            JdbcApplyTargetDialect.POSTGRES,
            "org.h2.Driver",
            "jdbc:h2:mem:missing-after-row",
            () -> connection,
            TargetTableResolver.identity(),
            staticTargetSchema(sampleOrdersTargetMetadata("app", "demo", "sample_orders")))) {
      assertThatThrownBy(() -> sink.appendEvents(List.of(invalid)))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("requires afterRow");
    }
  }

  @Test
  void failsWhenAfterRowPrimaryKeyDoesNotMatchEventPrimaryKey() throws Exception {
    Connection connection = mockBatchCapableConnection();
    when(connection.getAutoCommit()).thenReturn(true);
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put("id", 2L);
    afterRow.put("customer_name", "alice");
    afterRow.put("status", "NEW");
    afterRow.put("updated_at", Instant.parse("2026-03-28T12:00:00Z"));
    ChangeEvent invalid =
        ChangeEventTestFixtures.fromRowMaps(
            new TableId("app", "demo", "sample_orders"),
            OperationType.UPDATE,
            CaptureOrigin.LOG,
            Map.of("id", 1L),
            null,
            afterRow,
            new OpaqueSourcePosition("pos:1"),
            "tx-1",
            null);

    try (JdbcApplyChangeEventSink sink =
        new JdbcApplyChangeEventSink(
            JdbcApplyTargetDialect.POSTGRES,
            "org.h2.Driver",
            "jdbc:h2:mem:pk-mismatch",
            () -> connection,
            TargetTableResolver.identity(),
            staticTargetSchema(sampleOrdersTargetMetadata("app", "demo", "sample_orders")))) {
      assertThatThrownBy(() -> sink.appendEvents(List.of(invalid)))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("primary key does not match");
    }
  }

  @Test
  void acceptsEqualBinaryPrimaryKeyContentFromDistinctArrays() throws Exception {
    Connection connection = mockBatchCapableConnection();
    PreparedStatement statement = mock(PreparedStatement.class);
    when(connection.getAutoCommit()).thenReturn(true);
    when(statement.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(statement);
    when(statement.executeBatch()).thenReturn(new int[] {1});

    TableId tableId = new TableId("source", "demo", "binary_accounts");
    JdbcApplyTargetSchemaInspector.TargetTableMetadata metadata =
        new JdbcApplyTargetSchemaInspector.TargetTableMetadata(
            tableId,
            List.of(
                targetColumn("id", "bytea", NeutralColumnType.BINARY, true, 1),
                targetColumn("name", "text", NeutralColumnType.STRING, false, 0)));
    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>();
    primaryKey.put("id", new byte[] {0x01, 0x02});
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put("id", new byte[] {0x01, 0x02});
    afterRow.put("name", "alice");
    ChangeEvent event =
        ChangeEventTestFixtures.fromRowMaps(
            tableId,
            OperationType.UPDATE,
            CaptureOrigin.LOG,
            primaryKey,
            null,
            afterRow,
            new OpaqueSourcePosition("pos:binary-1"),
            "tx-binary-1",
            null);

    try (JdbcApplyChangeEventSink sink =
        new JdbcApplyChangeEventSink(
            JdbcApplyTargetDialect.POSTGRES,
            "org.h2.Driver",
            "jdbc:h2:mem:binary-primary-key",
            () -> connection,
            TargetTableResolver.identity(),
            staticTargetSchema(metadata))) {
      sink.appendEvents(List.of(event));
    }

    verify(statement).addBatch();
    verify(statement).executeBatch();
  }

  @Test
  void failsHardWhenValueCannotBeCoercedToStrictTargetType() throws Exception {
    Connection connection = mockBatchCapableConnection();
    PreparedStatement statement = mock(PreparedStatement.class);
    when(connection.getAutoCommit()).thenReturn(true);
    when(statement.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(statement);

    TableId tableId = new TableId("source", "demo", "sample_orders");
    JdbcApplyTargetSchemaInspector.TargetTableMetadata metadata =
        new JdbcApplyTargetSchemaInspector.TargetTableMetadata(
            tableId,
            List.of(
                targetColumn("id", "bigint", NeutralColumnType.INTEGER, true, 1),
                targetColumn("customer_name", "integer", NeutralColumnType.INTEGER, false, 0),
                targetColumn("status", "varchar(64)", NeutralColumnType.STRING, false, 0),
                targetColumn("updated_at", "timestamp", NeutralColumnType.TIMESTAMP, false, 0)));

    try (JdbcApplyChangeEventSink sink =
        new JdbcApplyChangeEventSink(
            JdbcApplyTargetDialect.POSTGRES,
            "org.h2.Driver",
            "jdbc:h2:mem:strict-target-type",
            () -> connection,
            TargetTableResolver.identity(),
            staticTargetSchema(metadata))) {
      Throwable failure =
          catchThrowable(
              () ->
                  sink.appendEvents(
                      List.of(
                          upsertEvent(tableId, CaptureOrigin.LOG, 1L, "alice", "NEW"))));
      assertThat(failure).isInstanceOf(TargetApplyContractException.class);
      TargetApplyContractException exception = (TargetApplyContractException) failure;
      assertThat(exception.failure().type())
          .isEqualTo(TargetApplyFailureType.TARGET_VALUE_COERCION_FAILED);
      assertThat(exception.failure().columnName()).isEqualTo("customer_name");
      assertThat(exception.failure().targetNeutralType()).isEqualTo(NeutralColumnType.INTEGER);
    }
  }

  @Test
  void bindsSpecializedSetterPathsForMysqlTargets() throws Exception {
    Connection connection = mockBatchCapableConnection();
    PreparedStatement statement = mock(PreparedStatement.class);
    when(connection.getAutoCommit()).thenReturn(true);
    when(statement.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(statement);
    when(statement.executeBatch()).thenReturn(new int[] {1});

    TableId tableId = new TableId("source", "demo", "numeric_orders");
    JdbcApplyTargetSchemaInspector.TargetTableMetadata metadata =
        new JdbcApplyTargetSchemaInspector.TargetTableMetadata(
            tableId,
            List.of(
                targetColumn("id", "bigint", NeutralColumnType.INTEGER, true, 1),
                targetColumn("huge_counter", "decimal(65,0)", NeutralColumnType.INTEGER, false, 0),
                targetColumn("payload", "blob", NeutralColumnType.BINARY, false, 0),
                targetColumn("ratio", "double", NeutralColumnType.FLOAT, false, 0),
                targetColumn("happened_at", "timestamp", NeutralColumnType.TIMESTAMP, false, 0)));

    byte[] payload = new byte[] {1, 2, 3};
    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>();
    primaryKey.put("id", 1L);
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put("id", 1L);
    afterRow.put("huge_counter", new BigInteger("9223372036854775808"));
    afterRow.put("payload", payload);
    afterRow.put("ratio", new BigDecimal("1.25"));
    Instant happenedAt = Instant.parse("2026-03-29T12:34:56Z");
    afterRow.put("happened_at", happenedAt);

    try (JdbcApplyChangeEventSink sink =
        new JdbcApplyChangeEventSink(
            JdbcApplyTargetDialect.MYSQL,
            "org.h2.Driver",
            "jdbc:h2:mem:mysql-specialized-bindings",
            () -> connection,
            TargetTableResolver.identity(),
            staticTargetSchema(metadata))) {
      sink.appendEvents(
          List.of(
              ChangeEventTestFixtures.fromRowMaps(
                  tableId,
                  OperationType.UPDATE,
                  CaptureOrigin.LOG,
                  primaryKey,
                  null,
                  afterRow,
                  new OpaqueSourcePosition("pos:1"),
                  "tx-1",
                  null)));
    }

    verify(statement).setBigDecimal(eq(2), any(BigDecimal.class));
    verify(statement).setBytes(3, payload);
    verify(statement).setDouble(4, 1.25d);
    verify(statement).setTimestamp(5, Timestamp.from(happenedAt));
  }

  @Test
  void bindsCompositePrimaryKeyUpsertOnPostgresWithBothKeyColumnsInOrder() throws Exception {
    Connection connection = mockBatchCapableConnection();
    PreparedStatement statement = mock(PreparedStatement.class);
    when(connection.getAutoCommit()).thenReturn(true);
    when(statement.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(statement);
    when(statement.executeBatch()).thenReturn(new int[] {1});

    TableId tableId = new TableId("app", "demo", "accounts_by_region");

    try (JdbcApplyChangeEventSink sink =
        new JdbcApplyChangeEventSink(
            JdbcApplyTargetDialect.POSTGRES,
            "org.h2.Driver",
            "jdbc:h2:mem:composite-postgres-upsert",
            () -> connection,
            TargetTableResolver.identity(),
            staticTargetSchema(compositePkAccountsTargetMetadata(tableId)))) {
      sink.appendEvents(List.of(compositePkUpsertEvent(tableId, "acct-1", "EU", "alice")));
    }

    org.mockito.ArgumentCaptor<String> sqlCaptor = org.mockito.ArgumentCaptor.forClass(String.class);
    verify(connection).prepareStatement(sqlCaptor.capture());
    String sql = sqlCaptor.getValue();
    assertThat(sql)
        .contains("INSERT INTO \"demo\".\"accounts_by_region\"")
        .contains("ON CONFLICT (\"account_id\", \"region\") DO UPDATE SET")
        .contains("\"name\" = EXCLUDED.\"name\"");

    // Composite PK columns bind positionally in schema-declared order:
    // position 1 = account_id, position 2 = region, position 3 = name.
    verify(statement).setString(1, "acct-1");
    verify(statement).setString(2, "EU");
    verify(statement).setString(3, "alice");
  }

  @Test
  void bindsCompositePrimaryKeyDeleteOnMysqlWithBothKeyColumnsInOrder() throws Exception {
    Connection connection = mockBatchCapableConnection();
    PreparedStatement statement = mock(PreparedStatement.class);
    when(connection.getAutoCommit()).thenReturn(true);
    when(statement.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(statement);
    when(statement.executeBatch()).thenReturn(new int[] {1});

    TableId tableId = new TableId("mysql_source", "app", "accounts_by_region");

    try (JdbcApplyChangeEventSink sink =
        new JdbcApplyChangeEventSink(
            JdbcApplyTargetDialect.MYSQL,
            "org.h2.Driver",
            "jdbc:h2:mem:composite-mysql-delete",
            () -> connection,
            TargetTableResolver.identity(),
            staticTargetSchema(compositePkAccountsTargetMetadata(tableId)))) {
      sink.appendEvents(List.of(compositePkDeleteEvent(tableId, "acct-2", "US")));
    }

    org.mockito.ArgumentCaptor<String> sqlCaptor = org.mockito.ArgumentCaptor.forClass(String.class);
    verify(connection).prepareStatement(sqlCaptor.capture());
    assertThat(sqlCaptor.getValue())
        .isEqualTo(
            "DELETE FROM `app`.`accounts_by_region` "
                + "WHERE `account_id` = ? AND `region` = ?");

    verify(statement).setString(1, "acct-2");
    verify(statement).setString(2, "US");
  }

  @Test
  void failsWhenCompositePrimaryKeyContainsNullValue() throws Exception {
    Connection connection = mockBatchCapableConnection();
    when(connection.getAutoCommit()).thenReturn(true);

    TableId tableId = new TableId("app", "demo", "accounts_by_region");

    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>();
    primaryKey.put("account_id", "acct-1");
    primaryKey.put("region", null); // NULL in a composite PK column
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put("account_id", "acct-1");
    afterRow.put("region", null);
    afterRow.put("name", "alice");

    try (JdbcApplyChangeEventSink sink =
        new JdbcApplyChangeEventSink(
            JdbcApplyTargetDialect.POSTGRES,
            "org.h2.Driver",
            "jdbc:h2:mem:composite-null-pk",
            () -> connection,
            TargetTableResolver.identity(),
            staticTargetSchema(compositePkAccountsTargetMetadata(tableId)))) {
      ChangeEvent event =
          ChangeEventTestFixtures.fromRowMaps(
              tableId,
              OperationType.UPDATE,
              CaptureOrigin.LOG,
              primaryKey,
              null,
              afterRow,
              new OpaqueSourcePosition("pos:null-pk"),
              "tx-null-pk",
              null);
      assertThatThrownBy(() -> sink.appendEvents(List.of(event)))
          .isInstanceOfAny(RuntimeException.class);
    }
  }

  @Test
  void batchesCompositePrimaryKeyUpsertsWithStableBindingOrder() throws Exception {
    Connection connection = mockBatchCapableConnection();
    PreparedStatement statement = mock(PreparedStatement.class);
    when(connection.getAutoCommit()).thenReturn(true);
    when(statement.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(statement);
    when(statement.executeBatch()).thenReturn(new int[] {1, 1, 1});

    TableId tableId = new TableId("app", "demo", "accounts_by_region");

    try (JdbcApplyChangeEventSink sink =
        new JdbcApplyChangeEventSink(
            JdbcApplyTargetDialect.POSTGRES,
            "org.h2.Driver",
            "jdbc:h2:mem:composite-batch",
            () -> connection,
            TargetTableResolver.identity(),
            staticTargetSchema(compositePkAccountsTargetMetadata(tableId)))) {
      sink.appendEvents(
          List.of(
              compositePkUpsertEvent(tableId, "acct-1", "EU", "alice"),
              compositePkUpsertEvent(tableId, "acct-2", "US", "bob"),
              compositePkUpsertEvent(tableId, "acct-3", "APAC", "carol")));
    }

    // One compiled statement, three batch rows.
    verify(connection, times(1)).prepareStatement(anyString());
    verify(statement, times(3)).addBatch();
    verify(statement, times(1)).executeBatch();

    // Composite-PK binding order stays stable across batched rows.
    verify(statement).setString(1, "acct-1");
    verify(statement).setString(2, "EU");
    verify(statement).setString(3, "alice");
    verify(statement).setString(1, "acct-2");
    verify(statement).setString(2, "US");
    verify(statement).setString(3, "bob");
    verify(statement).setString(1, "acct-3");
    verify(statement).setString(2, "APAC");
    verify(statement).setString(3, "carol");
  }

  private static JdbcApplyTargetSchemaInspector.TargetTableMetadata compositePkAccountsTargetMetadata(
      TableId tableId) {
    return new JdbcApplyTargetSchemaInspector.TargetTableMetadata(
        tableId,
        List.of(
            targetColumn("account_id", "varchar(32)", NeutralColumnType.STRING, true, 1),
            targetColumn("region", "varchar(8)", NeutralColumnType.STRING, true, 2),
            targetColumn("name", "varchar(255)", NeutralColumnType.STRING, false, 0)));
  }

  private static ChangeEvent compositePkUpsertEvent(
      TableId tableId, String accountId, String region, String name) {
    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>();
    primaryKey.put("account_id", accountId);
    primaryKey.put("region", region);
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put("account_id", accountId);
    afterRow.put("region", region);
    afterRow.put("name", name);
    return ChangeEventTestFixtures.fromRowMaps(
        tableId,
        OperationType.UPDATE,
        CaptureOrigin.LOG,
        primaryKey,
        null,
        afterRow,
        new OpaqueSourcePosition("pos:" + accountId + "/" + region),
        "tx-" + accountId + "-" + region,
        null);
  }

  private static ChangeEvent compositePkDeleteEvent(
      TableId tableId, String accountId, String region) {
    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>();
    primaryKey.put("account_id", accountId);
    primaryKey.put("region", region);
    LinkedHashMap<String, Object> beforeRow = new LinkedHashMap<>();
    beforeRow.put("account_id", accountId);
    beforeRow.put("region", region);
    beforeRow.put("name", "pre-delete");
    return ChangeEventTestFixtures.fromRowMaps(
        tableId,
        OperationType.DELETE,
        CaptureOrigin.LOG,
        primaryKey,
        beforeRow,
        null,
        new OpaqueSourcePosition("pos:delete:" + accountId + "/" + region),
        "tx-delete-" + accountId + "-" + region,
        null);
  }

  private static ChangeEvent upsertEvent(
      TableId tableId,
      CaptureOrigin captureOrigin,
      long id,
      String customerName,
      String status) {
    return upsertEvent(
        tableId, captureOrigin, id, customerName, status, Instant.parse("2026-03-28T12:00:00Z"));
  }

  private static ChangeEvent upsertEvent(
      TableId tableId,
      CaptureOrigin captureOrigin,
      long id,
      String customerName,
      String status,
      Instant updatedAt) {
    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>();
    primaryKey.put("id", id);
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put("id", id);
    afterRow.put("customer_name", customerName);
    afterRow.put("status", status);
    afterRow.put("updated_at", updatedAt);
    return ChangeEventTestFixtures.fromRowMaps(
        tableId,
        OperationType.UPDATE,
        captureOrigin,
        primaryKey,
        null,
        afterRow,
        new OpaqueSourcePosition("pos:" + id),
        "tx-" + id,
        captureOrigin == CaptureOrigin.SELECT ? "dump-1" : null);
  }

  private static JdbcApplyChangeEventSink.TargetSchemaLookup staticTargetSchema(
      JdbcApplyTargetSchemaInspector.TargetTableMetadata metadata) {
    return (connection, dialect, tableId) -> Optional.of(metadata);
  }

  private static JdbcApplyTargetSchemaInspector.TargetTableMetadata sampleOrdersTargetMetadata(
      String databaseName, String schemaName, String tableName) {
    TableId tableId = new TableId(databaseName, schemaName, tableName);
    return new JdbcApplyTargetSchemaInspector.TargetTableMetadata(
        tableId,
        List.of(
            targetColumn("id", "bigint", NeutralColumnType.INTEGER, true, 1),
            targetColumn("customer_name", "varchar(255)", NeutralColumnType.STRING, false, 0),
            targetColumn("status", "varchar(64)", NeutralColumnType.STRING, false, 0),
            targetColumn("updated_at", "timestamp", NeutralColumnType.TIMESTAMP, false, 0)));
  }

  private static JdbcApplyTargetSchemaInspector.TargetColumnMetadata targetColumn(
      String name,
      String sourceType,
      NeutralColumnType neutralType,
      boolean primaryKey,
      int primaryKeyOrdinal) {
    return new JdbcApplyTargetSchemaInspector.TargetColumnMetadata(
        name, sourceType, neutralType, primaryKey, primaryKeyOrdinal, !primaryKey, null, sourceType, null);
  }

  private static ChangeEvent controlEvent(TableId tableId, OperationType operationType) {
    return ChangeEventTestFixtures.fromRowMaps(
        tableId,
        operationType,
        CaptureOrigin.LOG,
        Map.of("id", 1L),
        null,
        Map.of("id", 1L),
        new OpaqueSourcePosition("pos:control"),
        "tx-control",
        null);
  }

  private static ChangeEvent deleteEvent(TableId tableId, long id) {
    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>();
    primaryKey.put("id", id);
    return ChangeEventTestFixtures.fromRowMaps(
        tableId,
        OperationType.DELETE,
        CaptureOrigin.LOG,
        primaryKey,
        Map.of("id", id),
        null,
        new OpaqueSourcePosition("pos:delete:" + id),
        "tx-delete-" + id,
        null);
  }

  private static LinkedHashMap<String, Object> orderedMap(Object... kvPairs) {
    LinkedHashMap<String, Object> map = new LinkedHashMap<>();
    for (int index = 0; index < kvPairs.length; index += 2) {
      map.put((String) kvPairs[index], kvPairs[index + 1]);
    }
    return map;
  }

  private static Connection mockBatchCapableConnection() throws SQLException {
    Connection connection = mock(Connection.class);
    java.sql.DatabaseMetaData metaData = mock(java.sql.DatabaseMetaData.class);
    when(connection.getMetaData()).thenReturn(metaData);
    when(metaData.supportsBatchUpdates()).thenReturn(true);
    return connection;
  }
}
