package io.github.aandreakis.dblog.adapter.mysql.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import io.github.aandreakis.dblog.core.schema.SchemaDriftException;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class JdbcMySqlChunkReaderTests {
  @Test
  void readsOrderedTableChunkThroughJdbcAndUsesLookaheadForFinalChunk() throws Exception {
    TableSchema schema = schemaWithIgnoredColumn();
    Connection connection = configuredConnection();
    PreparedStatement statement = mock(PreparedStatement.class);
    ResultSet resultSet = mock(ResultSet.class);

    when(connection.prepareStatement(MySqlSql.tableChunkReadSql(schema, false, true)))
        .thenReturn(statement);
    when(statement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, true, true, false);
    when(resultSet.getObject(1)).thenReturn(1L, 2L, 3L);
    when(resultSet.getObject(2)).thenReturn("one", "two", "three");

    Optional<Chunk> chunk =
        new JdbcMySqlChunkReader()
            .nextTableChunk(connection, "job-1", schema, null, schema.primaryKeyTupleFromLiteral("3"), 2);

    assertThat(chunk).isPresent();
    assertThat(chunk.orElseThrow().rows()).hasSize(2);
    assertThat(chunk.orElseThrow().rows().get(0)).containsEntry("id", 1L).containsEntry("name", "one");
    assertThat(chunk.orElseThrow().rows().get(0)).doesNotContainKey("geom");
    assertThat(chunk.orElseThrow().lastPrimaryKey()).isEqualTo("2");
    assertThat(chunk.orElseThrow().finalChunk()).isFalse();

    verify(statement).setObject(1, 3L);
    verify(statement).setInt(2, 3);
  }

  @Test
  void preservesNullableSelectedColumnValuesInChunkRows() throws Exception {
    TableSchema schema = numericSchema();
    Connection connection = configuredConnection();
    PreparedStatement statement = mock(PreparedStatement.class);
    ResultSet resultSet = mock(ResultSet.class);

    when(connection.prepareStatement(MySqlSql.tableChunkReadSql(schema, false, false)))
        .thenReturn(statement);
    when(statement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getObject(1)).thenReturn(1L);
    when(resultSet.getObject(2)).thenReturn((Object) null);

    Chunk chunk =
        new JdbcMySqlChunkReader()
            .nextTableChunk(connection, "job-null", schema, (PrimaryKeyTuple) null, (PrimaryKeyTuple) null, 1)
            .orElseThrow();

    assertThat(chunk.rows()).hasSize(1);
    assertThat(chunk.rows().getFirst()).containsEntry("id", 1L).containsEntry("name", null);
    assertThat(chunk.finalChunk()).isTrue();
  }

  @Test
  void canonicalizesResumeAndUpperBoundBeforeBindingTableChunk() throws Exception {
    TableSchema schema = numericSchema();
    Connection connection = configuredConnection();
    PreparedStatement statement = mock(PreparedStatement.class);
    ResultSet resultSet = mock(ResultSet.class);

    when(connection.prepareStatement(MySqlSql.tableChunkReadSql(schema, true, true)))
        .thenReturn(statement);
    when(statement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, true, false);
    when(resultSet.getObject(1)).thenReturn(10L, 20L);
    when(resultSet.getObject(2)).thenReturn("ten", "twenty");

    Chunk chunk =
        new JdbcMySqlChunkReader()
            .nextTableChunk(
                connection,
                "job-2",
                schema,
                schema.primaryKeyTupleFromLiteral("02"),
                schema.primaryKeyTupleFromLiteral("020"),
                2)
            .orElseThrow();

    assertThat(chunk.startAfterPrimaryKey()).isEqualTo("2");
    assertThat(chunk.rows()).extracting(row -> row.get("id")).containsExactly(10L, 20L);
    assertThat(chunk.lastPrimaryKey()).isEqualTo("20");
    assertThat(chunk.finalChunk()).isTrue();

    verify(statement).setObject(1, 2L);
    verify(statement).setObject(2, 20L);
    verify(statement).setInt(3, 3);
  }

  @Test
  void readsTableScanUpperBoundPrimaryKeyThroughJdbc() throws Exception {
    TableSchema schema = numericSchema();
    Connection connection = configuredConnection();
    PreparedStatement statement = mock(PreparedStatement.class);
    ResultSet resultSet = mock(ResultSet.class);

    when(connection.prepareStatement(MySqlSql.tableScanUpperBoundPrimaryKeySql(schema)))
        .thenReturn(statement);
    when(statement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getObject(1)).thenReturn(20L);

    assertThat(
            new JdbcMySqlChunkReader()
                .tableScanUpperBoundPrimaryKeyTuple(connection, schema)
                .map(PrimaryKeyTuple::literal))
        .contains("20");
  }

  @Test
  void readsTargetedRepairKeysInCanonicalRequestedOrder() throws Exception {
    TableSchema schema = numericSchema();
    Connection connection = configuredConnection();
    PreparedStatement statement = mock(PreparedStatement.class);
    ResultSet resultSet = mock(ResultSet.class);

    when(connection.prepareStatement(MySqlSql.targetedPrimaryKeysReadSql(schema, 3)))
        .thenReturn(statement);
    when(statement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, true, false);
    when(resultSet.getObject(1)).thenReturn(10L, 2L);
    when(resultSet.getObject(2)).thenReturn("ten", "two");

    Chunk chunk =
        new JdbcMySqlChunkReader()
            .targetedPrimaryKeyTuples(
                connection, "job-3", schema, schema.primaryKeyTuplesFromLiterals(List.of("02", "2", "10", "99")))
            .orElseThrow();

    assertThat(chunk.rows()).extracting(row -> row.get("id")).containsExactly(2L, 10L);
    assertThat(chunk.lastPrimaryKey()).isEqualTo("10");
    verify(statement).setObject(1, 2L);
    verify(statement).setObject(2, 10L);
    verify(statement).setObject(3, 99L);
  }

  @Test
  void failsClosedWhenTargetedRepairWidensPrimaryKeyDecode() throws Exception {
    TableSchema schema = numericSchema();
    Connection connection = configuredConnection();
    PreparedStatement statement = mock(PreparedStatement.class);
    ResultSet resultSet = mock(ResultSet.class);

    when(connection.prepareStatement(MySqlSql.targetedPrimaryKeysReadSql(schema, 1)))
        .thenReturn(statement);
    when(statement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getObject(1)).thenReturn("1a");
    when(resultSet.getObject(2)).thenReturn("coerced-match");

    assertThatThrownBy(
            () ->
                new JdbcMySqlChunkReader()
                    .targetedPrimaryKeyTuples(
                        connection, "job-3-drift", schema, schema.primaryKeyTuplesFromLiterals(List.of("1"))))
        .isInstanceOf(SchemaDriftException.class)
        .hasMessageContaining("neutral type");
  }

  @Test
  void doesNotRequeryEveryStringKeyWhenTheBulkTargetedReadFindsNothing() throws Exception {
    TableSchema schema = stringSchema();
    Connection connection = configuredConnection();
    PreparedStatement bulkStatement = mock(PreparedStatement.class);
    PreparedStatement fallbackStatement = mock(PreparedStatement.class);
    ResultSet bulkResultSet = mock(ResultSet.class);
    ResultSet fallbackResultSet = mock(ResultSet.class);

    when(connection.prepareStatement(MySqlSql.targetedPrimaryKeysReadSql(schema, 2)))
        .thenReturn(bulkStatement);
    when(connection.prepareStatement(MySqlSql.targetedPrimaryKeysReadSql(schema, 1)))
        .thenReturn(fallbackStatement);
    when(bulkStatement.executeQuery()).thenReturn(bulkResultSet);
    when(fallbackStatement.executeQuery()).thenReturn(fallbackResultSet);
    when(bulkResultSet.next()).thenReturn(false);
    when(fallbackResultSet.next()).thenReturn(false);

    assertThat(
            new JdbcMySqlChunkReader()
                .targetedPrimaryKeyTuples(
                    connection,
                    "job-missing-strings",
                    schema,
                    schema.primaryKeyTuplesFromLiterals(List.of("missing-a", "missing-b"))))
        .isEmpty();

    verify(connection, times(1))
        .prepareStatement(org.mockito.ArgumentMatchers.anyString());
  }

  @Test
  void buildsCompositePrimaryKeyChunkQueries() {
    TableSchema schema = compositeSchema();

    assertThat(MySqlSql.tableChunkReadSql(schema, true, true))
        .isEqualTo(
            "SELECT `tenant_id`, `widget_id`, `name` FROM `appdb`.`widgets` WHERE (`tenant_id`, `widget_id`) > (?, ?) AND (`tenant_id`, `widget_id`) <= (?, ?) ORDER BY `tenant_id` ASC, `widget_id` ASC LIMIT ?");
    assertThat(MySqlSql.targetedPrimaryKeysReadSql(schema, 2))
        .isEqualTo(
            "SELECT `tenant_id`, `widget_id`, `name` FROM `appdb`.`widgets` WHERE (`tenant_id`, `widget_id`) IN ((?, ?), (?, ?))");
    assertThat(MySqlSql.tableChunkReadSql(schema, true, true))
        .isSameAs(MySqlSql.tableChunkReadSql(schema, true, true));
    assertThat(MySqlSql.targetedPrimaryKeysReadSql(schema, 2))
        .isSameAs(MySqlSql.targetedPrimaryKeysReadSql(schema, 2));
  }

  @Test
  void readsMysqlTimestampColumnsAsUtcInstantsToAlignWithBinlogPath() throws Exception {
    // MySQL TIMESTAMP is UTC-stored but session-TZ-displayed. The binlog decoder always
    // emits UTC Instants; the chunk reader must match by pinning the TIMESTAMP read to
    // UTC via a UTC Calendar rather than letting bare getObject return a session-local
    // LocalDateTime. Without this alignment the apply sink sees two different
    // representations for the same row within the same watermark window and Postgres
    // TIMESTAMPTZ drifts by the client JVM's default offset.
    TableSchema schema = timestampSchema();
    Connection connection = configuredConnection();
    PreparedStatement statement = mock(PreparedStatement.class);
    ResultSet resultSet = mock(ResultSet.class);
    Instant capturedInstant = Instant.parse("2026-04-17T00:53:21Z");

    when(connection.prepareStatement(MySqlSql.tableChunkReadSql(schema, false, false)))
        .thenReturn(statement);
    when(statement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getObject(1)).thenReturn(1L);
    when(resultSet.getTimestamp(org.mockito.ArgumentMatchers.eq(2), utcCalendarMatcher()))
        .thenReturn(java.sql.Timestamp.from(capturedInstant));

    Chunk chunk =
        new JdbcMySqlChunkReader()
            .nextTableChunk(connection, "job-ts", schema, (PrimaryKeyTuple) null, (PrimaryKeyTuple) null, 1)
            .orElseThrow();

    assertThat(chunk.rows().getFirst()).containsEntry("updated_at", capturedInstant);
    // The naked getObject path must NOT be used for TIMESTAMP columns. This guards against
    // a regression where the type-aware branch is removed and LocalDateTime leaks back in.
    org.mockito.Mockito.verify(resultSet, org.mockito.Mockito.never()).getObject(2);
    org.mockito.Mockito.verify(resultSet).getTimestamp(org.mockito.ArgumentMatchers.eq(2), utcCalendarMatcher());
  }

  @Test
  void readsMysqlDatetimeColumnsThroughTheZoneLessPath() throws Exception {
    // DATETIME is deliberately zone-less; keep it on the generic getObject path so the
    // LocalDateTime it returns continues to flow through the existing normalizer. Only
    // the zone-aware TIMESTAMP family needs the UTC-Calendar specialisation.
    TableSchema schema = datetimeSchema();
    Connection connection = configuredConnection();
    PreparedStatement statement = mock(PreparedStatement.class);
    ResultSet resultSet = mock(ResultSet.class);
    java.time.LocalDateTime wallClock = java.time.LocalDateTime.parse("2026-04-17T08:53:21");

    when(connection.prepareStatement(MySqlSql.tableChunkReadSql(schema, false, false)))
        .thenReturn(statement);
    when(statement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getObject(1)).thenReturn(1L);
    when(resultSet.getObject(2)).thenReturn(wallClock);

    Chunk chunk =
        new JdbcMySqlChunkReader()
            .nextTableChunk(connection, "job-dt", schema, (PrimaryKeyTuple) null, (PrimaryKeyTuple) null, 1)
            .orElseThrow();

    assertThat(chunk.rows().getFirst()).containsEntry("occurred_at", wallClock);
    org.mockito.Mockito.verify(resultSet, org.mockito.Mockito.never())
        .getTimestamp(org.mockito.ArgumentMatchers.eq(2), utcCalendarMatcher());
  }

  @Test
  void leavesNonTemporalColumnsOnTheGenericGetObjectPath() throws Exception {
    // Regression guard: only TIMESTAMP columns route through the UTC-Calendar branch.
    // Any other column (here BIGINT, VARCHAR) must keep using bare getObject so the
    // existing Connector/J type handling is preserved.
    TableSchema schema = numericSchema();
    Connection connection = configuredConnection();
    PreparedStatement statement = mock(PreparedStatement.class);
    ResultSet resultSet = mock(ResultSet.class);

    when(connection.prepareStatement(MySqlSql.tableChunkReadSql(schema, false, false)))
        .thenReturn(statement);
    when(statement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getObject(1)).thenReturn(42L);
    when(resultSet.getObject(2)).thenReturn("alice");

    new JdbcMySqlChunkReader()
        .nextTableChunk(connection, "job-plain", schema, (PrimaryKeyTuple) null, (PrimaryKeyTuple) null, 1)
        .orElseThrow();

    org.mockito.Mockito.verify(resultSet, org.mockito.Mockito.never())
        .getTimestamp(org.mockito.ArgumentMatchers.anyInt(), utcCalendarMatcher());
  }

  @Test
  void readsMysqlYearColumnsAsPlainIntegers() throws Exception {
    // MySQL YEAR maps to neutral INTEGER, but Connector/J returns YEAR columns as
    // java.sql.Date with the default yearIsDateType=true setting. The normalizer's
    // INTEGER path then tries to parse "2026-01-01" as a BigInteger and fails with
    // "Illegal embedded sign character", marking the entire ALL_TABLES dump FAILED.
    // Reading via getInt bypasses the Date conversion.
    TableSchema schema = yearSchema();
    Connection connection = configuredConnection();
    PreparedStatement statement = mock(PreparedStatement.class);
    ResultSet resultSet = mock(ResultSet.class);

    when(connection.prepareStatement(MySqlSql.tableChunkReadSql(schema, false, false)))
        .thenReturn(statement);
    when(statement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getObject(1)).thenReturn(1L);
    when(resultSet.getInt(2)).thenReturn(2026);
    when(resultSet.wasNull()).thenReturn(false);

    Chunk chunk =
        new JdbcMySqlChunkReader()
            .nextTableChunk(connection, "job-year", schema, (PrimaryKeyTuple) null, (PrimaryKeyTuple) null, 1)
            .orElseThrow();

    assertThat(chunk.rows().getFirst()).containsEntry("year_col", 2026L);
    // Must not route through naked getObject — that's the path that returns Date.
    org.mockito.Mockito.verify(resultSet, org.mockito.Mockito.never()).getObject(2);
    org.mockito.Mockito.verify(resultSet).getInt(2);
  }

  @Test
  void preservesNullYearValuesOnTheTypeAwareBranch() throws Exception {
    TableSchema schema = yearSchema();
    Connection connection = configuredConnection();
    PreparedStatement statement = mock(PreparedStatement.class);
    ResultSet resultSet = mock(ResultSet.class);

    when(connection.prepareStatement(MySqlSql.tableChunkReadSql(schema, false, false)))
        .thenReturn(statement);
    when(statement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getObject(1)).thenReturn(1L);
    when(resultSet.getInt(2)).thenReturn(0);
    when(resultSet.wasNull()).thenReturn(true);

    Chunk chunk =
        new JdbcMySqlChunkReader()
            .nextTableChunk(connection, "job-year-null", schema, (PrimaryKeyTuple) null, (PrimaryKeyTuple) null, 1)
            .orElseThrow();

    assertThat(chunk.rows().getFirst()).containsEntry("year_col", null);
  }

  @Test
  void preservesNullTimestampValuesOnTheTypeAwareBranch() throws Exception {
    // A SQL NULL for a TIMESTAMP column must still propagate as a Java null rather than
    // crashing the dump — getTimestamp(col, cal) returns null in that case.
    TableSchema schema = timestampSchema();
    Connection connection = configuredConnection();
    PreparedStatement statement = mock(PreparedStatement.class);
    ResultSet resultSet = mock(ResultSet.class);

    when(connection.prepareStatement(MySqlSql.tableChunkReadSql(schema, false, false)))
        .thenReturn(statement);
    when(statement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getObject(1)).thenReturn(1L);
    when(resultSet.getTimestamp(org.mockito.ArgumentMatchers.eq(2), utcCalendarMatcher()))
        .thenReturn(null);

    Chunk chunk =
        new JdbcMySqlChunkReader()
            .nextTableChunk(connection, "job-null-ts", schema, (PrimaryKeyTuple) null, (PrimaryKeyTuple) null, 1)
            .orElseThrow();

    assertThat(chunk.rows().getFirst()).containsEntry("updated_at", null);
  }

  private static java.util.Calendar utcCalendarMatcher() {
    return org.mockito.ArgumentMatchers.argThat(
        calendar -> calendar != null
            && "UTC".equals(calendar.getTimeZone().getID()));
  }

  @Test
  void returnsEmptyWithoutQueryingWhenNoTargetedKeysAreRequested() throws Exception {
    Connection connection = configuredConnection();

    assertThat(
            new JdbcMySqlChunkReader()
                .targetedPrimaryKeyTuples(connection, "job-4", numericSchema(), List.of()))
        .isEmpty();

    verifyNoInteractions(connection);
  }

  private static Connection configuredConnection() throws Exception {
    Connection connection = mock(Connection.class);
    return connection;
  }

  private static TableSchema numericSchema() {
    return TableSchema.create(
        new TableId("mysql-source", "appdb", "widgets"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
        Instant.parse("2026-03-21T00:00:00Z"));
  }

  private static TableSchema stringSchema() {
    return TableSchema.create(
        new TableId("mysql-source", "appdb", "string_widgets"),
        List.of(
            new ColumnDefinition("id", "varchar(255)", NeutralColumnType.STRING, true, false),
            new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
        Instant.parse("2026-03-21T00:00:00Z"));
  }

  private static TableSchema schemaWithIgnoredColumn() {
    return TableSchema.create(
        new TableId("mysql-source", "appdb", "widgets"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true),
            new ColumnDefinition("geom", "geometry", NeutralColumnType.UNSUPPORTED, false, true)),
        Instant.parse("2026-03-21T00:00:00Z"));
  }

  private static TableSchema compositeSchema() {
    return TableSchema.create(
        new TableId("mysql-source", "appdb", "widgets"),
        List.of(
            new ColumnDefinition("tenant_id", "varchar(255)", NeutralColumnType.STRING, true, false),
            new ColumnDefinition("widget_id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
        Instant.parse("2026-03-21T00:00:00Z"));
  }

  private static TableSchema timestampSchema() {
    return TableSchema.create(
        new TableId("mysql-source", "appdb", "widgets"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("updated_at", "timestamp", NeutralColumnType.TIMESTAMP, false, true)),
        Instant.parse("2026-03-21T00:00:00Z"));
  }

  private static TableSchema datetimeSchema() {
    return TableSchema.create(
        new TableId("mysql-source", "appdb", "widgets"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("occurred_at", "datetime", NeutralColumnType.TIMESTAMP, false, true)),
        Instant.parse("2026-03-21T00:00:00Z"));
  }

  private static TableSchema yearSchema() {
    return TableSchema.create(
        new TableId("mysql-source", "appdb", "widgets"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("year_col", "year", NeutralColumnType.INTEGER, false, true)),
        Instant.parse("2026-03-21T00:00:00Z"));
  }
}
