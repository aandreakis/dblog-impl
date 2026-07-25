package io.github.aandreakis.dblog.adapter.postgres.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
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
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class JdbcPostgresChunkReaderTests {
  @Test
  void readsTimetzAsOffsetTimeBeforeNeutralNormalization() throws Exception {
    TableSchema schema = timetzSchema();
    Connection connection = configuredConnection();
    PreparedStatement statement = mock(PreparedStatement.class);
    ResultSet resultSet = mock(ResultSet.class);
    OffsetTime sourceValue =
        OffsetTime.of(LocalTime.of(10, 15, 30), ZoneOffset.ofHours(2));

    when(connection.prepareStatement(PostgresSql.tableChunkReadSql(schema, false, false)))
        .thenReturn(statement);
    when(statement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getObject(1)).thenReturn(1L);
    when(resultSet.getObject(2, OffsetTime.class)).thenReturn(sourceValue);

    Chunk chunk =
        new JdbcPostgresChunkReader()
            .nextTableChunk(connection, "job-timetz", schema, null, null, 1)
            .orElseThrow();

    assertThat(chunk.rows().getFirst())
        .containsEntry("observed_at", LocalTime.of(10, 15, 30));
    verify(resultSet).getObject(2, OffsetTime.class);
  }

  /**
   * A bare {@code getObject} on a {@code time(n)} column hands back a {@link java.sql.Time}, whose
   * {@code toLocalTime()} drops the whole sub-second field. The pgoutput path parses the same value
   * from text and keeps microseconds, so the chunk row and the log event for one row disagree —
   * and where {@code TIME} is part of the primary key the in-window collision is missed and the
   * stale snapshot row survives. Read {@code TIME} type-aware, exactly as {@code timetz} already is.
   */
  @Test
  void readsPlainTimeWithSubSecondPrecision() throws Exception {
    TableSchema schema = timeSchema();
    Connection connection = configuredConnection();
    PreparedStatement statement = mock(PreparedStatement.class);
    ResultSet resultSet = mock(ResultSet.class);
    LocalTime sourceValue = LocalTime.of(10, 15, 30, 123_456_000);

    when(connection.prepareStatement(PostgresSql.tableChunkReadSql(schema, false, false)))
        .thenReturn(statement);
    when(statement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getObject(1)).thenReturn(1L);
    // What an untyped read yields today: java.sql.Time cannot carry more than milliseconds.
    when(resultSet.getObject(2)).thenReturn(java.sql.Time.valueOf(sourceValue.withNano(0)));
    when(resultSet.getObject(2, LocalTime.class)).thenReturn(sourceValue);

    Chunk chunk =
        new JdbcPostgresChunkReader()
            .nextTableChunk(connection, "job-time", schema, null, null, 1)
            .orElseThrow();

    assertThat(chunk.rows().getFirst()).containsEntry("observed_at", sourceValue);
  }

  @Test
  void readsOrderedTableChunkThroughJdbcAndUsesLookaheadForFinalChunk() throws Exception {
    TableSchema schema = schemaWithIgnoredColumn();
    Connection connection = configuredConnection();
    PreparedStatement statement = mock(PreparedStatement.class);
    ResultSet resultSet = mock(ResultSet.class);

    when(connection.prepareStatement(PostgresSql.tableChunkReadSql(schema, false, true)))
        .thenReturn(statement);
    when(statement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, true, true, false);
    when(resultSet.getObject(1)).thenReturn(1L, 2L, 3L);
    when(resultSet.getObject(2)).thenReturn("one", "two", "three");

    Optional<Chunk> chunk =
        new JdbcPostgresChunkReader()
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

    when(connection.prepareStatement(PostgresSql.tableChunkReadSql(schema, false, false)))
        .thenReturn(statement);
    when(statement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getObject(1)).thenReturn(1L);
    when(resultSet.getObject(2)).thenReturn((Object) null);

    Chunk chunk =
        new JdbcPostgresChunkReader()
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

    when(connection.prepareStatement(PostgresSql.tableChunkReadSql(schema, true, true)))
        .thenReturn(statement);
    when(statement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, true, false);
    when(resultSet.getObject(1)).thenReturn(10L, 20L);
    when(resultSet.getObject(2)).thenReturn("ten", "twenty");

    Chunk chunk =
        new JdbcPostgresChunkReader()
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
  void readsTargetedRepairKeysInCanonicalRequestedOrder() throws Exception {
    TableSchema schema = numericSchema();
    Connection connection = configuredConnection();
    PreparedStatement statement = mock(PreparedStatement.class);
    ResultSet resultSet = mock(ResultSet.class);

    when(connection.prepareStatement(PostgresSql.targetedPrimaryKeysReadSql(schema, 3)))
        .thenReturn(statement);
    when(statement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, true, false);
    when(resultSet.getObject(1)).thenReturn(10L, 2L);
    when(resultSet.getObject(2)).thenReturn("ten", "two");

    Chunk chunk =
        new JdbcPostgresChunkReader()
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

    when(connection.prepareStatement(PostgresSql.targetedPrimaryKeysReadSql(schema, 1)))
        .thenReturn(statement);
    when(statement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getObject(1)).thenReturn("1a");
    when(resultSet.getObject(2)).thenReturn("coerced-match");

    assertThatThrownBy(
            () ->
                new JdbcPostgresChunkReader()
                    .targetedPrimaryKeyTuples(
                        connection, "job-3-drift", schema, schema.primaryKeyTuplesFromLiterals(List.of("1"))))
        .isInstanceOf(SchemaDriftException.class)
        .hasMessageContaining("neutral type");
  }

  @Test
  void buildsCompositePrimaryKeyChunkQueries() {
    TableSchema schema = compositeSchema();

    assertThat(PostgresSql.tableChunkReadSql(schema, true, true))
        .isEqualTo(
            "SELECT \"tenant_id\", \"widget_id\", \"name\" FROM \"public\".\"widgets\" WHERE (\"tenant_id\", \"widget_id\") > (?, ?) AND (\"tenant_id\", \"widget_id\") <= (?, ?) ORDER BY \"tenant_id\" ASC, \"widget_id\" ASC LIMIT ?");
    assertThat(PostgresSql.targetedPrimaryKeysReadSql(schema, 2))
        .isEqualTo(
            "SELECT \"tenant_id\", \"widget_id\", \"name\" FROM \"public\".\"widgets\" WHERE (\"tenant_id\", \"widget_id\") IN ((?, ?), (?, ?))");
    assertThat(PostgresSql.tableChunkReadSql(schema, true, true))
        .isSameAs(PostgresSql.tableChunkReadSql(schema, true, true));
    assertThat(PostgresSql.targetedPrimaryKeysReadSql(schema, 2))
        .isSameAs(PostgresSql.targetedPrimaryKeysReadSql(schema, 2));
  }

  @Test
  void returnsEmptyWithoutQueryingWhenNoTargetedKeysAreRequested() throws Exception {
    Connection connection = configuredConnection();

    assertThat(
            new JdbcPostgresChunkReader()
                .targetedPrimaryKeyTuples(connection, "job-4", numericSchema(), List.of()))
        .isEmpty();

    verifyNoInteractions(connection);
  }

  private static Connection configuredConnection() throws Exception {
    return mock(Connection.class);
  }

  private static TableSchema numericSchema() {
    return TableSchema.create(
        new TableId("appdb", "public", "widgets"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true)),
        Instant.parse("2026-03-20T00:00:00Z"));
  }

  private static TableSchema timetzSchema() {
    return TableSchema.create(
        new TableId("appdb", "public", "timed_widgets"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition(
                "observed_at", "time with time zone", NeutralColumnType.TIME, false, false)),
        Instant.parse("2026-03-20T00:00:00Z"));
  }

  private static TableSchema timeSchema() {
    return TableSchema.create(
        new TableId("appdb", "public", "timed_widgets"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("observed_at", "time(6)", NeutralColumnType.TIME, false, false)),
        Instant.parse("2026-03-20T00:00:00Z"));
  }

  private static TableSchema schemaWithIgnoredColumn() {
    return TableSchema.create(
        new TableId("appdb", "public", "widgets"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true),
            new ColumnDefinition("geom", "geometry", NeutralColumnType.UNSUPPORTED, false, true)),
        Instant.parse("2026-03-20T00:00:00Z"));
  }

  private static TableSchema compositeSchema() {
    return TableSchema.create(
        new TableId("appdb", "public", "widgets"),
        List.of(
            new ColumnDefinition("tenant_id", "text", NeutralColumnType.STRING, true, false),
            new ColumnDefinition("widget_id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true)),
        Instant.parse("2026-03-20T00:00:00Z"));
  }
}
