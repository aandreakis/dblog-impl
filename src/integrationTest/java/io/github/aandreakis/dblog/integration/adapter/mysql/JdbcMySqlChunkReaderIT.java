package io.github.aandreakis.dblog.integration.adapter.mysql;

import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlChunkReader;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.testsupport.LiveMySqlTestContainers;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalTime;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

@Tag("integration-docker")
class JdbcMySqlChunkReaderIT {
  /**
   * Proves against the real driver that a {@code TIME(n)} chunk read keeps its millisecond field.
   * Connector/J surfaces the column as {@link java.sql.Time}, which does carry milliseconds, but
   * {@code java.sql.Time#toLocalTime()} used to discard them, truncating every chunk-read
   * time-of-day to a whole second.
   *
   * <p>Milliseconds, not microseconds, is the right target here: the binlog path cannot do better.
   * {@code AbstractRowsEventDataDeserializer#deserializeTimeV2} constructs a {@code
   * new java.sql.Time(millis)}, so MySQL {@code TIME(4..6)} loses its last three digits on the log
   * path no matter what the chunk path does. Reading the chunk side at microsecond precision would
   * therefore make the snapshot row disagree with the log event for the same row instead of
   * closing the gap. Both paths now meet at the binlog's ceiling. (PostgreSQL is not limited this
   * way — pgoutput delivers text, so that adapter matches at full microsecond precision.)
   */
  @Test
  void readsFractionalTimeColumnsAtTheBinlogPrecisionCeiling() throws Exception {
    assumeDockerIsAvailable();

    try (MySQLContainer mysql = LiveMySqlTestContainers.newDefaultContainer()) {
      mysql.start();
      try (Connection connection =
              DriverManager.getConnection(
                  mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
          Statement statement = connection.createStatement()) {
        configureSqlConnection(connection);
        statement.execute(
            "CREATE TABLE appdb.fractional_widgets "
                + "(id BIGINT PRIMARY KEY, observed_at TIME(6) NOT NULL)");
        statement.execute(
            "INSERT INTO appdb.fractional_widgets (id, observed_at) "
                + "VALUES (1, '10:15:30.123456')");

        TableSchema schema =
            TableSchema.create(
                new TableId("sourceA", "appdb", "fractional_widgets"),
                List.of(
                    new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                    new ColumnDefinition(
                        "observed_at", "time(6)", NeutralColumnType.TIME, false, false)),
                Instant.parse("2026-03-21T00:00:00Z"));

        Chunk chunk =
            new JdbcMySqlChunkReader()
                .nextTableChunk(
                    connection,
                    "job-time",
                    schema,
                    (io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple) null,
                    null,
                    10)
                .orElseThrow();

        assertThat(chunk.rows())
            .singleElement()
            .satisfies(
                row ->
                    assertThat(row.get("observed_at"))
                        .isEqualTo(LocalTime.of(10, 15, 30, 123_000_000)));
      }
    }
  }

  @Test
  void readsOrderedAndTargetedChunksThroughJdbcOnRealMySql() throws Exception {
    assumeDockerIsAvailable();

    try (MySQLContainer mysql =
        new MySQLContainer(DockerImageName.parse("mysql:8.4"))
            .withDatabaseName("appdb")
            .withUsername("dblog")
            .withPassword("dblog")
            .withEnv("MYSQL_ROOT_PASSWORD", "root")) {
      mysql.start();
      try (Connection connection =
              DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
          Statement statement = connection.createStatement()) {
        configureSqlConnection(connection);
        statement.execute("CREATE TABLE appdb.widgets (id BIGINT PRIMARY KEY, name VARCHAR(255), geom TEXT)");
        statement.execute("INSERT INTO appdb.widgets (id, name, geom) VALUES (10, 'ten', 'ignored')");
        statement.execute("INSERT INTO appdb.widgets (id, name, geom) VALUES (2, 'two', 'ignored')");
        statement.execute("INSERT INTO appdb.widgets (id, name, geom) VALUES (1, 'one', 'ignored')");
        statement.execute("INSERT INTO appdb.widgets (id, name, geom) VALUES (20, 'twenty', 'ignored')");

        TableSchema schema =
            TableSchema.create(
                new TableId("sourceA", "appdb", "widgets"),
                List.of(
                    new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                    new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true),
                    new ColumnDefinition("geom", "geometry", NeutralColumnType.UNSUPPORTED, false, true)),
                Instant.parse("2026-03-21T00:00:00Z"));
        JdbcMySqlChunkReader reader = new JdbcMySqlChunkReader();

        Chunk firstChunk =
            reader
                .nextTableChunk(
                    connection,
                    "job-1",
                    schema,
                    (io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple) null,
                    (io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple) null,
                    2)
                .orElseThrow();
        assertThat(firstChunk.rows()).extracting(row -> row.get("id")).containsExactly(1L, 2L);
        assertThat(firstChunk.rows().getFirst()).doesNotContainKey("geom");
        assertThat(firstChunk.finalChunk()).isFalse();

        Chunk resumedChunk =
            reader
                .nextTableChunk(
                    connection, "job-1", schema, schema.primaryKeyTupleFromLiteral("02"), null, 2)
                .orElseThrow();
        assertThat(resumedChunk.startAfterPrimaryKey()).isEqualTo("2");
        assertThat(resumedChunk.rows()).extracting(row -> row.get("id")).containsExactly(10L, 20L);
        assertThat(resumedChunk.finalChunk()).isTrue();

        Chunk targetedChunk =
            reader
                .targetedPrimaryKeyTuples(
                    connection,
                    "job-2",
                    schema,
                    schema.primaryKeyTuplesFromLiterals(List.of("2", "2", "3", "1")))
                .orElseThrow();
        assertThat(targetedChunk.rows()).extracting(row -> row.get("id")).containsExactly(2L, 1L);
        assertThat(targetedChunk.finalChunk()).isTrue();
      }
    }
  }

  @Test
  void resumesStringPrimaryKeyChunksUsingTheSourceCollation() throws Exception {
    assumeDockerIsAvailable();

    try (MySQLContainer mysql =
        new MySQLContainer(DockerImageName.parse("mysql:8.4"))
            .withDatabaseName("appdb")
            .withUsername("dblog")
            .withPassword("dblog")
            .withEnv("MYSQL_ROOT_PASSWORD", "root")) {
      mysql.start();
      try (Connection connection =
              DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
          Statement statement = connection.createStatement()) {
        configureSqlConnection(connection);
        statement.execute(
            "CREATE TABLE appdb.collated_widgets ("
                + "id VARCHAR(32) COLLATE utf8mb4_0900_ai_ci PRIMARY KEY, name VARCHAR(255))");
        statement.execute(
            "INSERT INTO appdb.collated_widgets (id, name) VALUES "
                + "('a', 'first'), ('b', 'second'), ('Z', 'last')");

        TableSchema schema =
            TableSchema.create(
                new TableId("sourceA", "appdb", "collated_widgets"),
                List.of(
                    new ColumnDefinition(
                        "id", "varchar(32)", NeutralColumnType.STRING, true, false),
                    new ColumnDefinition(
                        "name", "varchar(255)", NeutralColumnType.STRING, false, true)),
                Instant.parse("2026-07-21T00:00:00Z"));
        JdbcMySqlChunkReader reader = new JdbcMySqlChunkReader();

        var upperBound =
            reader.tableScanUpperBoundPrimaryKeyTuple(connection, schema).orElseThrow();
        assertThat(upperBound.literal()).isEqualTo("Z");

        Chunk firstChunk =
            reader
                .nextTableChunk(connection, "job-collation", schema, null, upperBound, 1)
                .orElseThrow();
        assertThat(firstChunk.rows()).extracting(row -> row.get("id")).containsExactly("a");

        Chunk secondChunk =
            reader
                .nextTableChunk(
                    connection,
                    "job-collation",
                    schema,
                    firstChunk.lastPrimaryKeyTuple(),
                    upperBound,
                    1)
                .orElseThrow();
        assertThat(secondChunk.rows()).extracting(row -> row.get("id")).containsExactly("b");
        assertThat(secondChunk.finalChunk()).isFalse();

        Chunk finalChunk =
            reader
                .nextTableChunk(
                    connection,
                    "job-collation",
                    schema,
                    secondChunk.lastPrimaryKeyTuple(),
                    upperBound,
                    1)
                .orElseThrow();
        assertThat(finalChunk.rows()).extracting(row -> row.get("id")).containsExactly("Z");
        assertThat(finalChunk.finalChunk()).isTrue();

        Chunk targetedChunk =
            reader
                .targetedPrimaryKeyTuples(
                    connection,
                    "job-collation-targeted",
                    schema,
                    schema.primaryKeyTuplesFromLiterals(List.of("A")))
                .orElseThrow();
        assertThat(targetedChunk.rows()).extracting(row -> row.get("id")).containsExactly("a");
        assertThat(targetedChunk.matchedRequestedPrimaryKeyTuples())
            .extracting(tuple -> tuple.literal())
            .containsExactly("A");
      }
    }
  }

  private static void configureSqlConnection(Connection connection) throws Exception {
    connection.setAutoCommit(true);
    connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
  }

}
