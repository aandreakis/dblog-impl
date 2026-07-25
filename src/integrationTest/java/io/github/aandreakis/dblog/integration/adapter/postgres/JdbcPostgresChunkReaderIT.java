package io.github.aandreakis.dblog.integration.adapter.postgres;

import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.postgres.internal.JdbcPostgresChunkReader;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.testsupport.LivePostgresTestContainers;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalTime;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Tag("integration-docker")
class JdbcPostgresChunkReaderIT {
  /**
   * Proves against the real driver that a {@code time(n)} chunk read keeps its fractional second.
   * An untyped {@code getObject} yields a {@link java.sql.Time}, which cannot carry microseconds
   * and whose {@code toLocalTime()} drops milliseconds too, while the pgoutput path parses the
   * same value from text at full precision — so the snapshot row and the log event for one row
   * would disagree.
   */
  @Test
  void readsFractionalTimeColumnsThroughJdbcWithoutTruncation() throws Exception {
    assumeDockerIsAvailable();

    try (PostgreSQLContainer postgres = LivePostgresTestContainers.newBaseContainer()) {
      postgres.start();
      try (Connection connection =
              DriverManager.getConnection(
                  postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
          Statement statement = connection.createStatement()) {
        configureSqlConnection(connection);
        statement.execute(
            "CREATE TABLE public.fractional_widgets "
                + "(id BIGINT PRIMARY KEY, observed_at TIME(6) NOT NULL)");
        statement.execute(
            "INSERT INTO public.fractional_widgets (id, observed_at) "
                + "VALUES (1, TIME '10:15:30.123456')");

        TableSchema schema =
            TableSchema.create(
                new TableId(postgres.getDatabaseName(), "public", "fractional_widgets"),
                List.of(
                    new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                    new ColumnDefinition(
                        "observed_at", "time(6)", NeutralColumnType.TIME, false, false)),
                Instant.parse("2026-03-20T00:00:00Z"));

        Chunk chunk =
            new JdbcPostgresChunkReader()
                .nextTableChunk(connection, "job-time", schema, null, null, 10)
                .orElseThrow();

        assertThat(chunk.rows())
            .singleElement()
            .satisfies(
                row ->
                    assertThat(row.get("observed_at"))
                        .isEqualTo(LocalTime.of(10, 15, 30, 123_456_000)));
      }
    }
  }

  /**
   * Covers the source-equivalent key fallback in {@code targetedPrimaryKeyTuples}, which re-queries
   * per unmatched key so the source decides equality for collated text. Requesting {@code 'A'} must
   * repair the row stored as {@code 'a'} under a case-insensitive collation, and the chunk must
   * report the <em>requested</em> literal as matched so the repair is not reported missing.
   *
   * <p>Needs a nondeterministic ICU collation: PostgreSQL's default collation is case-sensitive, so
   * with an ordinary text key the fallback branch is never entered — which is why no existing
   * Postgres test reached it.
   */
  @Test
  void repairsCollationEquivalentStringPrimaryKeysThroughTheSourceFallback() throws Exception {
    assumeDockerIsAvailable();

    try (PostgreSQLContainer postgres = LivePostgresTestContainers.newBaseContainer()) {
      postgres.start();
      try (Connection connection =
              DriverManager.getConnection(
                  postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
          Statement statement = connection.createStatement()) {
        configureSqlConnection(connection);
        statement.execute(
            "CREATE COLLATION case_insensitive "
                + "(provider = icu, locale = 'und-u-ks-level2', deterministic = false)");
        statement.execute(
            "CREATE TABLE public.collated_widgets "
                + "(id TEXT COLLATE case_insensitive PRIMARY KEY, name TEXT)");
        statement.execute("INSERT INTO public.collated_widgets (id, name) VALUES ('a', 'lower a')");

        TableSchema schema =
            TableSchema.create(
                new TableId(postgres.getDatabaseName(), "public", "collated_widgets"),
                List.of(
                    new ColumnDefinition("id", "text", NeutralColumnType.STRING, true, false),
                    new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true)),
                Instant.parse("2026-03-20T00:00:00Z"));

        Chunk targetedChunk =
            new JdbcPostgresChunkReader()
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

  @Test
  void readsSupportedTimetzColumnsThroughJdbc() throws Exception {
    assumeDockerIsAvailable();

    try (PostgreSQLContainer postgres = LivePostgresTestContainers.newBaseContainer()) {
      postgres.start();
      try (Connection connection =
              DriverManager.getConnection(
                  postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
          Statement statement = connection.createStatement()) {
        configureSqlConnection(connection);
        statement.execute(
            "CREATE TABLE public.timed_widgets "
                + "(id BIGINT PRIMARY KEY, observed_at TIMETZ NOT NULL)");
        statement.execute(
            "INSERT INTO public.timed_widgets (id, observed_at) "
                + "VALUES (1, TIMETZ '10:15:30+02:00')");

        TableSchema schema =
            TableSchema.create(
                new TableId(postgres.getDatabaseName(), "public", "timed_widgets"),
                List.of(
                    new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                    new ColumnDefinition(
                        "observed_at", "timetz", NeutralColumnType.TIME, false, false)),
                Instant.parse("2026-03-20T00:00:00Z"));

        Chunk chunk =
            new JdbcPostgresChunkReader()
                .nextTableChunk(connection, "job-timetz", schema, null, null, 10)
                .orElseThrow();

        assertThat(chunk.rows())
            .singleElement()
            .satisfies(
                row ->
                    assertThat(row.get("observed_at"))
                        .isEqualTo(LocalTime.of(10, 15, 30)));
      }
    }
  }

  @Test
  void readsOrderedAndTargetedChunksThroughJdbcOnRealPostgres() throws Exception {
    assumeDockerIsAvailable();

    try (PostgreSQLContainer postgres = LivePostgresTestContainers.newBaseContainer()) {
      postgres.start();
      try (Connection connection =
              DriverManager.getConnection(
                  postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
          Statement statement = connection.createStatement()) {
        configureSqlConnection(connection);
        statement.execute("CREATE TABLE public.widgets (id BIGINT PRIMARY KEY, name TEXT, geom TEXT)");
        statement.execute("INSERT INTO public.widgets (id, name, geom) VALUES (10, 'ten', 'ignored')");
        statement.execute("INSERT INTO public.widgets (id, name, geom) VALUES (2, 'two', 'ignored')");
        statement.execute("INSERT INTO public.widgets (id, name, geom) VALUES (1, 'one', 'ignored')");
        statement.execute("INSERT INTO public.widgets (id, name, geom) VALUES (20, 'twenty', 'ignored')");

        TableSchema schema =
            TableSchema.create(
                new TableId(postgres.getDatabaseName(), "public", "widgets"),
                List.of(
                    new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                    new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true),
                    new ColumnDefinition("geom", "geometry", NeutralColumnType.UNSUPPORTED, false, true)),
                Instant.parse("2026-03-20T00:00:00Z"));
        JdbcPostgresChunkReader reader = new JdbcPostgresChunkReader();

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

  private static void configureSqlConnection(Connection connection) throws Exception {
    connection.setAutoCommit(true);
    connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
  }

}
