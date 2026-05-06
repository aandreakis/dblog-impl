package io.github.aandreakis.dblog.integration.adapter.mysql;

import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlChunkReader;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

@Tag("integration-docker")
class JdbcMySqlChunkReaderIT {
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

  private static void configureSqlConnection(Connection connection) throws Exception {
    connection.setAutoCommit(true);
    connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
  }

}
