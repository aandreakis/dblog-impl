package io.github.aandreakis.dblog.integration.adapter.postgres;

import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.adapter.postgres.internal.JdbcPostgresWatermarkTableHelper;
import io.github.aandreakis.dblog.core.model.WatermarkToken;
import io.github.aandreakis.dblog.testsupport.LivePostgresTestContainers;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Tag("integration-docker")
class JdbcPostgresWatermarkTableHelperIT {
  private static PostgreSQLContainer postgres;

  @BeforeAll
  static void startPostgres() {
    assumeDockerIsAvailable();
    postgres = LivePostgresTestContainers.newBaseContainer();
    postgres.start();
  }

  @AfterAll
  static void stopPostgres() {
    if (postgres != null) {
      postgres.close();
    }
  }

  @BeforeEach
  void resetMetadataSchema() throws Exception {
    try (Connection connection = openAdminConnection();
        Statement statement = connection.createStatement()) {
      configureSqlConnection(connection);
      statement.execute("DROP SCHEMA IF EXISTS dblog_meta CASCADE");
    }
  }

  @Test
  void ensuresSingletonMetadataTableAndWritesWatermarkTokens() throws Exception {
    try (Connection connection = openAdminConnection();
        Statement statement = connection.createStatement()) {
      configureSqlConnection(connection);
      JdbcPostgresWatermarkTableHelper helper = new JdbcPostgresWatermarkTableHelper();

      helper.ensureMetadataTable(connection);
      helper.ensureMetadataTable(connection);

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT id, run_id, token FROM dblog_meta.watermarks ORDER BY id ASC")) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getLong(1)).isEqualTo(1L);
        assertThat(resultSet.getString(2)).isNull();
        assertThat(resultSet.getString(3)).isNull();
        assertThat(resultSet.next()).isFalse();
      }

      helper.writeWatermark(connection, "run-1", new WatermarkToken("lw-it"));
      assertThat(currentToken(statement)).isEqualTo("lw-it");

      helper.writeWatermark(connection, "run-1", new WatermarkToken("hw-it"));
      assertThat(currentToken(statement)).isEqualTo("hw-it");
    }
  }

  @Test
  void failsClosedWhenWatermarkSingletonRowIsMissing() throws Exception {
    try (Connection connection = openAdminConnection();
        Statement statement = connection.createStatement()) {
      configureSqlConnection(connection);
      JdbcPostgresWatermarkTableHelper helper = new JdbcPostgresWatermarkTableHelper();
      helper.ensureMetadataTable(connection);
      statement.execute("DELETE FROM dblog_meta.watermarks WHERE id = 1");

      assertThatThrownBy(
              () -> helper.writeWatermark(connection, "run-1", new WatermarkToken("missing")))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("missing or duplicated");
    }
  }

  @Test
  void failsClosedWhenWatermarkSingletonRowIsDuplicated() throws Exception {
    try (Connection connection = openAdminConnection();
        Statement statement = connection.createStatement()) {
      configureSqlConnection(connection);
      JdbcPostgresWatermarkTableHelper helper = new JdbcPostgresWatermarkTableHelper();
      helper.ensureMetadataTable(connection);
      // Drop the PK constraint to allow a second row, then insert a duplicate id=1.
      statement.execute("ALTER TABLE dblog_meta.watermarks DROP CONSTRAINT watermarks_pkey");
      statement.execute(
          "INSERT INTO dblog_meta.watermarks (id, run_id, token) VALUES (1, 'dup', 'dup')");

      assertThatThrownBy(
              () -> helper.writeWatermark(connection, "run-1", new WatermarkToken("duplicate")))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("missing or duplicated");
    }
  }

  @Test
  void failsClosedWhenWatermarkTableIsMalformedAtStartup() throws Exception {
    try (Connection connection = openAdminConnection();
        Statement statement = connection.createStatement()) {
      configureSqlConnection(connection);
      JdbcPostgresWatermarkTableHelper helper = new JdbcPostgresWatermarkTableHelper();
      helper.ensureMetadataTable(connection);
      statement.execute("ALTER TABLE dblog_meta.watermarks DROP COLUMN token");

      assertThatThrownBy(
              () -> helper.writeWatermark(connection, "run-1", new WatermarkToken("broken")))
          .isInstanceOf(java.sql.SQLException.class);
    }
  }

  private static String currentToken(Statement statement) throws Exception {
    try (ResultSet resultSet =
        statement.executeQuery("SELECT token FROM dblog_meta.watermarks WHERE id = 1")) {
      assertThat(resultSet.next()).isTrue();
      return resultSet.getString(1);
    }
  }

  private static void configureSqlConnection(Connection connection) throws Exception {
    connection.setAutoCommit(true);
    connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
  }

  private static Connection openAdminConnection() throws Exception {
    return DriverManager.getConnection(
        postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
  }

}
