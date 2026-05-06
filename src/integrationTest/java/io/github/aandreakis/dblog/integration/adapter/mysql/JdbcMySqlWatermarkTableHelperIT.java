package io.github.aandreakis.dblog.integration.adapter.mysql;

import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlWatermarkTableHelper;
import io.github.aandreakis.dblog.core.model.WatermarkToken;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

@Tag("integration-docker")
class JdbcMySqlWatermarkTableHelperIT {
  private static MySQLContainer mysql;

  @BeforeAll
  static void startMysql() {
    assumeDockerIsAvailable();
    mysql =
        new MySQLContainer(DockerImageName.parse("mysql:8.4"))
            .withDatabaseName("appdb")
            .withUsername("dblog")
            .withPassword("dblog")
            .withEnv("MYSQL_ROOT_PASSWORD", "root");
    mysql.start();
  }

  @AfterAll
  static void stopMysql() {
    if (mysql != null) {
      mysql.close();
    }
  }

  @BeforeEach
  void resetMetadataSchema() throws Exception {
    try (Connection connection = openRootConnection();
        Statement statement = connection.createStatement()) {
      configureSqlConnection(connection);
      statement.execute("DROP DATABASE IF EXISTS dblog_meta");
    }
  }

  @Test
  void ensuresSingletonMetadataTableAndWritesWatermarkTokens() throws Exception {
    try (Connection connection = openRootConnection();
        Statement statement = connection.createStatement()) {
      configureSqlConnection(connection);
      JdbcMySqlWatermarkTableHelper helper = new JdbcMySqlWatermarkTableHelper();

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
      assertThat(currentToken(connection)).isEqualTo("lw-it");

      helper.writeWatermark(connection, "run-1", new WatermarkToken("hw-it"));
      assertThat(currentToken(connection)).isEqualTo("hw-it");
    }
  }

  @Test
  void failsClosedWhenWatermarkSingletonRowIsMissing() throws Exception {
    try (Connection connection = openRootConnection();
        Statement statement = connection.createStatement()) {
      configureSqlConnection(connection);
      JdbcMySqlWatermarkTableHelper helper = new JdbcMySqlWatermarkTableHelper();
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
    try (Connection connection = openRootConnection();
        Statement statement = connection.createStatement()) {
      configureSqlConnection(connection);
      JdbcMySqlWatermarkTableHelper helper = new JdbcMySqlWatermarkTableHelper();
      helper.ensureMetadataTable(connection);
      statement.execute("ALTER TABLE dblog_meta.watermarks DROP PRIMARY KEY");
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
    try (Connection connection = openRootConnection();
        Statement statement = connection.createStatement()) {
      configureSqlConnection(connection);
      JdbcMySqlWatermarkTableHelper helper = new JdbcMySqlWatermarkTableHelper();
      helper.ensureMetadataTable(connection);
      statement.execute("ALTER TABLE dblog_meta.watermarks DROP COLUMN token");

      assertThatThrownBy(
              () -> helper.writeWatermark(connection, "run-1", new WatermarkToken("broken")))
          .isInstanceOf(java.sql.SQLException.class);
    }
  }

  private static String currentToken(Connection connection) throws Exception {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("SELECT token FROM dblog_meta.watermarks WHERE id = 1")) {
      assertThat(resultSet.next()).isTrue();
      return resultSet.getString(1);
    }
  }

  private static void configureSqlConnection(Connection connection) throws Exception {
    connection.setAutoCommit(true);
    connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
  }

  private static Connection openRootConnection() throws Exception {
    return DriverManager.getConnection(mysql.getJdbcUrl(), "root", mysql.getPassword());
  }

}
