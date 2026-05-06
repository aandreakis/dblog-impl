package io.github.aandreakis.dblog.integration.adapter.mysql;

import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlHeartbeatTableHelper;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

@Tag("integration-docker")
class JdbcMySqlHeartbeatTableHelperIT {
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
  void ensuresSingletonHeartbeatTableAndWritesSparseTimestamps() throws Exception {
    try (Connection connection = openRootConnection();
        Statement statement = connection.createStatement()) {
      configureSqlConnection(connection);
      JdbcMySqlHeartbeatTableHelper helper = new JdbcMySqlHeartbeatTableHelper();
      Instant firstBeat = Instant.parse("2026-03-21T00:05:00Z");
      Instant secondBeat = firstBeat.plusSeconds(30);
      Instant thirdBeat = firstBeat.plus(Duration.ofMinutes(6));

      helper.ensureHeartbeatTable(connection);
      helper.ensureHeartbeatTable(connection);

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT id, run_id, source_stream_id, last_beat_at "
                  + "FROM dblog_meta.heartbeats ORDER BY id ASC")) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getLong(1)).isEqualTo(1L);
        assertThat(resultSet.getString(2)).isNull();
        assertThat(resultSet.getString(3)).isNull();
        assertThat(resultSet.getTimestamp(4)).isNull();
        assertThat(resultSet.next()).isFalse();
      }

      assertThat(
              helper.writeHeartbeatIfDue(
                  connection, "run-1", "stream-1", firstBeat, Duration.ofMinutes(5)))
          .isTrue();
      assertThat(
              helper.writeHeartbeatIfDue(
                  connection, "run-1", "stream-1", secondBeat, Duration.ofMinutes(5)))
          .isFalse();
      assertThat(
              helper.writeHeartbeatIfDue(
                  connection, "run-1", "stream-1", thirdBeat, Duration.ofMinutes(5)))
          .isTrue();
    }
  }

  @Test
  void failsClosedWhenHeartbeatSingletonRowIsMissing() throws Exception {
    try (Connection connection = openRootConnection();
        Statement statement = connection.createStatement()) {
      configureSqlConnection(connection);
      JdbcMySqlHeartbeatTableHelper helper = new JdbcMySqlHeartbeatTableHelper();
      helper.ensureHeartbeatTable(connection);
      statement.execute("DELETE FROM dblog_meta.heartbeats WHERE id = 1");

      assertThatThrownBy(
              () ->
                  helper.writeHeartbeatIfDue(
                      connection,
                      "run-1",
                      "stream-1",
                      Instant.parse("2026-03-21T00:05:00Z"),
                      Duration.ofMinutes(5)))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("missing or duplicated");
    }
  }

  @Test
  void failsClosedWhenHeartbeatSingletonRowIsDuplicated() throws Exception {
    try (Connection connection = openRootConnection();
        Statement statement = connection.createStatement()) {
      configureSqlConnection(connection);
      JdbcMySqlHeartbeatTableHelper helper = new JdbcMySqlHeartbeatTableHelper();
      helper.ensureHeartbeatTable(connection);
      statement.execute("ALTER TABLE dblog_meta.heartbeats DROP PRIMARY KEY");
      statement.execute(
          "INSERT INTO dblog_meta.heartbeats (id, run_id, source_stream_id, last_beat_at) "
              + "VALUES (1, NULL, NULL, NULL)");

      assertThatThrownBy(
              () ->
                  helper.writeHeartbeatIfDue(
                      connection,
                      "run-1",
                      "stream-1",
                      Instant.parse("2026-03-21T00:05:00Z"),
                      Duration.ofMinutes(5)))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("missing or duplicated");
    }
  }

  @Test
  void failsClosedWhenHeartbeatTableIsMalformedAtStartup() throws Exception {
    try (Connection connection = openRootConnection();
        Statement statement = connection.createStatement()) {
      configureSqlConnection(connection);
      JdbcMySqlHeartbeatTableHelper helper = new JdbcMySqlHeartbeatTableHelper();
      helper.ensureHeartbeatTable(connection);
      statement.execute("ALTER TABLE dblog_meta.heartbeats DROP COLUMN last_beat_at");

      assertThatThrownBy(
              () ->
                  helper.writeHeartbeatIfDue(
                      connection,
                      "run-1",
                      "stream-1",
                      Instant.parse("2026-03-21T00:05:00Z"),
                      Duration.ofMinutes(5)))
          .isInstanceOf(java.sql.SQLException.class);
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
