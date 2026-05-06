package io.github.aandreakis.dblog.integration.adapter.postgres;

import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.postgres.internal.JdbcPostgresHeartbeatTableHelper;
import io.github.aandreakis.dblog.testsupport.LivePostgresTestContainers;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Tag("integration-docker")
class JdbcPostgresHeartbeatTableHelperIT {
  @Test
  void ensuresSingletonHeartbeatTableAndWritesSparseTimestamps() throws Exception {
    assumeDockerIsAvailable();

    try (PostgreSQLContainer postgres = LivePostgresTestContainers.newBaseContainer()) {
      postgres.start();
      try (Connection connection =
              DriverManager.getConnection(
                  postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
          Statement statement = connection.createStatement()) {
        configureSqlConnection(connection);
        JdbcPostgresHeartbeatTableHelper helper = new JdbcPostgresHeartbeatTableHelper();
        Instant firstBeat = Instant.parse("2026-03-20T00:05:00Z");
        Instant secondBeat = firstBeat.plusSeconds(30);
        Instant thirdBeat = firstBeat.plus(Duration.ofMinutes(6));

        helper.ensureHeartbeatTable(connection);
        helper.ensureHeartbeatTable(connection);

        try (ResultSet resultSet =
            statement.executeQuery(
                "SELECT id, run_id, source_stream_id, last_beat_at FROM dblog_meta.heartbeats ORDER BY id ASC")) {
          assertThat(resultSet.next()).isTrue();
          assertThat(resultSet.getLong(1)).isEqualTo(1L);
          assertThat(resultSet.getString(2)).isNull();
          assertThat(resultSet.getString(3)).isNull();
          assertThat(resultSet.getTimestamp(4)).isNull();
          assertThat(resultSet.next()).isFalse();
        }

        assertThat(
                helper.writeHeartbeatIfDue(connection, "run-1", "stream-1", firstBeat, Duration.ofMinutes(5)))
            .isTrue();
        assertThat(
                helper.writeHeartbeatIfDue(connection, "run-1", "stream-1", secondBeat, Duration.ofMinutes(5)))
            .isFalse();
        assertThat(
                helper.writeHeartbeatIfDue(connection, "run-1", "stream-1", thirdBeat, Duration.ofMinutes(5)))
            .isTrue();
      }
    }
  }

  private static void configureSqlConnection(Connection connection) throws Exception {
    connection.setAutoCommit(true);
    connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
  }

}
