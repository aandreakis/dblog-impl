package io.github.aandreakis.dblog.sink.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

class HikariTargetConnectionPoolTests {
  @Test
  void opensReusableJdbcConnectionsFromASinkOwnedPool() throws Exception {
    String jdbcUrl = "jdbc:h2:mem:hikari_target_pool;DB_CLOSE_DELAY=-1";
    try (Connection connection = DriverManager.getConnection(jdbcUrl);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TABLE sample (id INT PRIMARY KEY, name VARCHAR(32))");
    }

    try (HikariTargetConnectionPool pool =
        new HikariTargetConnectionPool(
            "org.h2.Driver",
            jdbcUrl,
            "",
            "",
            "test-target-pool",
            2,
            Duration.ofSeconds(2))) {
      try (Connection first = pool.open();
          Connection second = pool.open()) {
        assertThat(first.isClosed()).isFalse();
        assertThat(second.isClosed()).isFalse();
        assertThat(first.getAutoCommit()).isFalse();
        assertThat(second.getAutoCommit()).isFalse();
        try (Statement statement = first.createStatement()) {
          statement.execute("INSERT INTO sample (id, name) VALUES (1, 'one')");
        }
        first.commit();
        try (Statement statement = second.createStatement();
            var resultSet = statement.executeQuery("SELECT COUNT(*) FROM sample")) {
          assertThat(resultSet.next()).isTrue();
          assertThat(resultSet.getInt(1)).isEqualTo(1);
        }
      }
    }
  }

  @Test
  void rejectsInvalidPoolSettings() {
    assertThatThrownBy(
            () ->
                new HikariTargetConnectionPool(
                    "org.h2.Driver",
                    "jdbc:h2:mem:invalid_pool",
                    "",
                    "",
                    "invalid-pool",
                    0,
                    Duration.ofSeconds(2)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maximumPoolSize");

    assertThatThrownBy(
            () ->
                new HikariTargetConnectionPool(
                    "org.h2.Driver",
                    "jdbc:h2:mem:invalid_timeout",
                    "",
                    "",
                    "invalid-timeout",
                    1,
                    Duration.ZERO))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("connectionTimeout");
  }

  @Test
  void exposesDriverSpecificPreparedStatementAndBatchTuningProperties() {
    assertThat(
            HikariTargetConnectionPool.driverTuningProperties(
                "com.mysql.cj.jdbc.Driver", "jdbc:mysql://127.0.0.1:3306/app"))
        .containsEntry("cachePrepStmts", "true")
        .containsEntry("useServerPrepStmts", "true")
        .containsEntry("rewriteBatchedStatements", "true")
        .containsEntry("elideSetAutoCommits", "true");

    assertThat(
            HikariTargetConnectionPool.driverTuningProperties(
                "org.postgresql.Driver", "jdbc:postgresql://127.0.0.1:5432/app"))
        .containsEntry("reWriteBatchedInserts", "true")
        .containsEntry("prepareThreshold", "3");

    assertThat(
            HikariTargetConnectionPool.driverTuningProperties(
                "org.h2.Driver", "jdbc:h2:mem:test"))
        .isEqualTo(Map.of());
  }
}
