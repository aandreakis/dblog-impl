package io.github.aandreakis.dblog.sink.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

class HikariTypedSinkConnectionPoolTests {
  @Test
  void keepsTypedSinkH2DatabaseAvailableAcrossBorrowedConnections() throws Exception {
    String jdbcUrl = "jdbc:h2:file:/tmp/dblog-typed-sink-pool-test-" + System.nanoTime();
    try (HikariTypedSinkConnectionPool pool =
        new HikariTypedSinkConnectionPool("org.h2.Driver", jdbcUrl, "typed-sink-pool")) {
      try (Connection first = pool.open(); Statement statement = first.createStatement()) {
        statement.execute("CREATE TABLE sample (id INT PRIMARY KEY, name VARCHAR(32))");
        statement.execute("INSERT INTO sample (id, name) VALUES (1, 'one')");
      }

      try (Connection second = pool.open();
          Statement statement = second.createStatement();
          var resultSet = statement.executeQuery("SELECT COUNT(*) FROM sample")) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getInt(1)).isEqualTo(1);
      }
    } finally {
      try (Connection cleanup = DriverManager.getConnection(jdbcUrl + ";IFEXISTS=TRUE");
          Statement statement = cleanup.createStatement()) {
        statement.execute("DROP ALL OBJECTS DELETE FILES");
      } catch (Exception ignored) {
        // best-effort cleanup only
      }
    }
  }
}
