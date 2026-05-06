package io.github.aandreakis.dblog.runtime.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Shared narrow SQL-connection contract for caller-supplied runtime JDBC connections. */
public final class RuntimeSqlSupport {
  private static final Duration DEFAULT_NETWORK_TIMEOUT = Duration.ofSeconds(5);
  private static final ExecutorService NETWORK_TIMEOUT_EXECUTOR =
      Executors.newThreadPerTaskExecutor(
          Thread.ofVirtual().name("dblog-runtime-sql-timeout-", 0).factory());

  private RuntimeSqlSupport() {}

  public static void configureRuntimeSqlConnection(Connection connection) throws SQLException {
    configureRuntimeSqlConnection(connection, DEFAULT_NETWORK_TIMEOUT);
  }

  public static void configureRuntimeSqlConnection(Connection connection, Duration networkTimeout)
      throws SQLException {
    Objects.requireNonNull(connection, "connection");
    Objects.requireNonNull(networkTimeout, "networkTimeout");
    connection.setAutoCommit(true);
    connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    try {
      connection.setNetworkTimeout(
          NETWORK_TIMEOUT_EXECUTOR,
          Math.max(1, Math.toIntExact(networkTimeout.toMillis())));
    } catch (SQLException | RuntimeException ignored) {
      // Best-effort only: some drivers or proxies may not support connection-level network timeouts.
    }
  }

  public static void requireRuntimeSqlConnection(Connection connection, String label)
      throws SQLException {
    Objects.requireNonNull(connection, "connection");
    String normalizedLabel = requireNonBlank(label, "label");
    if (connection.isClosed()) {
      throw new IllegalStateException(normalizedLabel + " SQL connection must be open");
    }
    if (!connection.getAutoCommit()) {
      throw new IllegalStateException(
          normalizedLabel + " SQL connection must use auto-commit");
    }
    int isolation = connection.getTransactionIsolation();
    if (isolation != Connection.TRANSACTION_READ_COMMITTED) {
      throw new IllegalStateException(
          normalizedLabel
              + " SQL connection must use READ_COMMITTED; actual="
              + isolationName(isolation));
    }
  }

  private static String isolationName(int isolation) {
    return switch (isolation) {
      case Connection.TRANSACTION_NONE -> "TRANSACTION_NONE";
      case Connection.TRANSACTION_READ_UNCOMMITTED -> "READ_UNCOMMITTED";
      case Connection.TRANSACTION_READ_COMMITTED -> "READ_COMMITTED";
      case Connection.TRANSACTION_REPEATABLE_READ -> "REPEATABLE_READ";
      case Connection.TRANSACTION_SERIALIZABLE -> "SERIALIZABLE";
      default -> "UNKNOWN(" + isolation + ")";
    };
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
