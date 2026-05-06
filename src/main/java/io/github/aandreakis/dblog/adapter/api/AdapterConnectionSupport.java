package io.github.aandreakis.dblog.adapter.api;

import io.github.aandreakis.dblog.runtime.sql.RuntimeSqlSupport;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Dialect-agnostic JDBC connection-opening helpers shared across adapter modules.
 *
 * <p>Opens a JDBC connection from a {@link RelationalSourceConfig}, applies the narrow runtime
 * session defaults ({@code autoCommit=true}, {@code READ_COMMITTED}, best-effort network
 * timeout) via {@link RuntimeSqlSupport#configureRuntimeSqlConnection(Connection)}, and closes
 * the connection with suppressed-exception chaining if session setup fails.
 *
 * <p>Forward-compatible: the body is plain JDBC — nothing dialect-specific. Future SQL Server
 * or Oracle adapters can reuse it unchanged.
 */
public final class AdapterConnectionSupport {
  private AdapterConnectionSupport() {}

  /**
   * Open a new SQL {@link Connection} for this dialect and configure it with the runtime
   * session defaults.
   *
   * @param config the source config carrying {@code jdbcUrl}, {@code username}, {@code password}
   * @param dialectLabel human-readable dialect name used in error messages (e.g. {@code "MySQL"})
   * @throws SQLException if {@code DriverManager.getConnection} or session configuration fails;
   *     on configuration failure the connection is closed first with suppressed-exception chaining.
   */
  public static Connection openConfiguredSqlConnection(
      RelationalSourceConfig config, String dialectLabel) throws SQLException {
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(dialectLabel, "dialectLabel");
    Connection connection =
        DriverManager.getConnection(config.jdbcUrl(), config.username(), config.password());
    try {
      RuntimeSqlSupport.configureRuntimeSqlConnection(connection);
    } catch (SQLException | RuntimeException failure) {
      try {
        connection.close();
      } catch (SQLException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
      throw failure;
    }
    return connection;
  }
}
