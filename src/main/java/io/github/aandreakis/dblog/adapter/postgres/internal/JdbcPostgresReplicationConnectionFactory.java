package io.github.aandreakis.dblog.adapter.postgres.internal;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

/** Opens a JDBC connection in PostgreSQL logical-replication mode. */
public final class JdbcPostgresReplicationConnectionFactory
    implements PostgresReplicationConnectionFactory {
  @Override
  public Connection open(String jdbcUrl, String username, String password) throws SQLException {
    jdbcUrl = requireNonBlank(jdbcUrl, "jdbcUrl");
    username = requireNonBlank(username, "username");
    Objects.requireNonNull(password, "password");

    Properties properties = new Properties();
    properties.setProperty("user", username);
    properties.setProperty("password", password);
    properties.setProperty("assumeMinServerVersion", "10");
    properties.setProperty("replication", "database");
    properties.setProperty("preferQueryMode", "simple");
    return DriverManager.getConnection(jdbcUrl, properties);
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
