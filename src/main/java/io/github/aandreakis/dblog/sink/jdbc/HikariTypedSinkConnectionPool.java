package io.github.aandreakis.dblog.sink.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;

/** Sink-owned Hikari-backed JDBC connection source for the structured typed sink. */
final class HikariTypedSinkConnectionPool implements JdbcTypedChangeEventSink.SqlConnectionSource {
  private final HikariDataSource dataSource;

  HikariTypedSinkConnectionPool(String driverClassName, String jdbcUrl, String poolLabel) {
    String requiredDriverClassName = requireNonBlank(driverClassName, "driverClassName");
    String requiredJdbcUrl = requireNonBlank(jdbcUrl, "jdbcUrl");
    String requiredPoolLabel = requireNonBlank(poolLabel, "poolLabel");

    HikariConfig config = new HikariConfig();
    config.setPoolName(poolName(requiredPoolLabel, requiredJdbcUrl));
    config.setDriverClassName(requiredDriverClassName);
    config.setJdbcUrl(requiredJdbcUrl);
    config.setMaximumPoolSize(1);
    config.setMinimumIdle(1);
    config.setConnectionTimeout(Duration.ofSeconds(2).toMillis());
    config.setValidationTimeout(1_000L);
    config.setInitializationFailTimeout(1L);
    config.setRegisterMbeans(false);
    this.dataSource = new HikariDataSource(config);
  }

  @Override
  public Connection open() throws SQLException {
    return dataSource.getConnection();
  }

  @Override
  public void close() {
    boolean interrupted = Thread.interrupted();
    try {
      dataSource.close();
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private static String poolName(String poolLabel, String jdbcUrl) {
    String normalizedLabel = poolLabel.replaceAll("[^A-Za-z0-9]+", "-");
    return "dblog-" + normalizedLabel + "-" + Integer.toUnsignedString(jdbcUrl.hashCode(), 16);
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
