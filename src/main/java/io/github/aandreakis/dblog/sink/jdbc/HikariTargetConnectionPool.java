package io.github.aandreakis.dblog.sink.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Sink-owned Hikari-backed JDBC connection source for target apply sinks. */
final class HikariTargetConnectionPool implements JdbcApplyChangeEventSink.SqlConnectionSource {
  private final HikariDataSource dataSource;

  HikariTargetConnectionPool(
      String driverClassName,
      String jdbcUrl,
      String username,
      String password,
      String poolLabel,
      int maximumPoolSize,
      Duration connectionTimeout) {
    String requiredDriverClassName = requireNonBlank(driverClassName, "driverClassName");
    String requiredJdbcUrl = requireNonBlank(jdbcUrl, "jdbcUrl");
    String requiredPoolLabel = requireNonBlank(poolLabel, "poolLabel");
    if (maximumPoolSize <= 0) {
      throw new IllegalArgumentException("maximumPoolSize must be > 0");
    }
    Duration requiredConnectionTimeout =
        Objects.requireNonNull(connectionTimeout, "connectionTimeout");
    if (requiredConnectionTimeout.isZero() || requiredConnectionTimeout.isNegative()) {
      throw new IllegalArgumentException("connectionTimeout must be > 0");
    }

    HikariConfig config = new HikariConfig();
    config.setPoolName(poolName(requiredPoolLabel, requiredJdbcUrl));
    config.setDriverClassName(requiredDriverClassName);
    config.setJdbcUrl(requiredJdbcUrl);
    if (username != null && !username.isBlank()) {
      config.setUsername(username);
    }
    if (password != null) {
      config.setPassword(password);
    }
    config.setMaximumPoolSize(maximumPoolSize);
    config.setMinimumIdle(0);
    config.setAutoCommit(false);
    config.setConnectionTimeout(requiredConnectionTimeout.toMillis());
    config.setValidationTimeout(
        Math.max(1_000L, Math.min(requiredConnectionTimeout.toMillis(), 5_000L)));
    config.setInitializationFailTimeout(1L);
    config.setRegisterMbeans(false);
    driverTuningProperties(requiredDriverClassName, requiredJdbcUrl)
        .forEach(config::addDataSourceProperty);
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

  static Map<String, String> driverTuningProperties(String driverClassName, String jdbcUrl) {
    String normalizedDriver = requireNonBlank(driverClassName, "driverClassName").toLowerCase(Locale.ROOT);
    String normalizedJdbcUrl = requireNonBlank(jdbcUrl, "jdbcUrl").toLowerCase(Locale.ROOT);
    LinkedHashMap<String, String> properties = new LinkedHashMap<>();
    if (normalizedDriver.contains("mysql") || normalizedJdbcUrl.startsWith("jdbc:mysql:")) {
      properties.put("cachePrepStmts", "true");
      properties.put("prepStmtCacheSize", "256");
      properties.put("prepStmtCacheSqlLimit", "4096");
      properties.put("useServerPrepStmts", "true");
      properties.put("rewriteBatchedStatements", "true");
      properties.put("cacheResultSetMetadata", "true");
      properties.put("useLocalSessionState", "true");
      properties.put("elideSetAutoCommits", "true");
      properties.put("maintainTimeStats", "false");
      return Map.copyOf(properties);
    }
    if (normalizedDriver.contains("postgresql")
        || normalizedJdbcUrl.startsWith("jdbc:postgresql:")) {
      properties.put("reWriteBatchedInserts", "true");
      properties.put("prepareThreshold", "3");
      properties.put("preparedStatementCacheQueries", "256");
      properties.put("preparedStatementCacheSizeMiB", "8");
      return Map.copyOf(properties);
    }
    return Map.of();
  }
}
