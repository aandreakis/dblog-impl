package io.github.aandreakis.dblog.verification.scenario;

import io.github.aandreakis.dblog.config.ScenarioRequestMode;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Parser for scenario-mode properties. */
public final class ScenarioModeConfigParser {
  private ScenarioModeConfigParser() {}

  public static ScenarioModeConfig parse(Map<String, String> properties) {
    Objects.requireNonNull(properties, "properties");
    String adapter = requireNonBlank(properties.get("dblog.scenario.adapter"), "dblog.scenario.adapter");
    String normalizedAdapter = normalizeAdapter(adapter);
    CommonScenarioConfig common = common(properties);
    return switch (normalizedAdapter) {
      case "mysql" -> new MySqlScenarioModeConfig(
          common,
          requireNonBlank(properties.get("dblog.scenario.mysql.jdbc-url"), "dblog.scenario.mysql.jdbc-url"),
          requireNonBlank(properties.get("dblog.scenario.mysql.username"), "dblog.scenario.mysql.username"),
          properties.getOrDefault("dblog.scenario.mysql.password", ""),
          mysqlDatabaseName(properties),
          mysqlHostname(properties),
          mysqlPort(properties),
          longProperty(properties, "dblog.scenario.mysql.server-id", 223344L),
          durationProperty(properties, "dblog.scenario.mysql.connect-timeout", Duration.ofSeconds(5)),
          booleanProperty(properties, "dblog.scenario.mysql.retry-log-connection-loss", false),
          durationProperty(properties, "dblog.scenario.mysql.reconnect-backoff", Duration.ofSeconds(3)),
          intProperty(properties, "dblog.scenario.mysql.source-event-queue-capacity", 50_000));
      case "postgres" -> new PostgresScenarioModeConfig(
          common,
          requireNonBlank(properties.get("dblog.scenario.postgres.jdbc-url"), "dblog.scenario.postgres.jdbc-url"),
          postgresReplicationJdbcUrl(properties),
          requireNonBlank(properties.get("dblog.scenario.postgres.username"), "dblog.scenario.postgres.username"),
          properties.getOrDefault("dblog.scenario.postgres.password", ""),
          postgresDatabaseName(properties),
          durationProperty(properties, "dblog.scenario.postgres.status-interval", Duration.ofSeconds(5)),
          booleanProperty(properties, "dblog.scenario.postgres.retry-log-connection-loss", false),
          durationProperty(properties, "dblog.scenario.postgres.reconnect-backoff", Duration.ofSeconds(3)));
      default -> throw new IllegalArgumentException("Unsupported scenario adapter: " + adapter);
    };
  }

  private static CommonScenarioConfig common(Map<String, String> properties) {
    String scenarioId = requireNonBlank(properties.get("dblog.scenario.id"), "dblog.scenario.id");
    return new CommonScenarioConfig(
        scenarioId,
        requireNonBlank(properties.get("dblog.scenario.adapter"), "dblog.scenario.adapter"),
        requireNonBlank(
            properties.getOrDefault("dblog.scenario.source-id", scenarioId + "-source"),
            "dblog.scenario.source-id"),
        requirePath(properties.get("dblog.scenario.state-path"), "dblog.scenario.state-path"),
        requirePath(properties.get("dblog.scenario.sink-path"), "dblog.scenario.sink-path"),
        booleanProperty(properties, "dblog.scenario.reset-source", true),
        intProperty(properties, "dblog.chunk.size", 100),
        intProperty(properties, "dblog.scenario.mutation-count", 100),
        intProperty(properties, "dblog.scenario.mutation-batch-size", 1),
        durationProperty(properties, "dblog.scenario.idle-drain-timeout", Duration.ofMillis(500)),
        durationProperty(properties, "dblog.scenario.mutation-pause", Duration.ofMillis(20)),
        ScenarioRequestMode.parse(properties.get("dblog.scenario.request-mode")),
        faultPlan(properties));
  }

  private static ScenarioFaultPlan faultPlan(Map<String, String> properties) {
    return new ScenarioFaultPlan(
        durationProperty(properties, "dblog.scenario.sink-delay", Duration.ZERO),
        integerProperty(properties, "dblog.scenario.fail-sink-after-append-count"),
        integerProperty(properties, "dblog.scenario.crash-before-request-ack-batch-index"),
        durationProperty(properties, "dblog.scenario.runtime-read-delay", Duration.ZERO),
        integerProperty(properties, "dblog.scenario.fail-runtime-read-after-count"),
        durationProperty(properties, "dblog.scenario.runtime-ack-delay", Duration.ZERO),
        integerProperty(properties, "dblog.scenario.fail-runtime-ack-after-count"),
        durationProperty(properties, "dblog.scenario.chunk-read-delay", Duration.ZERO),
        integerProperty(properties, "dblog.scenario.fail-chunk-read-after-count"),
        durationProperty(properties, "dblog.scenario.state-store-delay", Duration.ZERO),
        integerProperty(properties, "dblog.scenario.fail-state-store-operation-after-count"),
        trimToNull(properties.get("dblog.scenario.fail-state-store-operation-name")),
        durationProperty(properties, "dblog.scenario.terminate-replication-backend-after", Duration.ZERO),
        durationProperty(properties, "dblog.scenario.close-runtime-sql-connection-after", Duration.ZERO),
        durationProperty(properties, "dblog.scenario.alter-captured-schema-after", Duration.ZERO),
        durationProperty(properties, "dblog.scenario.alter-metadata-shape-after", Duration.ZERO),
        durationProperty(properties, "dblog.scenario.delete-metadata-row-after", Duration.ZERO),
        durationProperty(properties, "dblog.scenario.null-heartbeat-timestamp-after", Duration.ZERO));
  }

  private static String normalizeAdapter(String adapter) {
    String normalized = adapter.trim().toLowerCase(Locale.ROOT);
    return normalized.equals("postgresql") ? "postgres" : normalized;
  }

  private static String mysqlDatabaseName(Map<String, String> properties) {
    String configured = trimToNull(properties.get("dblog.scenario.mysql.database-name"));
    return configured == null ? databaseNameFromJdbcUrl(mysqlJdbcUrl(properties), "MySQL") : configured;
  }

  private static String mysqlHostname(Map<String, String> properties) {
    String configured = trimToNull(properties.get("dblog.scenario.mysql.hostname"));
    return configured == null ? hostFromJdbcUrl(mysqlJdbcUrl(properties), "jdbc:mysql://", "MySQL") : configured;
  }

  private static int mysqlPort(Map<String, String> properties) {
    String configured = trimToNull(properties.get("dblog.scenario.mysql.port"));
    return configured == null
        ? portFromJdbcUrl(mysqlJdbcUrl(properties), "jdbc:mysql://", 3306, "MySQL")
        : Integer.parseInt(configured);
  }

  private static String postgresReplicationJdbcUrl(Map<String, String> properties) {
    String configured = trimToNull(properties.get("dblog.scenario.postgres.replication-jdbc-url"));
    return configured == null ? postgresJdbcUrl(properties) : configured;
  }

  private static String postgresDatabaseName(Map<String, String> properties) {
    String configured = trimToNull(properties.get("dblog.scenario.postgres.database-name"));
    return configured == null ? databaseNameFromJdbcUrl(postgresJdbcUrl(properties), "PostgreSQL") : configured;
  }

  private static String mysqlJdbcUrl(Map<String, String> properties) {
    return requireNonBlank(properties.get("dblog.scenario.mysql.jdbc-url"), "dblog.scenario.mysql.jdbc-url");
  }

  private static String postgresJdbcUrl(Map<String, String> properties) {
    return requireNonBlank(properties.get("dblog.scenario.postgres.jdbc-url"), "dblog.scenario.postgres.jdbc-url");
  }

  private static String hostFromJdbcUrl(String jdbcUrl, String prefix, String label) {
    String authority = authorityFromJdbcUrl(jdbcUrl, prefix, label);
    int atIndex = authority.lastIndexOf('@');
    String hostPort = atIndex >= 0 ? authority.substring(atIndex + 1) : authority;
    int colonIndex = hostPort.lastIndexOf(':');
    return colonIndex >= 0 ? hostPort.substring(0, colonIndex) : hostPort;
  }

  private static int portFromJdbcUrl(
      String jdbcUrl, String prefix, int defaultPort, String label) {
    String authority = authorityFromJdbcUrl(jdbcUrl, prefix, label);
    int atIndex = authority.lastIndexOf('@');
    String hostPort = atIndex >= 0 ? authority.substring(atIndex + 1) : authority;
    int colonIndex = hostPort.lastIndexOf(':');
    return colonIndex >= 0 ? Integer.parseInt(hostPort.substring(colonIndex + 1)) : defaultPort;
  }

  private static String authorityFromJdbcUrl(String jdbcUrl, String prefix, String label) {
    if (!jdbcUrl.startsWith(prefix)) {
      throw new IllegalArgumentException("Unsupported " + label + " JDBC URL: " + jdbcUrl);
    }
    String tail = jdbcUrl.substring(prefix.length());
    int slashIndex = tail.indexOf('/');
    if (slashIndex < 0) {
      throw new IllegalArgumentException("Unsupported " + label + " JDBC URL: " + jdbcUrl);
    }
    return tail.substring(0, slashIndex);
  }

  private static String databaseNameFromJdbcUrl(String jdbcUrl, String label) {
    int slashIndex = jdbcUrl.lastIndexOf('/');
    if (slashIndex < 0 || slashIndex == jdbcUrl.length() - 1) {
      throw new IllegalArgumentException(
          "Could not infer " + label + " database name from JDBC URL: " + jdbcUrl);
    }
    String tail = jdbcUrl.substring(slashIndex + 1);
    int queryIndex = tail.indexOf('?');
    return queryIndex >= 0 ? tail.substring(0, queryIndex) : tail;
  }

  private static boolean booleanProperty(Map<String, String> properties, String key, boolean defaultValue) {
    String raw = trimToNull(properties.get(key));
    return raw == null ? defaultValue : Boolean.parseBoolean(raw);
  }

  private static int intProperty(Map<String, String> properties, String key, int defaultValue) {
    String raw = trimToNull(properties.get(key));
    return raw == null ? defaultValue : Integer.parseInt(raw);
  }

  private static Integer integerProperty(Map<String, String> properties, String key) {
    String raw = trimToNull(properties.get(key));
    return raw == null ? null : Integer.parseInt(raw);
  }

  private static long longProperty(Map<String, String> properties, String key, long defaultValue) {
    String raw = trimToNull(properties.get(key));
    return raw == null ? defaultValue : Long.parseLong(raw);
  }

  private static Duration durationProperty(
      Map<String, String> properties, String key, Duration defaultValue) {
    String raw = trimToNull(properties.get(key));
    return raw == null ? defaultValue : Duration.parse(raw);
  }

  private static Path requirePath(String raw, String key) {
    String trimmed = requireNonBlank(raw, key);
    return Path.of(trimmed);
  }

  private static String requireNonBlank(String value, String key) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(key + " must not be blank");
    }
    return value.trim();
  }

  private static String trimToNull(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }

  public sealed interface ScenarioModeConfig permits MySqlScenarioModeConfig, PostgresScenarioModeConfig {
    CommonScenarioConfig common();
  }

  public record CommonScenarioConfig(
      String scenarioId,
      String adapter,
      String sourceId,
      Path statePath,
      Path sinkPath,
      boolean resetSource,
      int chunkSize,
      int mutationCount,
      int mutationBatchSize,
      Duration idleDrainTimeout,
      Duration mutationPause,
      ScenarioRequestMode requestMode,
      ScenarioFaultPlan faultPlan) {
    public CommonScenarioConfig {
      Objects.requireNonNull(scenarioId, "scenarioId");
      Objects.requireNonNull(adapter, "adapter");
      Objects.requireNonNull(sourceId, "sourceId");
      Objects.requireNonNull(statePath, "statePath");
      Objects.requireNonNull(sinkPath, "sinkPath");
      Objects.requireNonNull(idleDrainTimeout, "idleDrainTimeout");
      Objects.requireNonNull(mutationPause, "mutationPause");
      Objects.requireNonNull(requestMode, "requestMode");
      Objects.requireNonNull(faultPlan, "faultPlan");
      if (chunkSize <= 0 || mutationCount <= 0 || mutationBatchSize <= 0) {
        throw new IllegalArgumentException("scenario counts and chunkSize must be > 0");
      }
    }
  }

  public record MySqlScenarioModeConfig(
      CommonScenarioConfig common,
      String jdbcUrl,
      String username,
      String password,
      String databaseName,
      String hostname,
      int port,
      long serverId,
      Duration connectTimeout,
      boolean retryLogConnectionLoss,
      Duration reconnectBackoff,
      int sourceEventQueueCapacity)
      implements ScenarioModeConfig {
    public MySqlScenarioModeConfig {
      Objects.requireNonNull(common, "common");
      Objects.requireNonNull(jdbcUrl, "jdbcUrl");
      Objects.requireNonNull(username, "username");
      Objects.requireNonNull(password, "password");
      Objects.requireNonNull(databaseName, "databaseName");
      Objects.requireNonNull(hostname, "hostname");
      Objects.requireNonNull(connectTimeout, "connectTimeout");
      Objects.requireNonNull(reconnectBackoff, "reconnectBackoff");
      if (port <= 0 || serverId <= 0 || sourceEventQueueCapacity <= 0) {
        throw new IllegalArgumentException("mysql scenario numeric properties must be > 0");
      }
    }
  }

  public record PostgresScenarioModeConfig(
      CommonScenarioConfig common,
      String jdbcUrl,
      String replicationJdbcUrl,
      String username,
      String password,
      String databaseName,
      Duration statusInterval,
      boolean retryLogConnectionLoss,
      Duration reconnectBackoff)
      implements ScenarioModeConfig {
    public PostgresScenarioModeConfig {
      Objects.requireNonNull(common, "common");
      Objects.requireNonNull(jdbcUrl, "jdbcUrl");
      Objects.requireNonNull(replicationJdbcUrl, "replicationJdbcUrl");
      Objects.requireNonNull(username, "username");
      Objects.requireNonNull(password, "password");
      Objects.requireNonNull(databaseName, "databaseName");
      Objects.requireNonNull(statusInterval, "statusInterval");
      Objects.requireNonNull(reconnectBackoff, "reconnectBackoff");
    }
  }
}
