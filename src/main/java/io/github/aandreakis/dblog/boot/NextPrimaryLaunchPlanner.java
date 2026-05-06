package io.github.aandreakis.dblog.boot;

import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.config.DbLogBootstrapMode;
import io.github.aandreakis.dblog.config.DbLogControlPlaneProperties;
import io.github.aandreakis.dblog.config.DbLogMysqlProperties;
import io.github.aandreakis.dblog.config.DbLogPostgresProperties;
import io.github.aandreakis.dblog.config.DbLogProperties;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

final class NextPrimaryLaunchPlanner {
  NextPrimaryApplication.RuntimeLaunchPlan runtimePlan(
      DbLogProperties boundProperties, String adapterKey) {
    Objects.requireNonNull(boundProperties, "boundProperties");
    Path statePath = requirePath(boundProperties.getRuntime().getStatePath(), "dblog.runtime.state-path");
    return new NextPrimaryApplication.RuntimeLaunchPlan(
        NextPrimaryApplication.requireNonBlank(adapterKey, "adapterKey"),
        boundProperties,
        parseSourceConfig(boundProperties),
        statePath,
        boundProperties.getChunk().getSize(),
        parseControlPlaneConfig(boundProperties));
  }

  NextPrimaryApplication.StartupCheckLaunchPlan startupCheckPlan(
      DbLogProperties boundProperties, String adapterKey) {
    Objects.requireNonNull(boundProperties, "boundProperties");
    Path statePath = requirePath(boundProperties.getRuntime().getStatePath(), "dblog.runtime.state-path");
    return new NextPrimaryApplication.StartupCheckLaunchPlan(
        NextPrimaryApplication.requireNonBlank(adapterKey, "adapterKey"),
        boundProperties,
        parseSourceConfig(boundProperties),
        statePath);
  }

  static String bootstrapAdapterKey(DbLogBootstrapMode mode, DbLogProperties properties) {
    Objects.requireNonNull(properties, "properties");
    return switch (Objects.requireNonNull(mode, "mode")) {
      case RUNTIME, STARTUP_CHECK ->
          NextPrimaryApplication.requireNonBlank(
              properties.getSource().getAdapter(), "dblog.source.adapter");
      case SCENARIO ->
          NextPrimaryApplication.requireNonBlank(
              properties.getScenario().getAdapter(), "dblog.scenario.adapter");
    };
  }

  private RelationalSourceConfig parseSourceConfig(DbLogProperties properties) {
    Objects.requireNonNull(properties, "properties");
    DbLogProperties.Source source = properties.getSource();
    String adapterKey =
        NextPrimaryApplication.requireNonBlank(
            source.getAdapter(), "dblog.source.adapter");
    String normalizedAdapter = adapterKey.trim().toLowerCase(Locale.ROOT);
    String sourceId =
        NextPrimaryApplication.requireNonBlank(
            source.getId(), "dblog.source.id");
    List<String> capturedTables = List.copyOf(source.getTables());
    if (capturedTables.isEmpty()) {
      throw new IllegalArgumentException("dblog.source.tables must not be empty");
    }

    return switch (normalizedAdapter) {
      case "mysql" ->
          new RelationalSourceConfig(
              sourceId,
              NextPrimaryApplication.requireNonBlank(
                  source.getMysql().getJdbcUrl(), "dblog.source.mysql.jdbc-url"),
              NextPrimaryApplication.requireNonBlank(
                  source.getMysql().getUsername(), "dblog.source.mysql.username"),
              source.getMysql().getPassword(),
              NextPrimaryApplication.trimToNull(source.getMysql().getDatabaseName()),
              capturedTables,
              mysqlOptions(source.getMysql()),
              properties.getRuntime().isRetainTransactionHistory());
      case "postgres", "postgresql" ->
          new RelationalSourceConfig(
              sourceId,
              NextPrimaryApplication.requireNonBlank(
                  source.getPostgres().getJdbcUrl(),
                  "dblog.source.postgres.jdbc-url"),
              NextPrimaryApplication.requireNonBlank(
                  source.getPostgres().getUsername(),
                  "dblog.source.postgres.username"),
              source.getPostgres().getPassword(),
              NextPrimaryApplication.trimToNull(source.getPostgres().getDatabaseName()),
              capturedTables,
              postgresOptions(source.getPostgres()),
              properties.getRuntime().isRetainTransactionHistory());
      default -> throw new IllegalArgumentException("Unsupported source adapter: " + adapterKey);
    };
  }

  private Map<String, String> mysqlOptions(DbLogMysqlProperties mysql) {
    LinkedHashMap<String, String> options = new LinkedHashMap<>();
    options.put("mysql.serverId", Long.toString(mysql.getServerId()));
    options.put("mysql.connectTimeout", mysql.getConnectTimeout().toString());
    options.put("mysql.heartbeatInterval", mysql.getHeartbeatInterval().toString());
    options.put("mysql.keepAliveInterval", mysql.getKeepAliveInterval().toString());
    options.put("mysql.netWriteTimeout", mysql.getNetWriteTimeout().toString());
    options.put("mysql.sourceEventQueueCapacity", Integer.toString(mysql.getSourceEventQueueCapacity()));
    if (mysql.isRetryLogConnectionLoss()) {
      options.put("mysql.retryLogConnectionLoss", "true");
    }
    options.put("mysql.reconnectBackoff", mysql.getReconnectBackoff().toString());
    return Map.copyOf(options);
  }

  private Map<String, String> postgresOptions(DbLogPostgresProperties postgres) {
    LinkedHashMap<String, String> options = new LinkedHashMap<>();
    copyIfPresent(
        options,
        "postgres.replicationJdbcUrl",
        NextPrimaryApplication.trimToNull(postgres.getReplicationJdbcUrl()));
    copyIfPresent(
        options,
        "postgres.publicationName",
        NextPrimaryApplication.trimToNull(postgres.getPublicationName()));
    options.put("postgres.publicationOwnership", postgres.getPublicationOwnership().name());
    copyIfPresent(
        options,
        "postgres.slotName",
        NextPrimaryApplication.trimToNull(postgres.getSlotName()));
    options.put("postgres.slotOwnership", postgres.getSlotOwnership().name());
    options.put("postgres.statusInterval", postgres.getStatusInterval().toString());
    if (postgres.isRetryLogConnectionLoss()) {
      options.put("postgres.retryLogConnectionLoss", "true");
    }
    options.put("postgres.reconnectBackoff", postgres.getReconnectBackoff().toString());
    return Map.copyOf(options);
  }

  private NextPrimaryApplication.ControlPlaneConfig parseControlPlaneConfig(
      DbLogProperties properties) {
    Objects.requireNonNull(properties, "properties");
    DbLogControlPlaneProperties controlPlane = properties.getControlPlane();
    return new NextPrimaryApplication.ControlPlaneConfig(
        controlPlane.isEnabled(),
        controlPlane.getHost(),
        controlPlane.getPort(),
        controlPlane.isAllowNonLoopback(),
        controlPlane.getExecutorMaxThreads(),
        controlPlane.getExecutorQueueCapacity(),
        controlPlane.getMaxRequestBodyBytes(),
        controlPlane.getPortFile());
  }

  private static void copyIfPresent(
      Map<String, String> target, String targetKey, String value) {
    if (value != null) {
      target.put(targetKey, value);
    }
  }

  private static Path requirePath(Path path, String key) {
    if (path == null) {
      throw new IllegalArgumentException(key + " must not be blank");
    }
    return path;
  }
}
