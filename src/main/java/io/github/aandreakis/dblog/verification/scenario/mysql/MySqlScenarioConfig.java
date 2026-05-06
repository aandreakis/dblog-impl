package io.github.aandreakis.dblog.verification.scenario.mysql;

import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.config.DbLogMysqlProperties;
import io.github.aandreakis.dblog.config.DbLogProperties;
import io.github.aandreakis.dblog.config.DbLogScenarioProperties;
import io.github.aandreakis.dblog.config.ScenarioRequestMode;
import io.github.aandreakis.dblog.runtime.bootstrap.RuntimeTuningSupport;
import io.github.aandreakis.dblog.verification.scenario.ScenarioConfigCommonSupport;
import io.github.aandreakis.dblog.verification.scenario.ScenarioConfigView;
import io.github.aandreakis.dblog.verification.scenario.ScenarioFaultPlan;
import io.github.aandreakis.dblog.verification.scenario.mysql.internal.MySqlScenarioConfigSupport;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.springframework.core.env.Environment;

/** Runtime configuration for the MySQL production scenario harness. */
public record MySqlScenarioConfig(
    String scenarioId,
    String sourceId,
    String databaseName,
    String jdbcUrl,
    String username,
    String password,
    String hostname,
    int port,
    long serverId,
    Path statePath,
    Path sinkPath,
    boolean resetSource,
    int chunkSize,
    int mutationCount,
    int mutationBatchSize,
    Duration idleDrainTimeout,
    Duration mutationPause,
    Duration connectTimeout,
    Duration heartbeatInterval,
    Duration keepAliveInterval,
    Duration netWriteTimeout,
    boolean retryLogConnectionLoss,
    Duration reconnectBackoff,
    int sourceEventQueueCapacity,
    Duration sinkDelay,
    Duration closeRuntimeSqlConnectionAfter,
    Duration alterCapturedSchemaAfter,
    Duration alterMetadataShapeAfter,
    Duration deleteMetadataRowAfter,
    Integer failSinkAfterAppendCount,
    Integer crashBeforeRequestAckBatchIndex,
    ScenarioRequestMode requestMode,
    ScenarioFaultPlan faultPlan)
    implements ScenarioConfigView {

  public MySqlScenarioConfig(
      String scenarioId,
      String sourceId,
      String databaseName,
      String jdbcUrl,
      String username,
      String password,
      String hostname,
      int port,
      long serverId,
      Path statePath,
      Path sinkPath,
      boolean resetSource,
      int chunkSize,
      int mutationCount,
      Duration idleDrainTimeout,
      Duration mutationPause,
      Duration connectTimeout,
      Duration sinkDelay,
      Duration closeRuntimeSqlConnectionAfter,
      Duration alterCapturedSchemaAfter,
      Duration alterMetadataShapeAfter,
      Duration deleteMetadataRowAfter,
      Integer failSinkAfterAppendCount,
      Integer crashBeforeRequestAckBatchIndex,
      ScenarioRequestMode requestMode,
      ScenarioFaultPlan faultPlan) {
    this(
        scenarioId,
        sourceId,
        databaseName,
        jdbcUrl,
        username,
        password,
        hostname,
        port,
        serverId,
        statePath,
        sinkPath,
        resetSource,
        chunkSize,
        mutationCount,
        1,
        idleDrainTimeout,
        mutationPause,
        connectTimeout,
        Duration.ofSeconds(5),
        Duration.ofSeconds(300),
        Duration.ofMinutes(10),
        false,
        Duration.ofSeconds(3),
        50_000,
        sinkDelay,
        closeRuntimeSqlConnectionAfter,
        alterCapturedSchemaAfter,
        alterMetadataShapeAfter,
        deleteMetadataRowAfter,
        failSinkAfterAppendCount,
        crashBeforeRequestAckBatchIndex,
        requestMode,
        faultPlan);
  }

  public MySqlScenarioConfig(
      String scenarioId,
      String sourceId,
      String databaseName,
      String jdbcUrl,
      String username,
      String password,
      String hostname,
      int port,
      long serverId,
      Path statePath,
      Path sinkPath,
      boolean resetSource,
      int chunkSize,
      int mutationCount,
      int mutationBatchSize,
      Duration idleDrainTimeout,
      Duration mutationPause,
      Duration connectTimeout,
      boolean retryLogConnectionLoss,
      Duration reconnectBackoff,
      Duration sinkDelay,
      Duration closeRuntimeSqlConnectionAfter,
      Duration alterCapturedSchemaAfter,
      Duration alterMetadataShapeAfter,
      Duration deleteMetadataRowAfter,
      Integer failSinkAfterAppendCount,
      Integer crashBeforeRequestAckBatchIndex,
      ScenarioRequestMode requestMode,
      ScenarioFaultPlan faultPlan) {
    this(
        scenarioId,
        sourceId,
        databaseName,
        jdbcUrl,
        username,
        password,
        hostname,
        port,
        serverId,
        statePath,
        sinkPath,
        resetSource,
        chunkSize,
        mutationCount,
        mutationBatchSize,
        idleDrainTimeout,
        mutationPause,
        connectTimeout,
        Duration.ofSeconds(5),
        Duration.ofSeconds(300),
        Duration.ofMinutes(10),
        retryLogConnectionLoss,
        reconnectBackoff,
        50_000,
        sinkDelay,
        closeRuntimeSqlConnectionAfter,
        alterCapturedSchemaAfter,
        alterMetadataShapeAfter,
        deleteMetadataRowAfter,
        failSinkAfterAppendCount,
        crashBeforeRequestAckBatchIndex,
        requestMode,
        faultPlan);
  }

  public MySqlScenarioConfig(
      String scenarioId,
      String sourceId,
      String databaseName,
      String jdbcUrl,
      String username,
      String password,
      String hostname,
      int port,
      long serverId,
      Path statePath,
      Path sinkPath,
      boolean resetSource,
      int chunkSize,
      int mutationCount,
      int mutationBatchSize,
      Duration idleDrainTimeout,
      Duration mutationPause,
      Duration connectTimeout,
      Duration sinkDelay,
      Duration closeRuntimeSqlConnectionAfter,
      Duration alterCapturedSchemaAfter,
      Duration alterMetadataShapeAfter,
      Duration deleteMetadataRowAfter,
      Integer failSinkAfterAppendCount,
      Integer crashBeforeRequestAckBatchIndex,
      ScenarioRequestMode requestMode,
      ScenarioFaultPlan faultPlan) {
    this(
        scenarioId,
        sourceId,
        databaseName,
        jdbcUrl,
        username,
        password,
        hostname,
        port,
        serverId,
        statePath,
        sinkPath,
        resetSource,
        chunkSize,
        mutationCount,
        mutationBatchSize,
        idleDrainTimeout,
        mutationPause,
        connectTimeout,
        Duration.ofSeconds(5),
        Duration.ofSeconds(300),
        Duration.ofMinutes(10),
        false,
        Duration.ofSeconds(3),
        50_000,
        sinkDelay,
        closeRuntimeSqlConnectionAfter,
        alterCapturedSchemaAfter,
        alterMetadataShapeAfter,
        deleteMetadataRowAfter,
        failSinkAfterAppendCount,
        crashBeforeRequestAckBatchIndex,
        requestMode,
        faultPlan);
  }

  public MySqlScenarioConfig {
    ScenarioConfigCommonSupport.CommonSettings common =
        ScenarioConfigCommonSupport.validateCommon(
            scenarioId,
            sourceId,
            statePath,
            sinkPath,
            resetSource,
            chunkSize,
            mutationCount,
            mutationBatchSize,
            idleDrainTimeout,
            mutationPause,
            sinkDelay,
            closeRuntimeSqlConnectionAfter,
            alterCapturedSchemaAfter,
            alterMetadataShapeAfter,
            deleteMetadataRowAfter,
            failSinkAfterAppendCount,
            crashBeforeRequestAckBatchIndex,
            requestMode,
            faultPlan);
    scenarioId = common.scenarioId();
    sourceId = common.sourceId();
    statePath = common.statePath();
    sinkPath = common.sinkPath();
    resetSource = common.resetSource();
    chunkSize = common.chunkSize();
    mutationCount = common.mutationCount();
    mutationBatchSize = common.mutationBatchSize();
    idleDrainTimeout = common.idleDrainTimeout();
    mutationPause = common.mutationPause();
    sinkDelay = common.sinkDelay();
    closeRuntimeSqlConnectionAfter = common.closeRuntimeSqlConnectionAfter();
    alterCapturedSchemaAfter = common.alterCapturedSchemaAfter();
    alterMetadataShapeAfter = common.alterMetadataShapeAfter();
    deleteMetadataRowAfter = common.deleteMetadataRowAfter();
    failSinkAfterAppendCount = common.failSinkAfterAppendCount();
    crashBeforeRequestAckBatchIndex = common.crashBeforeRequestAckBatchIndex();
    requestMode = common.requestMode();
    faultPlan = common.faultPlan();
    databaseName =
        MySqlScenarioConfigSupport.requireNonBlank(databaseName, "databaseName");
    jdbcUrl = MySqlScenarioConfigSupport.requireNonBlank(jdbcUrl, "jdbcUrl");
    username = MySqlScenarioConfigSupport.requireNonBlank(username, "username");
    password = Objects.requireNonNull(password, "password");
    hostname = MySqlScenarioConfigSupport.requireNonBlank(hostname, "hostname");
    if (port <= 0 || port > 65535) throw new IllegalArgumentException("port must be 1..65535");
    if (serverId <= 0) throw new IllegalArgumentException("serverId must be > 0");
    connectTimeout =
        MySqlScenarioConfigSupport.positiveDuration(connectTimeout, "connectTimeout");
    heartbeatInterval =
        MySqlScenarioConfigSupport.positiveDuration(heartbeatInterval, "heartbeatInterval");
    keepAliveInterval =
        MySqlScenarioConfigSupport.positiveDuration(
            keepAliveInterval, "keepAliveInterval");
    netWriteTimeout =
        MySqlScenarioConfigSupport.positiveDuration(netWriteTimeout, "netWriteTimeout");
    if (keepAliveInterval.compareTo(heartbeatInterval) <= 0) {
      throw new IllegalArgumentException(
          "keepAliveInterval must be greater than heartbeatInterval");
    }
    reconnectBackoff =
        MySqlScenarioConfigSupport.positiveDuration(reconnectBackoff, "reconnectBackoff");
    if (sourceEventQueueCapacity <= 0) {
      throw new IllegalArgumentException("sourceEventQueueCapacity must be > 0");
    }
  }

  public static MySqlScenarioConfig from(Environment environment) {
    ScenarioConfigCommonSupport.CommonSettings common =
        ScenarioConfigCommonSupport.from(
            environment, "-mysql", ScenarioConfigCommonSupport.scenarioChunkSize(environment));
    String jdbcUrl =
        MySqlScenarioConfigSupport.required(
            environment, "dblog.scenario.mysql.jdbc-url");
    MySqlScenarioConfigSupport.HostPort hostPort =
        MySqlScenarioConfigSupport.parseHostPort(
            jdbcUrl, "jdbc:mysql://", "MySQL", 3306);
    return new MySqlScenarioConfig(
        common.scenarioId(),
        common.sourceId(),
        environment.getProperty(
            "dblog.scenario.mysql.database-name",
            MySqlScenarioConfigSupport.databaseNameFromJdbcUrl(jdbcUrl, "MySQL")),
        jdbcUrl,
        MySqlScenarioConfigSupport.required(
            environment, "dblog.scenario.mysql.username"),
        environment.getProperty("dblog.scenario.mysql.password", ""),
        environment.getProperty("dblog.scenario.mysql.hostname", hostPort.host()),
        environment.getProperty("dblog.scenario.mysql.port", Integer.class, hostPort.port()),
        environment.getProperty("dblog.scenario.mysql.server-id", Long.class, 223344L),
        common.statePath(),
        common.sinkPath(),
        common.resetSource(),
        common.chunkSize(),
        common.mutationCount(),
        common.mutationBatchSize(),
        common.idleDrainTimeout(),
        common.mutationPause(),
        environment.getProperty(
            "dblog.scenario.mysql.connect-timeout", Duration.class, Duration.ofSeconds(5)),
        environment.getProperty(
            "dblog.scenario.mysql.heartbeat-interval", Duration.class, Duration.ofSeconds(5)),
        environment.getProperty(
            "dblog.scenario.mysql.keep-alive-interval", Duration.class, Duration.ofSeconds(300)),
        environment.getProperty(
            "dblog.scenario.mysql.net-write-timeout", Duration.class, Duration.ofMinutes(10)),
        environment.getProperty(
            "dblog.scenario.mysql.retry-log-connection-loss", Boolean.class, false),
        environment.getProperty(
            "dblog.scenario.mysql.reconnect-backoff", Duration.class, Duration.ofSeconds(3)),
        environment.getProperty(
            "dblog.scenario.mysql.source-event-queue-capacity", Integer.class, 50_000),
        common.sinkDelay(),
        common.closeRuntimeSqlConnectionAfter(),
        common.alterCapturedSchemaAfter(),
        common.alterMetadataShapeAfter(),
        common.deleteMetadataRowAfter(),
        common.failSinkAfterAppendCount(),
        common.crashBeforeRequestAckBatchIndex(),
        common.requestMode(),
        common.faultPlan());
  }

  public static MySqlScenarioConfig from(DbLogProperties properties) {
    ScenarioConfigCommonSupport.CommonSettings common =
        ScenarioConfigCommonSupport.from(properties, "-mysql");
    DbLogScenarioProperties scenario = properties.getScenario();
    DbLogMysqlProperties mysql = scenario.getMysql();
    String jdbcUrl =
        MySqlScenarioConfigSupport.requireNonBlank(
            mysql.getJdbcUrl(), "scenario.mysql.jdbcUrl");
    MySqlScenarioConfigSupport.HostPort hostPort =
        MySqlScenarioConfigSupport.parseHostPort(
            jdbcUrl, "jdbc:mysql://", "MySQL", 3306);
    return new MySqlScenarioConfig(
        common.scenarioId(),
        common.sourceId(),
        mysql.getDatabaseName() == null
            ? MySqlScenarioConfigSupport.databaseNameFromJdbcUrl(jdbcUrl, "MySQL")
            : mysql.getDatabaseName(),
        jdbcUrl,
        MySqlScenarioConfigSupport.requireNonBlank(
            mysql.getUsername(), "scenario.mysql.username"),
        mysql.getPassword() == null ? "" : mysql.getPassword(),
        mysql.getHostname() == null ? hostPort.host() : mysql.getHostname(),
        mysql.getPort() == null ? hostPort.port() : mysql.getPort(),
        mysql.getServerId() == null ? 223344L : mysql.getServerId(),
        common.statePath(),
        common.sinkPath(),
        common.resetSource(),
        common.chunkSize(),
        common.mutationCount(),
        common.mutationBatchSize(),
        common.idleDrainTimeout(),
        common.mutationPause(),
        mysql.getConnectTimeout(),
        mysql.getHeartbeatInterval(),
        mysql.getKeepAliveInterval(),
        mysql.getNetWriteTimeout(),
        mysql.isRetryLogConnectionLoss(),
        mysql.getReconnectBackoff(),
        mysql.getSourceEventQueueCapacity(),
        common.sinkDelay(),
        common.closeRuntimeSqlConnectionAfter(),
        common.alterCapturedSchemaAfter(),
        common.alterMetadataShapeAfter(),
        common.deleteMetadataRowAfter(),
        common.failSinkAfterAppendCount(),
        common.crashBeforeRequestAckBatchIndex(),
        common.requestMode(),
        common.faultPlan());
  }

  public RelationalSourceConfig mirrorSourceConfig() {
    return new RelationalSourceConfig(
        sourceId,
        jdbcUrl,
        username,
        password,
        databaseName,
        List.of(databaseName + ".widgets", databaseName + ".gadgets"),
        Map.of(),
        false);
  }
}
