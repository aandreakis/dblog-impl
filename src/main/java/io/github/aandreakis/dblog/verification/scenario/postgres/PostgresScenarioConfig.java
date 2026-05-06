package io.github.aandreakis.dblog.verification.scenario.postgres;

import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.config.DbLogPostgresProperties;
import io.github.aandreakis.dblog.config.DbLogProperties;
import io.github.aandreakis.dblog.config.DbLogScenarioProperties;
import io.github.aandreakis.dblog.config.ScenarioRequestMode;
import io.github.aandreakis.dblog.runtime.bootstrap.RuntimeTuningSupport;
import io.github.aandreakis.dblog.verification.scenario.ScenarioConfigCommonSupport;
import io.github.aandreakis.dblog.verification.scenario.ScenarioConfigView;
import io.github.aandreakis.dblog.verification.scenario.ScenarioFaultPlan;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.springframework.core.env.Environment;

/** Runtime configuration for the PostgreSQL production scenario harness. */
public record PostgresScenarioConfig(
    String scenarioId,
    String sourceId,
    String databaseName,
    String jdbcUrl,
    String replicationJdbcUrl,
    String username,
    String password,
    Path statePath,
    Path sinkPath,
    boolean resetSource,
    int chunkSize,
    int mutationCount,
    int mutationBatchSize,
    Duration statusInterval,
    boolean retryLogConnectionLoss,
    Duration reconnectBackoff,
    Duration idleDrainTimeout,
    Duration mutationPause,
    Duration sinkDelay,
    Duration terminateReplicationBackendAfter,
    Duration closeRuntimeSqlConnectionAfter,
    Duration alterCapturedSchemaAfter,
    Duration alterMetadataShapeAfter,
    Duration deleteMetadataRowAfter,
    Integer failSinkAfterAppendCount,
    Integer crashBeforeRequestAckBatchIndex,
    ScenarioRequestMode requestMode,
    ScenarioFaultPlan faultPlan)
    implements ScenarioConfigView {

  public PostgresScenarioConfig(
      String scenarioId,
      String sourceId,
      String databaseName,
      String jdbcUrl,
      String replicationJdbcUrl,
      String username,
      String password,
      Path statePath,
      Path sinkPath,
      boolean resetSource,
      int chunkSize,
      int mutationCount,
      Duration statusInterval,
      Duration idleDrainTimeout,
      Duration mutationPause,
      Duration sinkDelay,
      Duration terminateReplicationBackendAfter,
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
        replicationJdbcUrl,
        username,
        password,
        statePath,
        sinkPath,
        resetSource,
        chunkSize,
        mutationCount,
        1,
        statusInterval,
        false,
        Duration.ofSeconds(3),
        idleDrainTimeout,
        mutationPause,
        sinkDelay,
        terminateReplicationBackendAfter,
        closeRuntimeSqlConnectionAfter,
        alterCapturedSchemaAfter,
        alterMetadataShapeAfter,
        deleteMetadataRowAfter,
        failSinkAfterAppendCount,
        crashBeforeRequestAckBatchIndex,
        requestMode,
        faultPlan);
  }

  public PostgresScenarioConfig(
      String scenarioId,
      String sourceId,
      String databaseName,
      String jdbcUrl,
      String replicationJdbcUrl,
      String username,
      String password,
      Path statePath,
      Path sinkPath,
      boolean resetSource,
      int chunkSize,
      int mutationCount,
      int mutationBatchSize,
      Duration statusInterval,
      Duration idleDrainTimeout,
      Duration mutationPause,
      Duration sinkDelay,
      Duration terminateReplicationBackendAfter,
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
        replicationJdbcUrl,
        username,
        password,
        statePath,
        sinkPath,
        resetSource,
        chunkSize,
        mutationCount,
        mutationBatchSize,
        statusInterval,
        false,
        Duration.ofSeconds(3),
        idleDrainTimeout,
        mutationPause,
        sinkDelay,
        terminateReplicationBackendAfter,
        closeRuntimeSqlConnectionAfter,
        alterCapturedSchemaAfter,
        alterMetadataShapeAfter,
        deleteMetadataRowAfter,
        failSinkAfterAppendCount,
        crashBeforeRequestAckBatchIndex,
        requestMode,
        faultPlan);
  }

  public PostgresScenarioConfig {
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
    databaseName = requireNonBlank(databaseName, "databaseName");
    jdbcUrl = requireNonBlank(jdbcUrl, "jdbcUrl");
    replicationJdbcUrl =
        requireNonBlank(replicationJdbcUrl == null ? jdbcUrl : replicationJdbcUrl, "replicationJdbcUrl");
    username = requireNonBlank(username, "username");
    password = Objects.requireNonNull(password, "password");
    statusInterval = positiveDuration(statusInterval, "statusInterval");
    reconnectBackoff = positiveDuration(reconnectBackoff, "reconnectBackoff");
    terminateReplicationBackendAfter =
        terminateReplicationBackendAfter == null
            ? Duration.ZERO
            : nonNegativeDuration(terminateReplicationBackendAfter, "terminateReplicationBackendAfter");
  }

  public static PostgresScenarioConfig from(Environment environment) {
    ScenarioConfigCommonSupport.CommonSettings common =
        ScenarioConfigCommonSupport.from(
            environment, "-postgres", ScenarioConfigCommonSupport.scenarioChunkSize(environment));
    String jdbcUrl = required(environment, "dblog.scenario.postgres.jdbc-url");
    return new PostgresScenarioConfig(
        common.scenarioId(),
        common.sourceId(),
        environment.getProperty(
            "dblog.scenario.postgres.database-name", databaseNameFromJdbcUrl(jdbcUrl)),
        jdbcUrl,
        environment.getProperty("dblog.scenario.postgres.replication-jdbc-url", jdbcUrl),
        required(environment, "dblog.scenario.postgres.username"),
        environment.getProperty("dblog.scenario.postgres.password", ""),
        common.statePath(),
        common.sinkPath(),
        common.resetSource(),
        common.chunkSize(),
        common.mutationCount(),
        common.mutationBatchSize(),
        environment.getProperty("dblog.scenario.status-interval", Duration.class, Duration.ofSeconds(5)),
        environment.getProperty("dblog.scenario.postgres.retry-log-connection-loss", Boolean.class, false),
        environment.getProperty("dblog.scenario.postgres.reconnect-backoff", Duration.class, Duration.ofSeconds(3)),
        common.idleDrainTimeout(),
        common.mutationPause(),
        common.sinkDelay(),
        environment.getProperty("dblog.scenario.terminate-replication-backend-after", Duration.class, Duration.ZERO),
        common.closeRuntimeSqlConnectionAfter(),
        common.alterCapturedSchemaAfter(),
        common.alterMetadataShapeAfter(),
        common.deleteMetadataRowAfter(),
        common.failSinkAfterAppendCount(),
        common.crashBeforeRequestAckBatchIndex(),
        common.requestMode(),
        common.faultPlan());
  }

  public static PostgresScenarioConfig from(DbLogProperties properties) {
    ScenarioConfigCommonSupport.CommonSettings common =
        ScenarioConfigCommonSupport.from(properties, "-postgres");
    DbLogScenarioProperties scenario = properties.getScenario();
    DbLogPostgresProperties postgres = scenario.getPostgres();
    String jdbcUrl = requireNonBlank(postgres.getJdbcUrl(), "scenario.postgres.jdbcUrl");
    return new PostgresScenarioConfig(
        common.scenarioId(),
        common.sourceId(),
        postgres.getDatabaseName() == null
            ? databaseNameFromJdbcUrl(jdbcUrl)
            : postgres.getDatabaseName(),
        jdbcUrl,
        postgres.getReplicationJdbcUrl() == null ? jdbcUrl : postgres.getReplicationJdbcUrl(),
        requireNonBlank(postgres.getUsername(), "scenario.postgres.username"),
        postgres.getPassword() == null ? "" : postgres.getPassword(),
        common.statePath(),
        common.sinkPath(),
        common.resetSource(),
        common.chunkSize(),
        common.mutationCount(),
        common.mutationBatchSize(),
        postgres.getStatusInterval(),
        postgres.isRetryLogConnectionLoss(),
        postgres.getReconnectBackoff(),
        common.idleDrainTimeout(),
        common.mutationPause(),
        common.sinkDelay(),
        scenario.getFault().getTerminateReplicationBackendAfter(),
        common.closeRuntimeSqlConnectionAfter(),
        common.alterCapturedSchemaAfter(),
        common.alterMetadataShapeAfter(),
        common.deleteMetadataRowAfter(),
        common.failSinkAfterAppendCount(),
        common.crashBeforeRequestAckBatchIndex(),
        common.requestMode(),
        common.faultPlan());
  }

  private static String databaseNameFromJdbcUrl(String jdbcUrl) {
    int slash = jdbcUrl.lastIndexOf('/');
    if (slash < 0 || slash == jdbcUrl.length() - 1) {
      throw new IllegalArgumentException(
          "Could not infer PostgreSQL database name from JDBC URL; set dblog.scenario.postgres.database-name explicitly");
    }
    String tail = jdbcUrl.substring(slash + 1);
    int params = tail.indexOf('?');
    return params >= 0 ? tail.substring(0, params) : tail;
  }

  private static String required(Environment environment, String key) {
    return ScenarioConfigCommonSupport.required(environment, key);
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static Duration positiveDuration(Duration value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isZero() || value.isNegative()) {
      throw new IllegalArgumentException(name + " must be > 0");
    }
    return value;
  }

  private static Duration nonNegativeDuration(Duration value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isNegative()) {
      throw new IllegalArgumentException(name + " must be >= 0");
    }
    return value;
  }

  public RelationalSourceConfig mirrorSourceConfig() {
    PostgresScenarioSchema scenarioSchema = PostgresScenarioSchema.forScenario(this);
    return new RelationalSourceConfig(
        sourceId,
        jdbcUrl,
        username,
        password,
        databaseName,
        List.of(
            scenarioSchema.schemaName() + ".widgets",
            scenarioSchema.schemaName() + ".gadgets"),
        Map.of(),
        false);
  }
}
