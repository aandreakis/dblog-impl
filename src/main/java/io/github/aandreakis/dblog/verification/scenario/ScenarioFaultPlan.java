package io.github.aandreakis.dblog.verification.scenario;

import io.github.aandreakis.dblog.config.DbLogFaultProperties;
import io.github.aandreakis.dblog.config.DbLogProperties;
import io.github.aandreakis.dblog.config.DbLogScenarioProperties;
import java.time.Duration;
import java.util.Objects;
import org.springframework.core.env.Environment;

/**
 * Scenario fault-injection plan.
 *
 * <p>This mirrors the shipped scenario fault surface without depending on any non-next config
 * helpers.
 */
public record ScenarioFaultPlan(
    Duration sinkDelay,
    Integer failSinkAfterAppendCount,
    Integer crashBeforeRequestAckBatchIndex,
    Duration runtimeReadDelay,
    Integer failRuntimeReadAfterCount,
    Duration runtimeAcknowledgeDelay,
    Integer failRuntimeAcknowledgeAfterCount,
    Duration chunkReadDelay,
    Integer failChunkReadAfterCount,
    Duration stateStoreDelay,
    Integer failStateStoreOperationAfterCount,
    String failStateStoreOperationName,
    Duration terminateReplicationBackendAfter,
    Duration closeRuntimeSqlConnectionAfter,
    Duration alterCapturedSchemaAfter,
    Duration alterMetadataShapeAfter,
    Duration deleteMetadataRowAfter,
    Duration nullHeartbeatTimestampAfter) {

  public ScenarioFaultPlan {
    sinkDelay = nonNegativeDuration(sinkDelay, "sinkDelay");
    runtimeReadDelay = nonNegativeDuration(runtimeReadDelay, "runtimeReadDelay");
    runtimeAcknowledgeDelay = nonNegativeDuration(runtimeAcknowledgeDelay, "runtimeAcknowledgeDelay");
    chunkReadDelay = nonNegativeDuration(chunkReadDelay, "chunkReadDelay");
    stateStoreDelay = nonNegativeDuration(stateStoreDelay, "stateStoreDelay");
    terminateReplicationBackendAfter =
        nonNegativeDuration(terminateReplicationBackendAfter, "terminateReplicationBackendAfter");
    closeRuntimeSqlConnectionAfter =
        nonNegativeDuration(closeRuntimeSqlConnectionAfter, "closeRuntimeSqlConnectionAfter");
    alterCapturedSchemaAfter =
        nonNegativeDuration(alterCapturedSchemaAfter, "alterCapturedSchemaAfter");
    alterMetadataShapeAfter =
        nonNegativeDuration(alterMetadataShapeAfter, "alterMetadataShapeAfter");
    deleteMetadataRowAfter =
        nonNegativeDuration(deleteMetadataRowAfter, "deleteMetadataRowAfter");
    nullHeartbeatTimestampAfter =
        nonNegativeDuration(nullHeartbeatTimestampAfter, "nullHeartbeatTimestampAfter");

    requirePositiveWhenPresent(failSinkAfterAppendCount, "failSinkAfterAppendCount");
    requirePositiveWhenPresent(crashBeforeRequestAckBatchIndex, "crashBeforeRequestAckBatchIndex");
    requirePositiveWhenPresent(failRuntimeReadAfterCount, "failRuntimeReadAfterCount");
    requirePositiveWhenPresent(
        failRuntimeAcknowledgeAfterCount, "failRuntimeAcknowledgeAfterCount");
    requirePositiveWhenPresent(failChunkReadAfterCount, "failChunkReadAfterCount");
    requirePositiveWhenPresent(
        failStateStoreOperationAfterCount, "failStateStoreOperationAfterCount");

    failStateStoreOperationName = normalizeOptional(failStateStoreOperationName);
  }

  public static ScenarioFaultPlan none() {
    return new ScenarioFaultPlan(
        Duration.ZERO,
        null,
        null,
        Duration.ZERO,
        null,
        Duration.ZERO,
        null,
        Duration.ZERO,
        null,
        Duration.ZERO,
        null,
        null,
        Duration.ZERO,
        Duration.ZERO,
        Duration.ZERO,
        Duration.ZERO,
        Duration.ZERO,
        Duration.ZERO);
  }

  public static ScenarioFaultPlan from(Environment environment) {
    Objects.requireNonNull(environment, "environment");
    return new ScenarioFaultPlan(
        environment.getProperty("dblog.scenario.sink-delay", Duration.class, Duration.ZERO),
        environment.getProperty("dblog.scenario.fail-sink-after-append-count", Integer.class),
        environment.getProperty(
            "dblog.scenario.crash-before-request-ack-batch-index", Integer.class),
        environment.getProperty("dblog.scenario.runtime-read-delay", Duration.class, Duration.ZERO),
        environment.getProperty("dblog.scenario.fail-runtime-read-after-count", Integer.class),
        environment.getProperty("dblog.scenario.runtime-ack-delay", Duration.class, Duration.ZERO),
        environment.getProperty("dblog.scenario.fail-runtime-ack-after-count", Integer.class),
        environment.getProperty("dblog.scenario.chunk-read-delay", Duration.class, Duration.ZERO),
        environment.getProperty("dblog.scenario.fail-chunk-read-after-count", Integer.class),
        environment.getProperty("dblog.scenario.state-store-delay", Duration.class, Duration.ZERO),
        environment.getProperty(
            "dblog.scenario.fail-state-store-operation-after-count", Integer.class),
        environment.getProperty("dblog.scenario.fail-state-store-operation-name"),
        environment.getProperty(
            "dblog.scenario.terminate-replication-backend-after", Duration.class, Duration.ZERO),
        environment.getProperty(
            "dblog.scenario.close-runtime-sql-connection-after", Duration.class, Duration.ZERO),
        environment.getProperty(
            "dblog.scenario.alter-captured-schema-after", Duration.class, Duration.ZERO),
        environment.getProperty(
            "dblog.scenario.alter-metadata-shape-after", Duration.class, Duration.ZERO),
        environment.getProperty(
            "dblog.scenario.delete-metadata-row-after", Duration.class, Duration.ZERO),
        environment.getProperty(
            "dblog.scenario.null-heartbeat-timestamp-after", Duration.class, Duration.ZERO));
  }

  public static ScenarioFaultPlan from(DbLogScenarioProperties scenario) {
    Objects.requireNonNull(scenario, "scenario");
    DbLogFaultProperties fault = scenario.getFault();
    return new ScenarioFaultPlan(
        fault.getSinkDelay(),
        scenario.getFailSinkAfterAppendCount(),
        scenario.getCrashBeforeRequestAckBatchIndex(),
        fault.getRuntimeReadDelay(),
        fault.getFailRuntimeReadAfterCount(),
        fault.getRuntimeAcknowledgeDelay(),
        fault.getFailRuntimeAcknowledgeAfterCount(),
        fault.getChunkReadDelay(),
        fault.getFailChunkReadAfterCount(),
        fault.getStateStoreDelay(),
        fault.getFailStateStoreOperationAfterCount(),
        fault.getFailStateStoreOperationName(),
        fault.getTerminateReplicationBackendAfter(),
        fault.getCloseRuntimeSqlConnectionAfter(),
        fault.getAlterCapturedSchemaAfter(),
        fault.getAlterMetadataShapeAfter(),
        fault.getDeleteMetadataRowAfter(),
        fault.getNullHeartbeatTimestampAfter());
  }

  public boolean shouldFailStateStoreOperation(String operationName, int invocationCount) {
    Objects.requireNonNull(operationName, "operationName");
    if (failStateStoreOperationAfterCount == null
        || invocationCount < failStateStoreOperationAfterCount) {
      return false;
    }
    if (failStateStoreOperationName == null) {
      return true;
    }
    return failStateStoreOperationName.equalsIgnoreCase(operationName);
  }

  private static Duration nonNegativeDuration(Duration value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isNegative()) {
      throw new IllegalArgumentException(name + " must be >= 0");
    }
    return value;
  }

  private static void requirePositiveWhenPresent(Integer value, String name) {
    if (value != null && value <= 0) {
      throw new IllegalArgumentException(name + " must be > 0 when present");
    }
  }

  private static String normalizeOptional(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value.trim();
  }
}
