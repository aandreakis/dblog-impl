package io.github.aandreakis.dblog.verification.scenario;

import io.github.aandreakis.dblog.config.DbLogProperties;
import io.github.aandreakis.dblog.config.DbLogScenarioProperties;
import io.github.aandreakis.dblog.config.ScenarioRequestMode;
import io.github.aandreakis.dblog.runtime.bootstrap.RuntimeTuningSupport;
import io.github.aandreakis.dblog.verification.scenario.ScenarioFaultPlan;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import org.springframework.core.env.Environment;

/**
 * Shared scenario-config parsing and validation support for the final next-owned scenario records.
 */
public final class ScenarioConfigCommonSupport {
  private ScenarioConfigCommonSupport() {}

  public static CommonSettings from(
      Environment environment, String defaultSourceIdSuffix, int chunkSize) {
    Objects.requireNonNull(environment, "environment");
    String scenarioId = required(environment, "dblog.scenario.id");
    return validateCommon(
        scenarioId,
        environment.getProperty("dblog.scenario.source-id", scenarioId + defaultSourceIdSuffix),
        Path.of(required(environment, "dblog.scenario.state-path")),
        Path.of(required(environment, "dblog.scenario.sink-path")),
        environment.getProperty("dblog.scenario.reset-source", Boolean.class, true),
        chunkSize,
        environment.getProperty("dblog.scenario.mutation-count", Integer.class, 100),
        environment.getProperty("dblog.scenario.mutation-batch-size", Integer.class, 1),
        environment.getProperty(
            "dblog.scenario.idle-drain-timeout", Duration.class, Duration.ofMillis(500)),
        environment.getProperty(
            "dblog.scenario.mutation-pause", Duration.class, Duration.ofMillis(20)),
        environment.getProperty("dblog.scenario.sink-delay", Duration.class, Duration.ZERO),
        environment.getProperty(
            "dblog.scenario.close-runtime-sql-connection-after", Duration.class, Duration.ZERO),
        environment.getProperty(
            "dblog.scenario.alter-captured-schema-after", Duration.class, Duration.ZERO),
        environment.getProperty(
            "dblog.scenario.alter-metadata-shape-after", Duration.class, Duration.ZERO),
        environment.getProperty(
            "dblog.scenario.delete-metadata-row-after", Duration.class, Duration.ZERO),
        environment.getProperty("dblog.scenario.fail-sink-after-append-count", Integer.class),
        environment.getProperty(
            "dblog.scenario.crash-before-request-ack-batch-index", Integer.class),
        ScenarioRequestMode.parse(
            environment.getProperty("dblog.scenario.request-mode", "ALL_TABLES")),
        ScenarioFaultPlan.from(environment));
  }

  public static int scenarioChunkSize(Environment environment) {
    Objects.requireNonNull(environment, "environment");
    Integer scenarioChunkSize =
        environment.getProperty("dblog.scenario.chunk-size", Integer.class);
    if (scenarioChunkSize != null) {
      if (scenarioChunkSize <= 0) {
        throw new IllegalArgumentException("Configured scenario chunk size must be > 0");
      }
      return scenarioChunkSize;
    }
    return RuntimeTuningSupport.chunkSize(environment);
  }

  public static CommonSettings from(DbLogProperties properties, String defaultSourceIdSuffix) {
    Objects.requireNonNull(properties, "properties");
    DbLogScenarioProperties scenario = properties.getScenario();
    String scenarioId = requireNonBlank(scenario.getId(), "scenario.id");
    return validateCommon(
        scenarioId,
        scenario.getSourceId() == null ? scenarioId + defaultSourceIdSuffix : scenario.getSourceId(),
        Objects.requireNonNull(scenario.getStatePath(), "scenario.statePath"),
        Objects.requireNonNull(scenario.getSinkPath(), "scenario.sinkPath"),
        scenario.isResetSource(),
        RuntimeTuningSupport.chunkSize(properties),
        scenario.getMutationCount(),
        scenario.getMutationBatchSize(),
        scenario.getIdleDrainTimeout(),
        scenario.getMutationPause(),
        scenario.getFault().getSinkDelay(),
        scenario.getFault().getCloseRuntimeSqlConnectionAfter(),
        scenario.getFault().getAlterCapturedSchemaAfter(),
        scenario.getFault().getAlterMetadataShapeAfter(),
        scenario.getFault().getDeleteMetadataRowAfter(),
        scenario.getFailSinkAfterAppendCount(),
        scenario.getCrashBeforeRequestAckBatchIndex(),
        ScenarioRequestMode.parse(
            scenario.getRequestMode() == null ? null : scenario.getRequestMode().name()),
        ScenarioFaultPlan.from(scenario));
  }

  public static CommonSettings validateCommon(
      String scenarioId,
      String sourceId,
      Path statePath,
      Path sinkPath,
      boolean resetSource,
      int chunkSize,
      int mutationCount,
      int mutationBatchSize,
      Duration idleDrainTimeout,
      Duration mutationPause,
      Duration sinkDelay,
      Duration closeRuntimeSqlConnectionAfter,
      Duration alterCapturedSchemaAfter,
      Duration alterMetadataShapeAfter,
      Duration deleteMetadataRowAfter,
      Integer failSinkAfterAppendCount,
      Integer crashBeforeRequestAckBatchIndex,
      ScenarioRequestMode requestMode,
      ScenarioFaultPlan faultPlan) {
    scenarioId = requireNonBlank(scenarioId, "scenarioId");
    sourceId = requireNonBlank(sourceId, "sourceId");
    statePath = Objects.requireNonNull(statePath, "statePath");
    sinkPath = Objects.requireNonNull(sinkPath, "sinkPath");
    requestMode = Objects.requireNonNull(requestMode, "requestMode");
    faultPlan = Objects.requireNonNull(faultPlan, "faultPlan");
    if (chunkSize <= 0) {
      throw new IllegalArgumentException("chunkSize must be > 0");
    }
    if (mutationCount <= 0) {
      throw new IllegalArgumentException("mutationCount must be > 0");
    }
    if (mutationBatchSize <= 0) {
      throw new IllegalArgumentException("mutationBatchSize must be > 0");
    }
    idleDrainTimeout = positiveDuration(idleDrainTimeout, "idleDrainTimeout");
    mutationPause = positiveDuration(mutationPause, "mutationPause");
    sinkDelay = nonNegativeDuration(sinkDelay, "sinkDelay");
    closeRuntimeSqlConnectionAfter =
        nonNegativeDuration(closeRuntimeSqlConnectionAfter, "closeRuntimeSqlConnectionAfter");
    alterCapturedSchemaAfter =
        nonNegativeDuration(alterCapturedSchemaAfter, "alterCapturedSchemaAfter");
    alterMetadataShapeAfter =
        nonNegativeDuration(alterMetadataShapeAfter, "alterMetadataShapeAfter");
    deleteMetadataRowAfter =
        nonNegativeDuration(deleteMetadataRowAfter, "deleteMetadataRowAfter");
    requirePositiveWhenPresent(failSinkAfterAppendCount, "failSinkAfterAppendCount");
    requirePositiveWhenPresent(
        crashBeforeRequestAckBatchIndex, "crashBeforeRequestAckBatchIndex");
    return new CommonSettings(
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
  }

  public static String required(Environment environment, String key) {
    String value = environment.getProperty(key);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("Required scenario property is missing: " + key);
    }
    return value;
  }

  public static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  public static Duration positiveDuration(Duration value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isZero() || value.isNegative()) {
      throw new IllegalArgumentException(name + " must be > 0");
    }
    return value;
  }

  public static Duration nonNegativeDuration(Duration value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isNegative()) {
      throw new IllegalArgumentException(name + " must be >= 0");
    }
    return value;
  }

  public static void requirePositiveWhenPresent(Integer value, String name) {
    if (value != null && value <= 0) {
      throw new IllegalArgumentException(name + " must be > 0 when present");
    }
  }

  public record CommonSettings(
      String scenarioId,
      String sourceId,
      Path statePath,
      Path sinkPath,
      boolean resetSource,
      int chunkSize,
      int mutationCount,
      int mutationBatchSize,
      Duration idleDrainTimeout,
      Duration mutationPause,
      Duration sinkDelay,
      Duration closeRuntimeSqlConnectionAfter,
      Duration alterCapturedSchemaAfter,
      Duration alterMetadataShapeAfter,
      Duration deleteMetadataRowAfter,
      Integer failSinkAfterAppendCount,
      Integer crashBeforeRequestAckBatchIndex,
      ScenarioRequestMode requestMode,
      ScenarioFaultPlan faultPlan) {}
}
