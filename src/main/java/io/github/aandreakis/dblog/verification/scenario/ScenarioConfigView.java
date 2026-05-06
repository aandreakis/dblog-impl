package io.github.aandreakis.dblog.verification.scenario;

import io.github.aandreakis.dblog.config.ScenarioRequestMode;
import java.nio.file.Path;
import java.time.Duration;

/** Shared read-only surface for scenario runner configuration records. */
public interface ScenarioConfigView {
  String scenarioId();

  String sourceId();

  String jdbcUrl();

  String username();

  String password();

  Path statePath();

  Path sinkPath();

  boolean resetSource();

  int chunkSize();

  int mutationCount();

  int mutationBatchSize();

  Duration idleDrainTimeout();

  Duration mutationPause();

  Duration sinkDelay();

  Duration closeRuntimeSqlConnectionAfter();

  Duration alterCapturedSchemaAfter();

  Duration alterMetadataShapeAfter();

  Duration deleteMetadataRowAfter();

  Integer failSinkAfterAppendCount();

  Integer crashBeforeRequestAckBatchIndex();

  ScenarioRequestMode requestMode();

  ScenarioFaultPlan faultPlan();
}
