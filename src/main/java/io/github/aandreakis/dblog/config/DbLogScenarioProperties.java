package io.github.aandreakis.dblog.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.nio.file.Path;
import java.time.Duration;

/** Scenario-mode configuration surface. */
public class DbLogScenarioProperties {
  private boolean enabled = false;
  private String id;
  private String adapter;
  private String sourceId;

  /**
   * H2 database-file <strong>prefix</strong>, not a directory. See
   * {@link DbLogProperties.Runtime#getStatePath()} for details.
   */
  private Path statePath;

  private Path sinkPath;
  private boolean resetSource = true;

  @Min(1)
  private int mutationCount = 100;

  @Min(1)
  private int mutationBatchSize = 1;

  @NotNull
  private Duration idleDrainTimeout = Duration.ofMillis(500);

  @NotNull
  private Duration mutationPause = Duration.ofMillis(20);

  private Integer failSinkAfterAppendCount;
  private Integer crashBeforeRequestAckBatchIndex;
  private ScenarioRequestMode requestMode = ScenarioRequestMode.ALL_TABLES;

  @Valid private final DbLogFaultProperties fault = new DbLogFaultProperties();
  @Valid private final DbLogMysqlProperties mysql = new DbLogMysqlProperties();
  @Valid private final DbLogPostgresProperties postgres = new DbLogPostgresProperties();

  public boolean isEnabled() { return enabled; }
  public void setEnabled(boolean enabled) { this.enabled = enabled; }
  public String getId() { return id; }
  public void setId(String id) { this.id = id; }
  public String getAdapter() { return adapter; }
  public void setAdapter(String adapter) { this.adapter = adapter; }
  public String getSourceId() { return sourceId; }
  public void setSourceId(String sourceId) { this.sourceId = sourceId; }
  public Path getStatePath() { return statePath; }
  public void setStatePath(Path statePath) { this.statePath = statePath; }
  public Path getSinkPath() { return sinkPath; }
  public void setSinkPath(Path sinkPath) { this.sinkPath = sinkPath; }
  public boolean isResetSource() { return resetSource; }
  public void setResetSource(boolean resetSource) { this.resetSource = resetSource; }
  public int getMutationCount() { return mutationCount; }
  public void setMutationCount(int mutationCount) { this.mutationCount = mutationCount; }
  public int getMutationBatchSize() { return mutationBatchSize; }
  public void setMutationBatchSize(int mutationBatchSize) { this.mutationBatchSize = mutationBatchSize; }
  public Duration getIdleDrainTimeout() { return idleDrainTimeout; }
  public void setIdleDrainTimeout(Duration idleDrainTimeout) { this.idleDrainTimeout = idleDrainTimeout; }
  public Duration getMutationPause() { return mutationPause; }
  public void setMutationPause(Duration mutationPause) { this.mutationPause = mutationPause; }
  public Integer getFailSinkAfterAppendCount() { return failSinkAfterAppendCount; }
  public void setFailSinkAfterAppendCount(Integer failSinkAfterAppendCount) { this.failSinkAfterAppendCount = failSinkAfterAppendCount; }
  public Integer getCrashBeforeRequestAckBatchIndex() { return crashBeforeRequestAckBatchIndex; }
  public void setCrashBeforeRequestAckBatchIndex(Integer crashBeforeRequestAckBatchIndex) { this.crashBeforeRequestAckBatchIndex = crashBeforeRequestAckBatchIndex; }
  public ScenarioRequestMode getRequestMode() { return requestMode; }
  public void setRequestMode(ScenarioRequestMode requestMode) { this.requestMode = requestMode; }
  public DbLogFaultProperties getFault() { return fault; }
  public DbLogMysqlProperties getMysql() { return mysql; }
  public DbLogPostgresProperties getPostgres() { return postgres; }
}
