package io.github.aandreakis.dblog.config;

import jakarta.validation.constraints.NotNull;
import java.time.Duration;

/** Fault-injection configuration for scenario mode. */
public class DbLogFaultProperties {
  @NotNull private Duration sinkDelay = Duration.ZERO;
  @NotNull private Duration runtimeReadDelay = Duration.ZERO;
  private Integer failRuntimeReadAfterCount;
  @NotNull private Duration runtimeAcknowledgeDelay = Duration.ZERO;
  private Integer failRuntimeAcknowledgeAfterCount;
  @NotNull private Duration chunkReadDelay = Duration.ZERO;
  private Integer failChunkReadAfterCount;
  @NotNull private Duration stateStoreDelay = Duration.ZERO;
  private Integer failStateStoreOperationAfterCount;
  private String failStateStoreOperationName;
  @NotNull private Duration terminateReplicationBackendAfter = Duration.ZERO;
  @NotNull private Duration closeRuntimeSqlConnectionAfter = Duration.ZERO;
  @NotNull private Duration alterCapturedSchemaAfter = Duration.ZERO;
  @NotNull private Duration alterMetadataShapeAfter = Duration.ZERO;
  @NotNull private Duration deleteMetadataRowAfter = Duration.ZERO;
  @NotNull private Duration nullHeartbeatTimestampAfter = Duration.ZERO;

  public Duration getSinkDelay() { return sinkDelay; }
  public void setSinkDelay(Duration sinkDelay) { this.sinkDelay = sinkDelay; }
  public Duration getRuntimeReadDelay() { return runtimeReadDelay; }
  public void setRuntimeReadDelay(Duration runtimeReadDelay) { this.runtimeReadDelay = runtimeReadDelay; }
  public Integer getFailRuntimeReadAfterCount() { return failRuntimeReadAfterCount; }
  public void setFailRuntimeReadAfterCount(Integer failRuntimeReadAfterCount) { this.failRuntimeReadAfterCount = failRuntimeReadAfterCount; }
  public Duration getRuntimeAcknowledgeDelay() { return runtimeAcknowledgeDelay; }
  public void setRuntimeAcknowledgeDelay(Duration runtimeAcknowledgeDelay) { this.runtimeAcknowledgeDelay = runtimeAcknowledgeDelay; }
  public Integer getFailRuntimeAcknowledgeAfterCount() { return failRuntimeAcknowledgeAfterCount; }
  public void setFailRuntimeAcknowledgeAfterCount(Integer failRuntimeAcknowledgeAfterCount) { this.failRuntimeAcknowledgeAfterCount = failRuntimeAcknowledgeAfterCount; }
  public Duration getChunkReadDelay() { return chunkReadDelay; }
  public void setChunkReadDelay(Duration chunkReadDelay) { this.chunkReadDelay = chunkReadDelay; }
  public Integer getFailChunkReadAfterCount() { return failChunkReadAfterCount; }
  public void setFailChunkReadAfterCount(Integer failChunkReadAfterCount) { this.failChunkReadAfterCount = failChunkReadAfterCount; }
  public Duration getStateStoreDelay() { return stateStoreDelay; }
  public void setStateStoreDelay(Duration stateStoreDelay) { this.stateStoreDelay = stateStoreDelay; }
  public Integer getFailStateStoreOperationAfterCount() { return failStateStoreOperationAfterCount; }
  public void setFailStateStoreOperationAfterCount(Integer failStateStoreOperationAfterCount) { this.failStateStoreOperationAfterCount = failStateStoreOperationAfterCount; }
  public String getFailStateStoreOperationName() { return failStateStoreOperationName; }
  public void setFailStateStoreOperationName(String failStateStoreOperationName) { this.failStateStoreOperationName = failStateStoreOperationName; }
  public Duration getTerminateReplicationBackendAfter() { return terminateReplicationBackendAfter; }
  public void setTerminateReplicationBackendAfter(Duration terminateReplicationBackendAfter) { this.terminateReplicationBackendAfter = terminateReplicationBackendAfter; }
  public Duration getCloseRuntimeSqlConnectionAfter() { return closeRuntimeSqlConnectionAfter; }
  public void setCloseRuntimeSqlConnectionAfter(Duration closeRuntimeSqlConnectionAfter) { this.closeRuntimeSqlConnectionAfter = closeRuntimeSqlConnectionAfter; }
  public Duration getAlterCapturedSchemaAfter() { return alterCapturedSchemaAfter; }
  public void setAlterCapturedSchemaAfter(Duration alterCapturedSchemaAfter) { this.alterCapturedSchemaAfter = alterCapturedSchemaAfter; }
  public Duration getAlterMetadataShapeAfter() { return alterMetadataShapeAfter; }
  public void setAlterMetadataShapeAfter(Duration alterMetadataShapeAfter) { this.alterMetadataShapeAfter = alterMetadataShapeAfter; }
  public Duration getDeleteMetadataRowAfter() { return deleteMetadataRowAfter; }
  public void setDeleteMetadataRowAfter(Duration deleteMetadataRowAfter) { this.deleteMetadataRowAfter = deleteMetadataRowAfter; }
  public Duration getNullHeartbeatTimestampAfter() { return nullHeartbeatTimestampAfter; }
  public void setNullHeartbeatTimestampAfter(Duration nullHeartbeatTimestampAfter) { this.nullHeartbeatTimestampAfter = nullHeartbeatTimestampAfter; }
}
