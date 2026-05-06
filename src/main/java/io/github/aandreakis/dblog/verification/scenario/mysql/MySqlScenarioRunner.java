package io.github.aandreakis.dblog.verification.scenario.mysql;

import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.SourceConnections;
import io.github.aandreakis.dblog.adapter.api.WatermarkWindowRuntime;
import io.github.aandreakis.dblog.adapter.mysql.MySqlBinlogTransaction;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourceAdapter;
import io.github.aandreakis.dblog.config.ScenarioRequestMode;
import io.github.aandreakis.dblog.controlplane.service.ControlPlaneEventCapture;
import io.github.aandreakis.dblog.core.checkpoint.CheckpointFlushPolicy;
import io.github.aandreakis.dblog.core.reconcile.WindowReconciler;
import io.github.aandreakis.dblog.core.request.DefaultDumpWindowCoordinator;
import io.github.aandreakis.dblog.core.request.DefaultTargetedRepairCoordinator;
import io.github.aandreakis.dblog.core.request.RuntimeStateDumpRequestCoordinator;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.runtime.host.RetryingWatermarkWindowRuntime;
import io.github.aandreakis.dblog.runtime.loop.RuntimeRequestPump;
import io.github.aandreakis.dblog.runtime.loop.RuntimeStreamingPump;
import io.github.aandreakis.dblog.runtime.observer.CompositeRuntimeLoopObserver;
import io.github.aandreakis.dblog.runtime.observer.DbLogRuntimeObservability;
import io.github.aandreakis.dblog.runtime.observer.RuntimeLoopObserver;
import io.github.aandreakis.dblog.runtime.telemetry.RuntimeMeasurementWrappers;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import io.github.aandreakis.dblog.tap.NoopTap;
import io.github.aandreakis.dblog.verification.scenario.AbstractScenarioRunner;
import io.github.aandreakis.dblog.verification.scenario.FaultInjectingSourceChunkReader;
import io.github.aandreakis.dblog.verification.scenario.FaultInjectingSourceRuntime;
import io.github.aandreakis.dblog.verification.scenario.FaultInjectingStateStore;
import io.github.aandreakis.dblog.verification.scenario.ScenarioExecutionException;
import io.github.aandreakis.dblog.verification.scenario.ScenarioJdbcSupport;
import io.github.aandreakis.dblog.verification.scenario.ScenarioLedgerVerifier;
import io.github.aandreakis.dblog.verification.scenario.ScenarioPayloadCodec;
import io.github.aandreakis.dblog.verification.scenario.ScenarioRowState;
import io.github.aandreakis.dblog.verification.scenario.ScenarioRunner;
import io.github.aandreakis.dblog.verification.scenario.ScenarioRunnerSupport;
import io.github.aandreakis.dblog.verification.scenario.ScenarioRuntimeEventSink;
import io.github.aandreakis.dblog.verification.scenario.ScenarioRuntimeLoopObserver;
import io.github.aandreakis.dblog.verification.scenario.ScenarioStore;
import io.github.aandreakis.dblog.verification.scenario.ScenarioTelemetry;
import io.github.aandreakis.dblog.verification.scenario.mysql.internal.MySqlScenarioFaultThreads;
import io.github.aandreakis.dblog.verification.scenario.mysql.internal.MySqlScenarioRunnerSupport;
import io.github.aandreakis.dblog.verification.scenario.mysql.internal.MySqlScenarioSchema;
import io.github.aandreakis.dblog.verification.scenario.mysql.internal.MySqlScenarioSupport;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Production scenario runner for the MySQL adapter. */
public final class MySqlScenarioRunner extends AbstractScenarioRunner<MySqlScenarioConfig> {
  private static final Logger log = LoggerFactory.getLogger(MySqlScenarioRunner.class);

  public MySqlScenarioRunner(MySqlScenarioConfig config) {
    super(config);
  }

  public MySqlScenarioRunner(
      MySqlScenarioConfig config, UnaryOperator<ScenarioStore> scenarioStoreDecorator) {
    super(config, scenarioStoreDecorator);
  }

  public MySqlScenarioRunner(
      MySqlScenarioConfig config,
      UnaryOperator<ScenarioStore> scenarioStoreDecorator,
      CheckpointFlushPolicy checkpointFlushPolicy,
      Duration heartbeatInterval,
      MeterRegistry meterRegistry,
      DbLogRuntimeObservability runtimeObservability) {
    super(
        config,
        scenarioStoreDecorator,
        checkpointFlushPolicy,
        heartbeatInterval,
        meterRegistry,
        runtimeObservability);
  }

  public MySqlScenarioRunner(
      MySqlScenarioConfig config,
      UnaryOperator<ScenarioStore> scenarioStoreDecorator,
      CheckpointFlushPolicy checkpointFlushPolicy,
      Duration heartbeatInterval,
      MeterRegistry meterRegistry,
      DbLogRuntimeObservability runtimeObservability,
      ControlPlaneEventCapture eventCapture) {
    super(
        config,
        scenarioStoreDecorator,
        checkpointFlushPolicy,
        heartbeatInterval,
        meterRegistry,
        runtimeObservability,
        eventCapture);
  }

  @Override
  public void run() throws Exception {
    try (ScenarioStore scenarioStore = scenarioStore()) {
      try (Connection sqlConnection =
              DriverManager.getConnection(config.jdbcUrl(), config.username(), config.password());
          Connection sideConnection =
              DriverManager.getConnection(config.jdbcUrl(), config.username(), config.password());
          Connection verificationConnection =
              DriverManager.getConnection(config.jdbcUrl(), config.username(), config.password())) {
        ScenarioJdbcSupport.configureRuntimeSqlConnection(sqlConnection, config.connectTimeout());
        ScenarioJdbcSupport.configureRuntimeSqlConnection(sideConnection, config.connectTimeout());
        ScenarioJdbcSupport.configureRuntimeSqlConnection(
            verificationConnection, config.connectTimeout());

        ScenarioTelemetry.record(
            scenarioStore,
            config.scenarioId(),
            "runner",
            "scenario-start",
            "adapter=mysql jdbcUrl=" + config.jdbcUrl() + " database=" + config.databaseName());

        MySqlScenarioSchema scenarioSchema =
            MySqlScenarioSchema.forScenario(config.sourceId(), config.databaseName());
        if (config.resetSource()) {
          resetScenarioSource(sqlConnection, scenarioSchema);
          seedInitialRows(sqlConnection, scenarioStore, scenarioSchema);
        } else {
          ScenarioTelemetry.record(
              scenarioStore,
              config.scenarioId(),
              "runner",
              "reusing-existing-source",
              "database=" + scenarioSchema.databaseName());
        }

        H2RuntimeStateStore localStateStore = new H2RuntimeStateStore(config.statePath());
        RuntimeStateStore runtimeStateStore =
            new FaultInjectingStateStore(
                localStateStore, scenarioStore, config.scenarioId(), config.faultPlan());
        runtimeStateStore.ownership().claimSourceOwnership(config.sourceId());
        MySqlSourceAdapter adapter = new MySqlSourceAdapter();
        RelationalSourceConfig sourceConfig = sourceConfig();
        FaultInjectingSourceChunkReader chunkReader =
            new FaultInjectingSourceChunkReader(
                adapter.chunkReader(),
                "mysql",
                scenarioStore,
                config.scenarioId(),
                config.faultPlan());
        Thread sqlConnectionCloser =
            MySqlScenarioFaultThreads.startOptionalSqlConnectionClose(
                "mysql",
                scenarioStore,
                config.scenarioId(),
                sqlConnection,
                config.closeRuntimeSqlConnectionAfter());
        Thread capturedSchemaAlterer =
            MySqlScenarioFaultThreads.startOptionalCapturedSchemaAlteration(
                "mysql",
                scenarioStore,
                config.scenarioId(),
                config.jdbcUrl(),
                config.username(),
                config.password(),
                config.alterCapturedSchemaAfter(),
                scenarioSchema);
        Thread metadataShapeAlterer =
            MySqlScenarioFaultThreads.startOptionalMetadataShapeAlteration(
                "mysql",
                scenarioStore,
                config.scenarioId(),
                config.jdbcUrl(),
                config.username(),
                config.password(),
                config.alterMetadataShapeAfter());
        Thread metadataRowDeleter =
            MySqlScenarioFaultThreads.startOptionalMetadataRowDeletion(
                "mysql",
                scenarioStore,
                config.scenarioId(),
                config.jdbcUrl(),
                config.username(),
                config.password(),
                config.deleteMetadataRowAfter());
        Thread heartbeatNuller = null;
        try {
          var openedRuntime =
              ScenarioRunnerSupport.<MySqlBinlogTransaction>typedOpenedRuntime(
                  RuntimeMeasurementWrappers.instrumentOpenedRuntime(
                      "mysql",
                      meterRegistry,
                      adapter.openRuntime(
                          sourceConfig,
                          runtimeStateStore,
                          scenarioSchema.capturedSchemas(),
                          SourceConnections.ofSql(sqlConnection),
                          chunkReader,
                          NoopTap.INSTANCE)));
          WatermarkWindowRuntime<MySqlBinlogTransaction> runtimeBase =
              ScenarioRunnerSupport.requireWatermarkWindowRuntime(
                  openedRuntime.runtime(), "mysql");
          WatermarkWindowRuntime<MySqlBinlogTransaction> reconnectingRuntime = runtimeBase;
          if (config.retryLogConnectionLoss()) {
            reconnectingRuntime =
                new RetryingWatermarkWindowRuntime<>(
                    "mysql",
                    runtimeObservability,
                    meterRegistry,
                    runtimeBase,
                    config.reconnectBackoff(),
                    () ->
                        reopenOwnedRuntime(
                            adapter,
                            sourceConfig,
                            runtimeStateStore,
                            scenarioSchema.capturedSchemas(),
                            scenarioStore));
          }
          try (WatermarkWindowRuntime<MySqlBinlogTransaction> runtimeDelegate = reconnectingRuntime;
              FaultInjectingSourceRuntime<MySqlBinlogTransaction> runtime =
                  new FaultInjectingSourceRuntime<>(
                      runtimeDelegate,
                      scenarioStore,
                      config.scenarioId(),
                      "mysql",
                      config.faultPlan(),
                      transaction -> transaction.checkpointPosition().displayValue())) {
            runtimeObservability.sourceFlowControlProviderCurrent(runtime::sourceFlowControlSnapshot);
            RuntimeLoopObserver<MySqlBinlogTransaction> runtimeObserver =
                CompositeRuntimeLoopObserver.of(
                    new ScenarioRuntimeLoopObserver<>(
                        scenarioStore,
                        config.scenarioId(),
                        transaction -> transaction.checkpointPosition().displayValue()),
                    runtimeObservability.observerCurrent("mysql", meterRegistry));
            ScenarioRuntimeEventSink scenarioRuntimeEventSink =
                new ScenarioRuntimeEventSink(scenarioStore, config.scenarioId(), eventCapture);
            RuntimeStreamingPump<MySqlBinlogTransaction> streamingPump =
                new RuntimeStreamingPump<>(
                    runtime,
                    scenarioRuntimeEventSink,
                    checkpointFlushPolicy,
                    runtimeObserver,
                    NoopTap.INSTANCE);
            RuntimeRequestPump<MySqlBinlogTransaction> requestPump =
                new RuntimeRequestPump<>(
                    streamingPump,
                    runtimeObserver,
                    Duration.ofMillis(250),
                    new AtomicBoolean(false));
            heartbeatNuller =
                MySqlScenarioFaultThreads.startOptionalNullHeartbeatTimestampUpdate(
                    "mysql",
                    scenarioStore,
                    config.scenarioId(),
                    config.jdbcUrl(),
                    config.username(),
                    config.password(),
                    config.faultPlan().nullHeartbeatTimestampAfter());
            boolean recoveringPendingRequest =
                shouldSkipStartupDrain(runtimeStateStore, scenarioSchema.capturedSchemas());
            ScenarioTelemetry.record(
                scenarioStore,
                config.scenarioId(),
                "runtime",
                "runtime-opened",
                "startPosition="
                    + ScenarioRunnerSupport.loadResumePositionDisplayValue(
                        runtimeStateStore, config.sourceId()));

            if (recoveringPendingRequest) {
              ScenarioTelemetry.record(
                  scenarioStore,
                  config.scenarioId(),
                  "runtime",
                  "startup-drain-skipped",
                  "pending request recovery detected; deferring generic startup drain until after request recovery");
            } else {
              drainStreaming(
                  streamingPump,
                  "startup-drain",
                  config.idleDrainTimeout());
            }
            if (config.resetSource()) {
              MySqlScenarioRunnerSupport.runBaselineStreamingPhase(
                  streamingPump,
                  sideConnection,
                  scenarioStore,
                  config.scenarioId(),
                  scenarioSchema,
                  config.idleDrainTimeout());
            }
            MySqlScenarioRunnerSupport.runRequestPhase(
                RuntimeMeasurementWrappers.instrumentCoordinator(
                    "mysql",
                    meterRegistry,
                    new RuntimeStateDumpRequestCoordinator<>(
                        "mysql",
                        config.sourceId(),
                        runtimeStateStore.dumpRequests(),
                        runtimeStateStore.schemas(),
                        new DefaultDumpWindowCoordinator<>(
                            "mysql",
                            "transaction",
                            runtime,
                            runtimeStateStore.dumpProgress(),
                            runtimeStateStore.schemas(),
                            chunkReader,
                            new WindowReconciler(NoopTap.INSTANCE),
                            Duration.ofSeconds(10),
                            Duration.ofMillis(25),
                            NoopTap.INSTANCE),
                        new DefaultTargetedRepairCoordinator<>(
                            "mysql",
                            "transaction",
                            runtime,
                            chunkReader,
                            new WindowReconciler(NoopTap.INSTANCE),
                            Duration.ofSeconds(10),
                            Duration.ofMillis(25),
                            NoopTap.INSTANCE),
                        scenarioSchema::capturedSchemas,
                        config.chunkSize(),
                        NoopTap.INSTANCE)),
                streamingPump,
                requestPump,
                sideConnection,
                runtimeStateStore,
                scenarioStore,
                scenarioSchema,
                config.resetSource(),
                config.scenarioId(),
                config.requestMode(),
                config.mutationCount(),
                config.mutationPause(),
                config.mutationBatchSize(),
                config.idleDrainTimeout(),
                "dblog-mysql-scenario-mutator",
                config.crashBeforeRequestAckBatchIndex(),
                "MySQL");
            try {
              drainStreaming(
                  streamingPump,
                  "final-drain",
                  config.idleDrainTimeout());
            } finally {
              cacheLatestSourceFlowControlSnapshot(
                  runtime::sourceFlowControlSnapshot);
              runtimeObservability.clearSourceFlowControlProvider();
            }
          }

          verifyFinalStateMatchesSource(verificationConnection, scenarioStore, scenarioSchema);
          verifyMutationCountCoveredByLogEvents(scenarioStore, scenarioSchema);
          ScenarioLedgerVerifier.verifyObservedLedger(
              scenarioStore, config.scenarioId(), scenarioSchema.capturedTableNames());
          logScenarioSummary(scenarioStore);
          recordScenarioSuccess(
              scenarioStore,
              config.scenarioId(),
              "final MySQL state invariants matched for requestMode="
                  + config.requestMode()
                  + " and source mutation count was covered by emitted log events");
        } finally {
          ScenarioJdbcSupport.joinQuietly(sqlConnectionCloser);
          ScenarioJdbcSupport.joinQuietly(capturedSchemaAlterer);
          ScenarioJdbcSupport.joinQuietly(metadataShapeAlterer);
          ScenarioJdbcSupport.joinQuietly(metadataRowDeleter);
          ScenarioJdbcSupport.joinQuietly(heartbeatNuller);
          localStateStore.close();
        }
      } catch (Throwable ex) {
        recordScenarioFailure(scenarioStore, config.scenarioId(), ex);
        throw ex;
      }
    }
  }

  private ScenarioStore scenarioStore() {
    return scenarioStore(config.sinkPath(), config.sinkDelay(), config.failSinkAfterAppendCount());
  }

  private RelationalSourceConfig sourceConfig() {
    return new RelationalSourceConfig(
        config.sourceId(),
        config.jdbcUrl(),
        config.username(),
        config.password(),
        config.databaseName(),
        List.copyOf(scenarioTables()),
        Map.of(
            "mysql.serverId", Long.toString(config.serverId()),
            "mysql.connectTimeout", config.connectTimeout().toString(),
            "mysql.heartbeatInterval", config.heartbeatInterval().toString(),
            "mysql.keepAliveInterval", config.keepAliveInterval().toString(),
            "mysql.netWriteTimeout", config.netWriteTimeout().toString(),
            "mysql.sourceEventQueueCapacity", Integer.toString(config.sourceEventQueueCapacity())),
        false);
  }

  private int drainStreaming(
      RuntimeStreamingPump<MySqlBinlogTransaction> streamingPump,
      String stageLabel,
      Duration idleTimeout)
      throws SQLException {
    return streamingPump.drainStreaming(stageLabel, idleTimeout);
  }

  private void verifyFinalStateMatchesSource(
      Connection verificationConnection,
      ScenarioStore scenarioStore,
      MySqlScenarioSchema scenarioSchema)
      throws SQLException {
    MySqlScenarioSupport.verifyFinalStateMatchesSource(
        verificationConnection,
        scenarioStore,
        config.scenarioId(),
        config.requestMode(),
        scenarioSchema);
  }

  private void verifyMutationCountCoveredByLogEvents(
      ScenarioStore scenarioStore, MySqlScenarioSchema scenarioSchema) {
    MySqlScenarioSupport.verifyMutationCountCoveredByLogEvents(
        scenarioStore, config.scenarioId(), scenarioSchema.capturedTableNames());
  }

  private void resetScenarioSource(Connection connection, MySqlScenarioSchema scenarioSchema) throws SQLException {
    MySqlScenarioSupport.resetScenarioSource(connection, scenarioSchema.databaseName());
  }

  private void seedInitialRows(
      Connection connection, ScenarioStore scenarioStore, MySqlScenarioSchema scenarioSchema) throws SQLException {
    MySqlScenarioSupport.seedInitialRows(
        connection,
        scenarioStore,
        config.scenarioId(),
        scenarioSchema.databaseName(),
        scenarioSchema.widgets(),
        scenarioSchema.gadgets());
  }

  private WatermarkWindowRuntime<MySqlBinlogTransaction> reopenOwnedRuntime(
      MySqlSourceAdapter adapter,
      RelationalSourceConfig sourceConfig,
      RuntimeStateStore stateStore,
      List<TableSchema> contractSchemas,
      ScenarioStore scenarioStore)
      throws SQLException {
    Connection runtimeSqlConnection =
        DriverManager.getConnection(config.jdbcUrl(), config.username(), config.password());
    try {
      ScenarioJdbcSupport.configureRuntimeSqlConnection(
          runtimeSqlConnection, config.connectTimeout());
      FaultInjectingSourceChunkReader chunkReader =
          new FaultInjectingSourceChunkReader(
              adapter.chunkReader(),
              "mysql",
              scenarioStore,
              config.scenarioId(),
              config.faultPlan());
      return ScenarioRunnerSupport.requireWatermarkWindowRuntime(
          ScenarioRunnerSupport.<MySqlBinlogTransaction>typedOpenedRuntime(
                  RuntimeMeasurementWrappers.instrumentOpenedRuntime(
                      "mysql",
                      meterRegistry,
                      adapter.openRuntime(
                          sourceConfig,
                          stateStore,
                          contractSchemas,
                          SourceConnections.ofSql(runtimeSqlConnection),
                          chunkReader,
                          NoopTap.INSTANCE)))
              .runtime(),
          "mysql");
    } catch (SQLException failure) {
      try {
        runtimeSqlConnection.close();
      } catch (SQLException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
      throw failure;
    } catch (Exception failure) {
      try {
        runtimeSqlConnection.close();
      } catch (SQLException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
      throw new SQLException("Failed to reopen MySQL live runtime", failure);
    }
  }

  private boolean shouldSkipStartupDrain(
      RuntimeStateStore stateStore, List<TableSchema> capturedSchemas) {
    return MySqlScenarioSupport.shouldSkipStartupDrain(
        config.resetSource(), stateStore, capturedSchemas);
  }

  private List<String> scenarioTables() {
    return List.of(
        config.databaseName() + ".widgets",
        config.databaseName() + ".gadgets");
  }

  private void logScenarioSummary(ScenarioStore scenarioStore) {
    MySqlScenarioSupport.logScenarioSummary(
        log, "MySQL", scenarioStore, config.scenarioId());
  }
}
