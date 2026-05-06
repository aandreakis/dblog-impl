package io.github.aandreakis.dblog.verification.scenario.postgres;

import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.SourceConnections;
import io.github.aandreakis.dblog.adapter.api.WatermarkWindowRuntime;
import io.github.aandreakis.dblog.adapter.postgres.PostgresLsn;
import io.github.aandreakis.dblog.adapter.postgres.PostgresPgoutputTransaction;
import io.github.aandreakis.dblog.adapter.postgres.PostgresSourceAdapter;
import io.github.aandreakis.dblog.adapter.postgres.internal.JdbcPostgresReplicationConnectionFactory;
import io.github.aandreakis.dblog.config.ScenarioRequestMode;
import io.github.aandreakis.dblog.controlplane.service.ControlPlaneEventCapture;
import io.github.aandreakis.dblog.core.checkpoint.CheckpointFlushPolicy;
import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.reconcile.WindowReconciler;
import io.github.aandreakis.dblog.core.request.DefaultDumpWindowCoordinator;
import io.github.aandreakis.dblog.core.request.DefaultTargetedRepairCoordinator;
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.core.request.DumpRequestCoordinator;
import io.github.aandreakis.dblog.core.request.DumpRequestState;
import io.github.aandreakis.dblog.core.request.DumpRequestStatus;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.request.RuntimeStateDumpRequestCoordinator;
import io.github.aandreakis.dblog.core.request.ScheduledRequestBatch;
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
import io.github.aandreakis.dblog.verification.scenario.ScenarioLedgerVerifier;
import io.github.aandreakis.dblog.verification.scenario.ScenarioRunner;
import io.github.aandreakis.dblog.verification.scenario.ScenarioRunnerSupport;
import io.github.aandreakis.dblog.verification.scenario.ScenarioRuntimeEventSink;
import io.github.aandreakis.dblog.verification.scenario.ScenarioRuntimeLoopObserver;
import io.github.aandreakis.dblog.verification.scenario.ScenarioStore;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** PostgreSQL production scenario runner on top of the shared runtime-loop boundary. */
public final class PostgresScenarioRunner extends AbstractScenarioRunner<PostgresScenarioConfig> {
  private static final Logger log = LoggerFactory.getLogger(PostgresScenarioRunner.class);

  public PostgresScenarioRunner(PostgresScenarioConfig config) {
    super(config);
  }

  public PostgresScenarioRunner(
      PostgresScenarioConfig config, UnaryOperator<ScenarioStore> scenarioStoreDecorator) {
    super(config, scenarioStoreDecorator);
  }

  public PostgresScenarioRunner(
      PostgresScenarioConfig config,
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

  public PostgresScenarioRunner(
      PostgresScenarioConfig config,
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
        PostgresScenarioSupport.configureRuntimeSqlConnection(sqlConnection);
        PostgresScenarioSupport.configureRuntimeSqlConnection(sideConnection);
        PostgresScenarioSupport.configureRuntimeSqlConnection(verificationConnection);

        scenarioStore.recordTelemetry(
            config.scenarioId(),
            "runner",
            "scenario-start",
            "jdbcUrl=" + config.jdbcUrl() + " database=" + config.databaseName());

        PostgresScenarioSchema scenarioSchema = PostgresScenarioSchema.forScenario(config);
        if (config.resetSource()) {
          PostgresScenarioSupport.cleanupStaleScenarioArtifacts(
              sqlConnection, scenarioStore, config.scenarioId());
          PostgresScenarioSupport.resetScenarioSource(sqlConnection, scenarioSchema);
          PostgresScenarioSupport.seedInitialRows(
              sqlConnection, scenarioStore, config.scenarioId(), scenarioSchema);
        } else {
          scenarioStore.recordTelemetry(
              config.scenarioId(),
              "runner",
              "reusing-existing-source",
              "schema=" + scenarioSchema.schemaName());
        }

        H2RuntimeStateStore localStateStore = new H2RuntimeStateStore(config.statePath());
        RuntimeStateStore runtimeStateStore =
            new FaultInjectingStateStore(
                localStateStore, scenarioStore, config.scenarioId(), config.faultPlan());
        runtimeStateStore.ownership().claimSourceOwnership(config.sourceId());
        PostgresSourceAdapter adapter = new PostgresSourceAdapter();
        RelationalSourceConfig sourceConfig = sourceConfig(scenarioSchema);
        FaultInjectingSourceChunkReader chunkReader =
            new FaultInjectingSourceChunkReader(
                adapter.chunkReader(),
                "postgres",
                scenarioStore,
                config.scenarioId(),
                config.faultPlan());
        Thread replicationKiller =
            PostgresScenarioFaultThreads.startOptionalReplicationTermination(config, scenarioStore);
        Thread sqlConnectionCloser =
            PostgresScenarioFaultThreads.startOptionalSqlConnectionClose(
                config, scenarioStore, sqlConnection);
        Thread capturedSchemaAlterer =
            PostgresScenarioFaultThreads.startOptionalCapturedSchemaAlteration(
                config, scenarioStore, scenarioSchema);
        Thread metadataShapeAlterer =
            PostgresScenarioFaultThreads.startOptionalMetadataShapeAlteration(
                config, scenarioStore);
        Thread metadataRowDeleter =
            PostgresScenarioFaultThreads.startOptionalMetadataRowDeletion(
                config, scenarioStore);
        Connection replicationConnection =
            new JdbcPostgresReplicationConnectionFactory()
                .open(config.jdbcUrl(), config.username(), config.password());
        try {
          SourceConnections runtimeConnections =
              SourceConnections.ofSqlAndReplication(sqlConnection, replicationConnection);
          var openedRuntime =
              ScenarioRunnerSupport.<PostgresPgoutputTransaction>typedOpenedRuntime(
                  RuntimeMeasurementWrappers.instrumentOpenedRuntime(
                      "postgres",
                      meterRegistry,
                      adapter.openRuntime(
                          sourceConfig,
                          runtimeStateStore,
                          scenarioSchema.capturedSchemas(),
                          runtimeConnections,
                          chunkReader,
                          NoopTap.INSTANCE)));
          WatermarkWindowRuntime<PostgresPgoutputTransaction> runtimeBase =
              ScenarioRunnerSupport.requireWatermarkWindowRuntime(
                  openedRuntime.runtime(), "postgres");
          WatermarkWindowRuntime<PostgresPgoutputTransaction> reconnectingRuntime = runtimeBase;
          if (config.retryLogConnectionLoss()) {
            reconnectingRuntime =
                new RetryingWatermarkWindowRuntime<>(
                    "postgres",
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
          try (WatermarkWindowRuntime<PostgresPgoutputTransaction> runtimeDelegate =
                  reconnectingRuntime;
              FaultInjectingSourceRuntime<PostgresPgoutputTransaction> runtime =
                  new FaultInjectingSourceRuntime<>(
                      runtimeDelegate,
                      scenarioStore,
                      config.scenarioId(),
                      "postgres",
                      config.faultPlan(),
                      transaction -> transaction.checkpointLsn().displayValue())) {
            runtimeObservability.sourceFlowControlProviderCurrent(runtime::sourceFlowControlSnapshot);
            RuntimeLoopObserver<PostgresPgoutputTransaction> runtimeObserver =
                CompositeRuntimeLoopObserver.of(
                    new ScenarioRuntimeLoopObserver<>(
                        scenarioStore,
                        config.scenarioId(),
                        transaction -> transaction.checkpointLsn().displayValue()),
                    runtimeObservability.observerCurrent("postgres", meterRegistry));
            ScenarioRuntimeEventSink scenarioRuntimeEventSink =
                new ScenarioRuntimeEventSink(scenarioStore, config.scenarioId(), eventCapture);
            RuntimeStreamingPump<PostgresPgoutputTransaction> streamingPump =
                new RuntimeStreamingPump<>(
                    runtime,
                    scenarioRuntimeEventSink,
                    checkpointFlushPolicy,
                    runtimeObserver,
                    NoopTap.INSTANCE);
            RuntimeRequestPump<PostgresPgoutputTransaction> requestPump =
                new RuntimeRequestPump<>(
                    streamingPump,
                    runtimeObserver,
                    Duration.ofMillis(250),
                    new AtomicBoolean(false));
            boolean recoveringPendingRequest =
                shouldSkipStartupDrain(runtimeStateStore, scenarioSchema.capturedSchemas());
            scenarioStore.recordTelemetry(
                config.scenarioId(),
                "runtime",
                "runtime-opened",
                "startLsn="
                    + ScenarioRunnerSupport.loadResumePositionDisplayValue(
                        runtimeStateStore, config.sourceId()));

            if (recoveringPendingRequest) {
              scenarioStore.recordTelemetry(
                  config.scenarioId(),
                  "runtime",
                  "startup-drain-skipped",
                  "pending request recovery detected; deferring generic startup drain until after request recovery");
            } else {
              drainStreaming(streamingPump, "startup-drain", config.idleDrainTimeout());
            }

            if (config.resetSource()) {
              runBaselineStreamingPhase(streamingPump, sideConnection, scenarioStore, scenarioSchema);
            } else {
              scenarioStore.recordTelemetry(
                  config.scenarioId(),
                  "runner",
                  "baseline-phase-skipped",
                  "resetSource=false; skipping fresh baseline mutations");
            }
            try {
              runRequestPhase(
                  RuntimeMeasurementWrappers.instrumentCoordinator(
                      "postgres",
                      meterRegistry,
                      new RuntimeStateDumpRequestCoordinator<>(
                          "postgres",
                          config.sourceId(),
                          runtimeStateStore.dumpRequests(),
                          runtimeStateStore.schemas(),
                          new DefaultDumpWindowCoordinator<>(
                              "postgres",
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
                              "postgres",
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
                  scenarioSchema);
              drainStreaming(streamingPump, "final-drain", config.idleDrainTimeout());
            } finally {
              cacheLatestSourceFlowControlSnapshot(runtime::sourceFlowControlSnapshot);
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
              "final source state matched sink state and source mutation count was covered by emitted log events");
        } finally {
          PostgresScenarioFaultThreads.joinQuietly(replicationKiller);
          PostgresScenarioFaultThreads.joinQuietly(sqlConnectionCloser);
          PostgresScenarioFaultThreads.joinQuietly(capturedSchemaAlterer);
          PostgresScenarioFaultThreads.joinQuietly(metadataShapeAlterer);
          PostgresScenarioFaultThreads.joinQuietly(metadataRowDeleter);
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

  private RelationalSourceConfig sourceConfig(PostgresScenarioSchema scenarioSchema) {
    return new RelationalSourceConfig(
        config.sourceId(),
        config.jdbcUrl(),
        config.username(),
        config.password(),
        config.databaseName(),
        List.of(
            scenarioSchema.schemaName() + ".widgets",
            scenarioSchema.schemaName() + ".gadgets"),
        Map.of(
            "postgres.replicationJdbcUrl", config.replicationJdbcUrl(),
            "postgres.publicationName", scenarioSchema.publicationName(),
            "postgres.slotName", scenarioSchema.slotName(),
            "postgres.statusInterval", config.statusInterval().toString()),
        false);
  }

  private void runBaselineStreamingPhase(
      RuntimeStreamingPump<PostgresPgoutputTransaction> streamingPump,
      Connection mutationConnection,
      ScenarioStore scenarioStore,
      PostgresScenarioSchema scenarioSchema)
      throws Exception {
    PostgresScenarioSupport.runBaselineMutations(
        mutationConnection, scenarioStore, config.scenarioId(), scenarioSchema);
    int drained = drainStreaming(streamingPump, "baseline-streaming", config.idleDrainTimeout());
    scenarioStore.recordTelemetry(
        config.scenarioId(), "baseline", "baseline-drain-complete", "transactions=" + drained);
  }

  private void runRequestPhase(
      DumpRequestCoordinator<PostgresPgoutputTransaction> coordinator,
      RuntimeStreamingPump<PostgresPgoutputTransaction> streamingPump,
      RuntimeRequestPump<PostgresPgoutputTransaction> requestPump,
      Connection mutationConnection,
      RuntimeStateStore stateStore,
      ScenarioStore scenarioStore,
      PostgresScenarioSchema scenarioSchema)
      throws Exception {
    if (config.requestMode() == ScenarioRequestMode.STREAMING_ONLY) {
      scenarioStore.recordTelemetry(
          config.scenarioId(),
          "request",
          "streaming-only-phase",
          "requestMode=STREAMING_ONLY; running mutation-only streaming benchmark path");
      runStreamingOnlyMutationPhase(
          streamingPump, mutationConnection, scenarioStore, scenarioSchema);
      return;
    }

    DumpRequest request =
        switch (config.requestMode()) {
          case ALL_TABLES ->
              stateStore.dumpRequests().createGenerated(DumpScope.ALL_TABLES, null, List.of());
          case TABLE ->
              throw new ScenarioExecutionException(
                  "TABLE request mode is not supported by the migrated PostgreSQL scenario runner");
          case PRIMARY_KEYS ->
              stateStore.dumpRequests().createGenerated(
                  DumpScope.PRIMARY_KEYS,
                  scenarioSchema.widgets().tableId(),
                  scenarioSchema.widgets().primaryKeyTuplesFromLiterals(List.of("2", "2", "1000", "1")));
          case STREAMING_ONLY -> throw new ScenarioExecutionException("request mode already handled");
        };
    scenarioStore.recordTelemetry(
        config.scenarioId(),
        "request",
        "request-enqueued",
        "requestId=" + request.requestId() + " scope=" + request.scope());

    AtomicReference<Throwable> mutatorFailure = new AtomicReference<>();
    Thread mutator =
        startOptionalMutator(mutationConnection, scenarioStore, scenarioSchema, mutatorFailure);
    AtomicInteger batchIndex = new AtomicInteger();
    try {
      requestPump.processPendingRequests(
          coordinator,
          Duration.ofMillis(250),
          (ScheduledRequestBatch<PostgresPgoutputTransaction> batch) -> {
            int currentBatchIndex = batchIndex.incrementAndGet();
            maybeCrashBeforeRequestAck(scenarioStore, currentBatchIndex, batch);
            if (mutatorFailure.get() != null) {
              throw new ScenarioExecutionException(
                  "Scenario mutator failed", mutatorFailure.get());
            }
          });
      while (mutator != null && mutator.isAlive()) {
        drainStreaming(streamingPump, "request-post-completion-drain", config.idleDrainTimeout());
        if (mutatorFailure.get() != null) {
          throw new ScenarioExecutionException("Scenario mutator failed", mutatorFailure.get());
        }
      }
    } finally {
      PostgresScenarioFaultThreads.joinQuietly(mutator);
    }

    if (mutatorFailure.get() != null) {
      throw new ScenarioExecutionException("Scenario mutator failed", mutatorFailure.get());
    }

    Optional<DumpRequestStatus> finalStatus = stateStore.dumpRequests().loadStatus(request.requestId());
    scenarioStore.recordTelemetry(
        config.scenarioId(),
        "request",
        "request-finished",
        finalStatus.map(DumpRequestStatus::toString).orElse("<missing>"));
  }

  private Thread startOptionalMutator(
      Connection mutationConnection,
      ScenarioStore scenarioStore,
      PostgresScenarioSchema scenarioSchema,
      AtomicReference<Throwable> mutatorFailure) {
    if (!config.resetSource()) {
      scenarioStore.recordTelemetry(
          config.scenarioId(),
          "runner",
          "mutation-thread-skipped",
          "resetSource=false; expecting recovery or post-crash replay run");
      return null;
    }
    return Thread.ofVirtual()
        .name("dblog-postgres-scenario-mutator")
        .start(
            () -> {
              try {
                PostgresScenarioSupport.runDeterministicMutationSequence(
                    mutationConnection,
                    scenarioStore,
                    config.scenarioId(),
                    scenarioSchema,
                    config.mutationCount(),
                    config.mutationPause(),
                    config.mutationBatchSize());
              } catch (Throwable ex) {
                mutatorFailure.set(ex);
              }
            });
  }

  private void runStreamingOnlyMutationPhase(
      RuntimeStreamingPump<PostgresPgoutputTransaction> streamingPump,
      Connection mutationConnection,
      ScenarioStore scenarioStore,
      PostgresScenarioSchema scenarioSchema)
      throws Exception {
    AtomicReference<Throwable> mutatorFailure = new AtomicReference<>();
    Thread mutator =
        startOptionalMutator(mutationConnection, scenarioStore, scenarioSchema, mutatorFailure);
    try {
      while (mutator != null && mutator.isAlive()) {
        drainStreaming(streamingPump, "streaming-only", config.idleDrainTimeout());
        if (mutatorFailure.get() != null) {
          throw new ScenarioExecutionException("Scenario mutator failed", mutatorFailure.get());
        }
      }
      drainStreaming(
          streamingPump, "streaming-only-final", config.idleDrainTimeout().multipliedBy(10));
    } finally {
      PostgresScenarioFaultThreads.joinQuietly(mutator);
    }

    if (mutatorFailure.get() != null) {
      throw new ScenarioExecutionException("Scenario mutator failed", mutatorFailure.get());
    }
    scenarioStore.recordTelemetry(
        config.scenarioId(),
        "request",
        "streaming-only-finished",
        "mutations=" + config.mutationCount());
  }

  private int drainStreaming(
      RuntimeStreamingPump<PostgresPgoutputTransaction> streamingPump,
      String stageLabel,
      Duration idleTimeout)
      throws SQLException {
    return streamingPump.drainStreaming(stageLabel, idleTimeout);
  }

  private WatermarkWindowRuntime<PostgresPgoutputTransaction> reopenOwnedRuntime(
      PostgresSourceAdapter adapter,
      RelationalSourceConfig sourceConfig,
      RuntimeStateStore stateStore,
      List<TableSchema> contractSchemas,
      ScenarioStore scenarioStore)
      throws SQLException {
    Connection runtimeSqlConnection =
        DriverManager.getConnection(config.jdbcUrl(), config.username(), config.password());
    Connection runtimeReplicationConnection;
    try {
      PostgresScenarioSupport.configureRuntimeSqlConnection(runtimeSqlConnection);
      runtimeReplicationConnection =
          new JdbcPostgresReplicationConnectionFactory()
              .open(config.jdbcUrl(), config.username(), config.password());
    } catch (SQLException ex) {
      try {
        runtimeSqlConnection.close();
      } catch (SQLException closeFailure) {
        ex.addSuppressed(closeFailure);
      }
      throw ex;
    }
    SourceConnections runtimeConnections =
        SourceConnections.ofSqlAndReplication(runtimeSqlConnection, runtimeReplicationConnection);
    try {
      FaultInjectingSourceChunkReader chunkReader =
          new FaultInjectingSourceChunkReader(
              adapter.chunkReader(),
              "postgres",
              scenarioStore,
              config.scenarioId(),
              config.faultPlan());
      return ScenarioRunnerSupport.requireWatermarkWindowRuntime(
          ScenarioRunnerSupport.<PostgresPgoutputTransaction>typedOpenedRuntime(
                  RuntimeMeasurementWrappers.instrumentOpenedRuntime(
                      "postgres",
                      meterRegistry,
                      adapter.openRuntime(
                          sourceConfig,
                          stateStore,
                          contractSchemas,
                          runtimeConnections,
                          chunkReader,
                          NoopTap.INSTANCE)))
              .runtime(),
          "postgres");
    } catch (RuntimeException ex) {
      throw new SQLException("Failed to reopen PostgreSQL live runtime", ex);
    }
  }

  private void verifyFinalStateMatchesSource(
      Connection verificationConnection,
      ScenarioStore scenarioStore,
      PostgresScenarioSchema scenarioSchema)
      throws SQLException {
    PostgresScenarioSupport.verifyFinalStateMatchesSource(
        verificationConnection,
        scenarioStore,
        config.scenarioId(),
        config.requestMode(),
        scenarioSchema);
  }

  private void verifyMutationCountCoveredByLogEvents(
      ScenarioStore scenarioStore, PostgresScenarioSchema scenarioSchema) {
    long sourceMutations =
        scenarioStore.loadTelemetry(config.scenarioId()).stream()
            .filter(record -> record.category().equals("source-mutation"))
            .count();
    Set<String> capturedTables = scenarioSchema.capturedTableNames();
    long logEvents =
        scenarioStore.loadEvents(config.scenarioId()).stream()
            .filter(record -> capturedTables.contains(record.tableDisplayName()))
            .filter(record -> record.captureOrigin().equals(CaptureOrigin.LOG.name()))
            .filter(
                record ->
                    !record.operationType().equals(OperationType.WATERMARK.name())
                        && !record.operationType().equals(OperationType.HEARTBEAT.name()))
            .count();
    if (logEvents < sourceMutations) {
      throw new ScenarioExecutionException(
          "Scenario emitted fewer captured LOG events than committed source mutations. mutations="
              + sourceMutations
              + " logEvents="
              + logEvents);
    }
  }

  private boolean shouldSkipStartupDrain(
      RuntimeStateStore stateStore, List<TableSchema> capturedSchemas) {
    if (config.resetSource()) {
      return false;
    }
    List<DumpRequest> pendingRequests = stateStore.dumpRequests().loadPending();
    if (pendingRequests.isEmpty()) {
      return false;
    }
    for (DumpRequest request : pendingRequests) {
      if (hasActiveRecoveryState(stateStore, request, capturedSchemas)) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasActiveRecoveryState(
      RuntimeStateStore stateStore, DumpRequest request, List<TableSchema> capturedSchemas) {
    Optional<DumpRequestStatus> status = stateStore.dumpRequests().loadStatus(request.requestId());
    if (request.scope() == DumpScope.PRIMARY_KEYS) {
      return false;
    }
    for (TableSchema schema : schemasForRequest(request, capturedSchemas)) {
      if (stateStore
          .dumpProgress()
          .load(request.requestId(), schema.tableId().displayName())
          .filter(progress -> progress.hasActiveChunk())
          .isPresent()) {
        return true;
      }
    }
    return false;
  }

  private static List<TableSchema> schemasForRequest(
      DumpRequest request, List<TableSchema> capturedSchemas) {
    if (request.scope() == DumpScope.TABLE) {
      for (TableSchema schema : capturedSchemas) {
        if (schema.tableId().displayName().equals(request.tableId().displayName())) {
          return List.of(schema);
        }
      }
      return List.of();
    }
    return capturedSchemas;
  }

  private void maybeCrashBeforeRequestAck(
      ScenarioStore scenarioStore,
      int batchIndex,
      ScheduledRequestBatch<PostgresPgoutputTransaction> batch) {
    Integer crashBatch = config.crashBeforeRequestAckBatchIndex();
    if (crashBatch == null || crashBatch != batchIndex) {
      return;
    }
    scenarioStore.recordTelemetry(
        config.scenarioId(),
        "failure-injection",
        "crash-before-request-ack",
        "batchIndex="
            + batchIndex
            + " requestId="
            + batch.request().requestId()
            + " table="
            + batch.tableId().displayName());
    throw new ScenarioExecutionException(
        "Injected scenario crash before request acknowledgement at batch index " + batchIndex);
  }

  private void logScenarioSummary(ScenarioStore scenarioStore) {
    PostgresScenarioSupport.logScenarioSummary(log, scenarioStore, config.scenarioId());
  }
}
