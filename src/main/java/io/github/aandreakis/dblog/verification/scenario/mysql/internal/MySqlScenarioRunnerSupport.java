package io.github.aandreakis.dblog.verification.scenario.mysql.internal;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.config.ScenarioRequestMode;
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.core.request.DumpRequestCoordinator;
import io.github.aandreakis.dblog.core.request.DumpRequestStatus;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.request.ScheduledRequestBatch;
import io.github.aandreakis.dblog.runtime.loop.RuntimeRequestPump;
import io.github.aandreakis.dblog.runtime.loop.RuntimeStreamingPump;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import io.github.aandreakis.dblog.verification.scenario.ScenarioExecutionException;
import io.github.aandreakis.dblog.verification.scenario.ScenarioJdbcSupport;
import io.github.aandreakis.dblog.verification.scenario.ScenarioStore;
import io.github.aandreakis.dblog.verification.scenario.ScenarioTelemetry;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/** Shared wiring helpers for the MySQL production scenario runners. */
public final class MySqlScenarioRunnerSupport {
  private MySqlScenarioRunnerSupport() {}

  public static Thread startOptionalMutator(
      boolean resetSource,
      Connection mutationConnection,
      ScenarioStore scenarioStore,
      String scenarioId,
      MySqlScenarioSchema scenarioSchema,
      int mutationCount,
      Duration mutationPause,
      int mutationBatchSize,
      AtomicReference<Throwable> mutatorFailure,
      String threadName) {
    if (!resetSource) {
      ScenarioTelemetry.record(
          scenarioStore,
          scenarioId,
          "runner",
          "mutation-thread-skipped",
          "resetSource=false; expecting recovery or post-crash replay run");
      return null;
    }
    return Thread.ofVirtual()
        .name(threadName)
        .start(
            () -> {
              try {
                MySqlScenarioSupport.runDeterministicMutationSequence(
                    mutationConnection,
                    scenarioStore,
                    scenarioId,
                    scenarioSchema,
                    mutationCount,
                    mutationPause,
                    mutationBatchSize);
              } catch (Throwable ex) {
                mutatorFailure.set(ex);
              }
            });
  }

  public static <TX extends SourceTransaction<?>> void runStreamingOnlyMutationPhase(
      RuntimeStreamingPump<TX> streamingPump,
      Connection mutationConnection,
      ScenarioStore scenarioStore,
      MySqlScenarioSchema scenarioSchema,
      boolean resetSource,
      String scenarioId,
      int mutationCount,
      Duration mutationPause,
      int mutationBatchSize,
      Duration idleDrainTimeout,
      String mutatorThreadName)
      throws Exception {
    AtomicReference<Throwable> mutatorFailure = new AtomicReference<>();
    Thread mutator =
        startOptionalMutator(
            resetSource,
            mutationConnection,
            scenarioStore,
            scenarioId,
            scenarioSchema,
            mutationCount,
            mutationPause,
            mutationBatchSize,
            mutatorFailure,
            mutatorThreadName);
    try {
      while (mutator != null && mutator.isAlive()) {
        drainStreaming(streamingPump, "streaming-only", idleDrainTimeout);
        if (mutatorFailure.get() != null) {
          throw new ScenarioExecutionException("Scenario mutator failed", mutatorFailure.get());
        }
      }
      drainStreaming(streamingPump, "streaming-only-final", idleDrainTimeout.multipliedBy(10));
    } finally {
      ScenarioJdbcSupport.joinQuietly(mutator);
    }
    if (mutatorFailure.get() != null) {
      throw new ScenarioExecutionException("Scenario mutator failed", mutatorFailure.get());
    }
    ScenarioTelemetry.record(
        scenarioStore,
        scenarioId,
        "request",
        "streaming-only-finished",
        "mutations=" + mutationCount);
  }

  public static <TX extends SourceTransaction<?>> void runBaselineStreamingPhase(
      RuntimeStreamingPump<TX> streamingPump,
      Connection mutationConnection,
      ScenarioStore scenarioStore,
      String scenarioId,
      MySqlScenarioSchema scenarioSchema,
      Duration idleDrainTimeout)
      throws Exception {
    MySqlScenarioSupport.runBaselineMutations(
        mutationConnection, scenarioStore, scenarioId, scenarioSchema);
    int drained = drainStreaming(streamingPump, "baseline-streaming", idleDrainTimeout);
    ScenarioTelemetry.record(
        scenarioStore,
        scenarioId,
        "baseline",
        "baseline-drain-complete",
        "transactions=" + drained);
  }

  public static <TX extends SourceTransaction<?>> void runRequestPhase(
      DumpRequestCoordinator<TX> coordinator,
      RuntimeStreamingPump<TX> streamingPump,
      RuntimeRequestPump<TX> requestPump,
      Connection mutationConnection,
      RuntimeStateStore stateStore,
      ScenarioStore scenarioStore,
      MySqlScenarioSchema scenarioSchema,
      boolean resetSource,
      String scenarioId,
      ScenarioRequestMode requestMode,
      int mutationCount,
      Duration mutationPause,
      int mutationBatchSize,
      Duration idleDrainTimeout,
      String mutatorThreadName,
      Integer crashBeforeRequestAckBatchIndex,
      String adapterLabel)
      throws Exception {
    if (requestMode == ScenarioRequestMode.STREAMING_ONLY) {
      ScenarioTelemetry.record(
          scenarioStore,
          scenarioId,
          "request",
          "streaming-only-phase",
          "requestMode=STREAMING_ONLY; running mutation-only streaming benchmark path");
      runStreamingOnlyMutationPhase(
          streamingPump,
          mutationConnection,
          scenarioStore,
          scenarioSchema,
          resetSource,
          scenarioId,
          mutationCount,
          mutationPause,
          mutationBatchSize,
          idleDrainTimeout,
          mutatorThreadName);
      return;
    }

    DumpRequest request =
        switch (requestMode) {
          case ALL_TABLES -> stateStore.dumpRequests().createGenerated(DumpScope.ALL_TABLES, null, List.of());
          case TABLE ->
              throw new ScenarioExecutionException(
                  "TABLE request mode is not supported by the migrated MySQL scenario runner");
          case PRIMARY_KEYS ->
              stateStore.dumpRequests().createGenerated(
                  DumpScope.PRIMARY_KEYS,
                  scenarioSchema.widgets().tableId(),
                  scenarioSchema.widgets().primaryKeyTuplesFromLiterals(List.of("2", "2", "1000", "1")));
          case STREAMING_ONLY ->
              throw new ScenarioExecutionException("request mode already handled");
        };
    ScenarioTelemetry.record(
        scenarioStore,
        scenarioId,
        "request",
        "request-enqueued",
        "requestId=" + request.requestId() + " scope=" + request.scope());

    AtomicReference<Throwable> mutatorFailure = new AtomicReference<>();
    Thread mutator =
        startOptionalMutator(
            resetSource,
            mutationConnection,
            scenarioStore,
            scenarioId,
            scenarioSchema,
            mutationCount,
            mutationPause,
            mutationBatchSize,
            mutatorFailure,
            mutatorThreadName);
    java.util.concurrent.atomic.AtomicInteger batchIndex = new java.util.concurrent.atomic.AtomicInteger();
    try {
      requestPump.processPendingRequests(
          coordinator,
          Duration.ofMillis(250),
          batch -> {
            int currentBatchIndex = batchIndex.incrementAndGet();
            maybeCrashBeforeRequestAck(
                scenarioStore,
                scenarioId,
                adapterLabel,
                crashBeforeRequestAckBatchIndex,
                currentBatchIndex,
                batch);
            if (mutatorFailure.get() != null) {
              throw new ScenarioExecutionException(
                  "Scenario mutator failed", mutatorFailure.get());
            }
          });
      while (mutator != null && mutator.isAlive()) {
        drainStreaming(streamingPump, "request-post-completion-drain", idleDrainTimeout);
        if (mutatorFailure.get() != null) {
          throw new ScenarioExecutionException("Scenario mutator failed", mutatorFailure.get());
        }
      }
    } finally {
      ScenarioJdbcSupport.joinQuietly(mutator);
    }
    if (mutatorFailure.get() != null) {
      throw new ScenarioExecutionException("Scenario mutator failed", mutatorFailure.get());
    }
    Optional<DumpRequestStatus> finalStatus = stateStore.dumpRequests().loadStatus(request.requestId());
    ScenarioTelemetry.record(
        scenarioStore,
        scenarioId,
        "request",
        "request-finished",
        finalStatus.map(DumpRequestStatus::toString).orElse("<missing>"));
  }

  private static <TX extends SourceTransaction<?>> void maybeCrashBeforeRequestAck(
      ScenarioStore scenarioStore,
      String scenarioId,
      String adapterLabel,
      Integer crashBatch,
      int batchIndex,
      ScheduledRequestBatch<TX> batch) {
    if (crashBatch == null || crashBatch != batchIndex) {
      return;
    }
    ScenarioTelemetry.injectedFailure(
        scenarioStore,
        scenarioId,
        "failure-injection",
        "crash-before-request-ack",
        "batchIndex="
            + batchIndex
            + " requestId="
            + batch.request().requestId()
            + " table="
            + batch.tableId().displayName());
    throw new ScenarioExecutionException(
        "Injected "
            + adapterLabel
            + " scenario crash before request acknowledgement at batch index "
            + batchIndex);
  }

  private static <TX extends SourceTransaction<?>> int drainStreaming(
      RuntimeStreamingPump<TX> streamingPump, String stageLabel, Duration idleTimeout)
      throws SQLException {
    return streamingPump.drainStreaming(stageLabel, idleTimeout);
  }
}
