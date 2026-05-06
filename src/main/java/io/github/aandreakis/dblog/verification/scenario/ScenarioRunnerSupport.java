package io.github.aandreakis.dblog.verification.scenario;

import io.github.aandreakis.dblog.adapter.api.OpenedSourceRuntime;
import io.github.aandreakis.dblog.adapter.api.SourceRuntime;
import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.adapter.api.WatermarkWindowRuntime;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.runtime.observer.DbLogRuntimeObservability;
import io.github.aandreakis.dblog.runtime.sql.SourceFlowControlSnapshot;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/** Shared helper surface for scenario verification runners. */
public final class ScenarioRunnerSupport {
  private ScenarioRunnerSupport() {}

  public static ScenarioStore scenarioStore(
      Path sinkPath,
      Duration sinkDelay,
      Integer failSinkAfterAppendCount,
      UnaryOperator<ScenarioStore> scenarioStoreDecorator) {
    ScenarioStore store =
        new JdbcScenarioStore("org.h2.Driver", ScenarioJdbcSupport.sinkJdbcUrl(sinkPath));
    if (!sinkDelay.isZero()) {
      store = new DelayingScenarioStore(store, sinkDelay);
    }
    if (failSinkAfterAppendCount != null) {
      store = new FailingScenarioStore(store, failSinkAfterAppendCount);
    }
    return scenarioStoreDecorator.apply(store);
  }

  public static void recordScenarioSuccess(
      ScenarioStore scenarioStore, String scenarioId, String detailMessage) {
    ScenarioTelemetry.record(
        scenarioStore, scenarioId, "verification", "scenario-success", detailMessage);
  }

  public static void recordScenarioFailure(
      ScenarioStore scenarioStore, String scenarioId, Throwable failure) {
    try {
      ScenarioTelemetry.record(
          scenarioStore,
          scenarioId,
          "verification",
          "scenario-failure",
          failure.getClass().getSimpleName() + ": " + String.valueOf(failure.getMessage()));
    } catch (Throwable telemetryFailure) {
      failure.addSuppressed(telemetryFailure);
    }
  }

  public static void cacheLatestSourceFlowControlSnapshot(
      DbLogRuntimeObservability runtimeObservability,
      Supplier<?> sourceFlowControlSupplier) {
    try {
      Object snapshot = sourceFlowControlSupplier.get();
      if (snapshot instanceof SourceFlowControlSnapshot current) {
        runtimeObservability.sourceFlowControlSnapshotCurrent(current);
      }
    } catch (RuntimeException ignored) {
      // Keep the last successfully cached snapshot if the runtime handle is already closing.
    }
  }

  @SuppressWarnings("unchecked")
  public static <TX extends SourceTransaction<?>> OpenedSourceRuntime<TX> typedOpenedRuntime(
      OpenedSourceRuntime<? extends SourceTransaction<?>> openedRuntime) {
    return (OpenedSourceRuntime<TX>) openedRuntime;
  }

  @SuppressWarnings("unchecked")
  public static <TX extends SourceTransaction<?>> WatermarkWindowRuntime<TX> requireWatermarkWindowRuntime(
      SourceRuntime<?> runtime, String adapterLabel) {
    if (runtime instanceof WatermarkWindowRuntime<?> watermarkRuntime) {
      return (WatermarkWindowRuntime<TX>) watermarkRuntime;
    }
    throw new IllegalArgumentException(
        adapterLabel + " runtime does not support watermark-window coordination");
  }

  public static String loadResumePositionDisplayValue(
      RuntimeStateStore stateStore, String sourceId) {
    Objects.requireNonNull(stateStore, "stateStore");
    Objects.requireNonNull(sourceId, "sourceId");
    Optional<SourcePosition> checkpoint = stateStore.streamPositions().loadCheckpoint(sourceId);
    if (checkpoint.isPresent()) {
      return checkpoint.orElseThrow().displayValue();
    }
    return stateStore.streamPositions().loadBootstrapPosition(sourceId)
        .map(SourcePosition::displayValue)
        .orElse("<none>");
  }
}
