package io.github.aandreakis.dblog.verification.scenario;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.request.ScheduledRequestBatch;
import io.github.aandreakis.dblog.runtime.observer.RuntimeLoopObserver;
import java.util.Objects;
import java.util.function.Function;

/** Scenario telemetry adapter for the shared runtime loop. */
public final class ScenarioRuntimeLoopObserver<TX extends SourceTransaction<?>>
    implements RuntimeLoopObserver<TX> {
  private final ScenarioStore store;
  private final String scenarioId;
  private final Function<TX, String> checkpointDisplay;

  public ScenarioRuntimeLoopObserver(
      ScenarioStore store, String scenarioId, Function<TX, String> checkpointDisplay) {
    this.store = Objects.requireNonNull(store, "store");
    this.scenarioId = Objects.requireNonNull(scenarioId, "scenarioId");
    this.checkpointDisplay = Objects.requireNonNull(checkpointDisplay, "checkpointDisplay");
  }

  @Override
  public void onTransactionPersisted(String stageLabel, TX transaction) {
    ScenarioTelemetry.record(
        store,
        scenarioId,
        "checkpoint",
        "transaction-drained",
        "stage="
            + stageLabel
            + " tx="
            + transaction.transactionId()
            + " checkpoint="
            + checkpointDisplay.apply(transaction)
            + " eventCount="
            + transaction.events().size());
  }

  @Override
  public void onCheckpointAdvanced(String stageLabel, String reason, TX transaction) {
    ScenarioTelemetry.record(
        store,
        scenarioId,
        "checkpoint",
        "checkpoint-advanced",
        "stage="
            + stageLabel
            + " reason="
            + reason
            + " tx="
            + transaction.transactionId()
            + " checkpoint="
            + checkpointDisplay.apply(transaction));
  }

  @Override
  public void onRequestBatch(ScheduledRequestBatch<TX> batch) {
    ScenarioTelemetry.record(
        store,
        scenarioId,
        "batch",
        "request-batch",
        "requestId="
            + batch.request().requestId()
            + " scope="
            + batch.request().scope()
            + " finalRequestBatch="
            + batch.finalRequestBatch()
            + " table="
            + batch.tableId().displayName()
            + " low="
            + batch.window().low().value()
            + " high="
            + batch.window().high().value());
  }
}
