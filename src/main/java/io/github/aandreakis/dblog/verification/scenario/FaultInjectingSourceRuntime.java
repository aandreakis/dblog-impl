package io.github.aandreakis.dblog.verification.scenario;

import io.github.aandreakis.dblog.adapter.api.ChunkReadWithWindowWork;
import io.github.aandreakis.dblog.adapter.api.ChunkReadWork;
import io.github.aandreakis.dblog.adapter.api.RuntimeStatusInspectable;
import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.adapter.api.WatermarkWindowResult;
import io.github.aandreakis.dblog.adapter.api.WatermarkWindowRuntime;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.runtime.sql.SourceFlowControlSnapshot;
import io.github.aandreakis.dblog.verification.scenario.support.ScenarioFailureSupport;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/** Scenario-only final runtime wrapper for telemetry and injected delay/failure. */
public final class FaultInjectingSourceRuntime<TX extends SourceTransaction<?>>
    implements WatermarkWindowRuntime<TX>, RuntimeStatusInspectable {
  private final WatermarkWindowRuntime<TX> delegate;
  private final RuntimeStatusInspectable statusInspectable;
  private final ScenarioStore scenarioStore;
  private final String scenarioId;
  private final String adapterLabel;
  private final ScenarioFaultPlan faultPlan;
  private final Function<TX, String> checkpointDisplay;
  private final ScenarioFailureSupport readSupport;
  private final ScenarioFailureSupport ackSupport;

  public FaultInjectingSourceRuntime(
      WatermarkWindowRuntime<TX> delegate,
      ScenarioStore scenarioStore,
      String scenarioId,
      String adapterLabel,
      ScenarioFaultPlan faultPlan,
      Function<TX, String> checkpointDisplay) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.statusInspectable =
        delegate instanceof RuntimeStatusInspectable inspectable
            ? inspectable
            : new RuntimeStatusInspectable() {};
    this.scenarioStore = Objects.requireNonNull(scenarioStore, "scenarioStore");
    this.scenarioId = Objects.requireNonNull(scenarioId, "scenarioId");
    this.adapterLabel = Objects.requireNonNull(adapterLabel, "adapterLabel");
    this.faultPlan = Objects.requireNonNull(faultPlan, "faultPlan");
    this.checkpointDisplay = Objects.requireNonNull(checkpointDisplay, "checkpointDisplay");
    this.readSupport = new ScenarioFailureSupport(scenarioStore, scenarioId);
    this.ackSupport = new ScenarioFailureSupport(scenarioStore, scenarioId);
  }

  @Override
  public <T> T executeChunkRead(SourceChunkReader reader, ChunkReadWork<T> work)
      throws SQLException {
    return delegate.executeChunkRead(reader, work);
  }

  @Override
  public <T> WatermarkWindowResult<T> executeChunkReadInWatermarkWindow(
      SourceChunkReader reader, ChunkReadWithWindowWork<T> work) throws SQLException {
    return delegate.executeChunkReadInWatermarkWindow(reader, work);
  }

  @Override
  public Optional<TX> readPendingTransaction() throws SQLException {
    readSupport.maybeDelay("runtime-read", adapterLabel + " read", faultPlan.runtimeReadDelay());
    Optional<TX> transaction = delegate.readPendingTransaction();
    transaction.ifPresent(
        value -> {
          int invocation = readSupport.nextInvocation();
          if (faultPlan.failRuntimeReadAfterCount() != null
              && invocation >= faultPlan.failRuntimeReadAfterCount()) {
            readSupport.fail(
                "failure-injection",
                "Injected " + adapterLabel + " runtime read failure",
                "transactionId=" + value.transactionId() + " invocation=" + invocation);
          }
          ScenarioTelemetry.runtimeRead(
              scenarioStore,
              scenarioId,
              adapterLabel,
              value.transactionId(),
              checkpointDisplay.apply(value),
              value.events().size());
        });
    return transaction;
  }

  @Override
  public void acknowledge(TX transaction) throws SQLException {
    ackSupport.maybeDelay(
        "checkpoint", adapterLabel + " acknowledge", faultPlan.runtimeAcknowledgeDelay());
    int invocation = ackSupport.nextInvocation();
    if (faultPlan.failRuntimeAcknowledgeAfterCount() != null
        && invocation >= faultPlan.failRuntimeAcknowledgeAfterCount()) {
      ackSupport.fail(
          "failure-injection",
          "Injected " + adapterLabel + " acknowledge failure",
          "transactionId=" + transaction.transactionId() + " invocation=" + invocation);
    }
    delegate.acknowledge(transaction);
    ScenarioTelemetry.runtimeAcknowledge(
        scenarioStore,
        scenarioId,
        adapterLabel,
        transaction.transactionId(),
        checkpointDisplay.apply(transaction));
  }

  @Override
  public boolean emitHeartbeatIfDue(Instant heartbeatTime, Duration minimumInterval)
      throws SQLException {
    return delegate.emitHeartbeatIfDue(heartbeatTime, minimumInterval);
  }

  @Override
  public List<TableSchema> currentCapturedSchemas() {
    return delegate.currentCapturedSchemas();
  }

  @Override
  public void updateCapturedSchema(TableSchema schema) {
    delegate.updateCapturedSchema(schema);
  }

  @Override
  public String currentRunId() {
    return delegate.currentRunId();
  }

  @Override
  public String lastAcknowledgedCheckpointDisplayValue() {
    return statusInspectable.lastAcknowledgedCheckpointDisplayValue();
  }

  @Override
  public int pendingTransactionCount() {
    return statusInspectable.pendingTransactionCount();
  }

  @Override
  public int capturedTableCount() {
    return statusInspectable.capturedTableCount();
  }

  @Override
  public SourceFlowControlSnapshot sourceFlowControlSnapshot() {
    return statusInspectable.sourceFlowControlSnapshot();
  }

  @Override
  public void close() throws Exception {
    delegate.close();
  }
}
