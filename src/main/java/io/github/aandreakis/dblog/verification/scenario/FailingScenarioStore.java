package io.github.aandreakis.dblog.verification.scenario;

import io.github.aandreakis.dblog.core.model.ChangeEvent;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/** Scenario-store decorator that fails after a configured number of append calls. */
public final class FailingScenarioStore implements ScenarioStore {
  private final ScenarioStore delegate;
  private final int failAfterAppendCount;
  private final AtomicInteger appendCalls = new AtomicInteger();

  public FailingScenarioStore(ScenarioStore delegate, int failAfterAppendCount) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    if (failAfterAppendCount <= 0) {
      throw new IllegalArgumentException("failAfterAppendCount must be > 0");
    }
    this.failAfterAppendCount = failAfterAppendCount;
  }

  @Override
  public void appendEvents(String scenarioId, String stageLabel, List<ChangeEvent> events) {
    int call = appendCalls.incrementAndGet();
    if (!events.isEmpty() && call >= failAfterAppendCount) {
      throw new IllegalStateException(
          "Injected sink failure before persisting scenario events at append call " + call);
    }
    delegate.appendEvents(scenarioId, stageLabel, events);
  }

  @Override
  public void recordTelemetry(String scenarioId, String category, String message, String detail) {
    delegate.recordTelemetry(scenarioId, category, message, detail);
  }

  @Override
  public List<ScenarioEventRecord> loadEvents(String scenarioId) {
    return delegate.loadEvents(scenarioId);
  }

  @Override
  public List<ScenarioRowState> loadCurrentRows(String scenarioId) {
    return delegate.loadCurrentRows(scenarioId);
  }

  @Override
  public List<ScenarioTelemetryRecord> loadTelemetry(String scenarioId) {
    return delegate.loadTelemetry(scenarioId);
  }

  @Override
  public void close() {
    delegate.close();
  }
}
