package io.github.aandreakis.dblog.verification.scenario;

import io.github.aandreakis.dblog.core.model.ChangeEvent;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/** Simple scenario-store decorator that delays sink apply to create backlog pressure. */
public final class DelayingScenarioStore implements ScenarioStore {
  private final ScenarioStore delegate;
  private final Duration delayPerAppend;

  public DelayingScenarioStore(ScenarioStore delegate, Duration delayPerAppend) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.delayPerAppend = Objects.requireNonNull(delayPerAppend, "delayPerAppend");
    if (delayPerAppend.isNegative() || delayPerAppend.isZero()) {
      throw new IllegalArgumentException("delayPerAppend must be > 0");
    }
  }

  @Override
  public void appendEvents(String scenarioId, String stageLabel, List<ChangeEvent> events) {
    sleep(delayPerAppend);
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

  private static void sleep(Duration duration) {
    try {
      Thread.sleep(duration.toMillis());
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while delaying scenario sink apply", ex);
    }
  }
}
