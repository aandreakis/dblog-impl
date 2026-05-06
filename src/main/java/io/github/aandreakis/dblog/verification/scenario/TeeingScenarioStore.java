package io.github.aandreakis.dblog.verification.scenario;

import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.sink.api.ChangeEventSink;
import java.util.List;
import java.util.Objects;

/** Scenario store wrapper that mirrors emitted change events to a secondary sink. */
public final class TeeingScenarioStore implements ScenarioStore {
  private final ScenarioStore delegate;
  private final ChangeEventSink sink;

  public TeeingScenarioStore(ScenarioStore delegate, ChangeEventSink sink) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.sink = Objects.requireNonNull(sink, "sink");
  }

  @Override
  public void appendEvents(String scenarioId, String stageLabel, List<ChangeEvent> events) {
    delegate.appendEvents(scenarioId, stageLabel, events);
    sink.appendEvents(events);
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
    Exception firstFailure = null;
    try {
      sink.close();
    } catch (Exception ex) {
      firstFailure = ex;
    }
    try {
      delegate.close();
    } catch (Exception ex) {
      if (firstFailure == null) {
        firstFailure = ex;
      }
    }
    if (firstFailure != null) {
      throw new IllegalStateException("Failed to close scenario store tee", firstFailure);
    }
  }
}
