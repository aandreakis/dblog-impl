package io.github.aandreakis.dblog.verification.scenario;

import io.github.aandreakis.dblog.core.model.ChangeEvent;
import java.util.List;

/** Persistent sink and telemetry store used by scenario-mode verification shells. */
public interface ScenarioStore extends AutoCloseable {
  void appendEvents(String scenarioId, String stageLabel, List<ChangeEvent> events);

  void recordTelemetry(String scenarioId, String category, String message, String detail);

  List<ScenarioEventRecord> loadEvents(String scenarioId);

  List<ScenarioRowState> loadCurrentRows(String scenarioId);

  List<ScenarioTelemetryRecord> loadTelemetry(String scenarioId);

  @Override
  void close();
}
