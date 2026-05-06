package io.github.aandreakis.dblog.verification.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.Duration;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ScenarioRunnerSupportTests {
  @TempDir Path tempDir;

  @Test
  void buildsJdbcScenarioStoreWhenNoDecoratorsAreRequested() {
    try (ScenarioStore store =
        ScenarioRunnerSupport.scenarioStore(
            tempDir.resolve("scenario-support-store"),
            Duration.ZERO,
            null,
            UnaryOperator.identity())) {
      assertThat(store).isInstanceOf(JdbcScenarioStore.class);
    }
  }

  @Test
  void recordsScenarioSuccessAndFailureTelemetry() {
    try (JdbcScenarioStore store =
        new JdbcScenarioStore(
            "org.h2.Driver",
            ScenarioJdbcSupport.sinkJdbcUrl(tempDir.resolve("scenario-support-telemetry")))) {
      ScenarioRunnerSupport.recordScenarioSuccess(store, "scenario-1", "ok");
      ScenarioRunnerSupport.recordScenarioFailure(
          store, "scenario-1", new IllegalStateException("boom"));

      assertThat(store.loadTelemetry("scenario-1"))
          .extracting(ScenarioTelemetryRecord::message)
          .contains("scenario-success", "scenario-failure");
    }
  }
}
