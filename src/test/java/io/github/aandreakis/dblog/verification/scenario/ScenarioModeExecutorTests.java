package io.github.aandreakis.dblog.verification.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.controlplane.service.ControlPlaneEventCapture;
import io.github.aandreakis.dblog.runtime.observer.DbLogRuntimeObservability;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ScenarioModeExecutorTests {
  @TempDir Path tempDir;

  @Test
  void executesMysqlScenarioThroughNextOwnedExecutorBoundary() throws Exception {
    AtomicReference<ScenarioModeExecutor.ScenarioExecutionContext> captured = new AtomicReference<>();
    AtomicBoolean executed = new AtomicBoolean(false);
    ScenarioModeExecutor executor =
        new ScenarioModeExecutor(
            context -> {
              captured.set(context);
              return () -> executed.set(true);
            },
            SimpleMeterRegistry::new,
            DbLogRuntimeObservability::new,
            ControlPlaneEventCapture::new);

    ScenarioModeExecutor.ScenarioModeResult result =
        executor.execute(mysqlProperties());

    assertThat(executed).isTrue();
    assertThat(result.scenarioId()).isEqualTo("scenario-1");
    assertThat(result.adapter()).isEqualTo("mysql");
    assertThat(captured.get()).isNotNull();
    assertThat(captured.get().properties().getScenario().getId()).isEqualTo("scenario-1");
    assertThat(captured.get().properties().getScenario().getAdapter()).isEqualTo("mysql");
    assertThat(captured.get().scenarioConfig().scenarioId()).isEqualTo("scenario-1");
  }

  private Map<String, String> mysqlProperties() {
    return Map.ofEntries(
        Map.entry("dblog.boot-mode", "scenario"),
        Map.entry("dblog.scenario.id", "scenario-1"),
        Map.entry("dblog.scenario.adapter", "mysql"),
        Map.entry("dblog.scenario.source-id", "scenario-source"),
        Map.entry("dblog.scenario.state-path", tempDir.resolve("state").toString()),
        Map.entry("dblog.scenario.sink-path", tempDir.resolve("sink").toString()),
        Map.entry("dblog.scenario.mysql.database-name", "appdb"),
        Map.entry("dblog.scenario.mysql.jdbc-url", "jdbc:mysql://localhost:3306/appdb"),
        Map.entry("dblog.scenario.mysql.username", "user"),
        Map.entry("dblog.scenario.mysql.password", "password"),
        Map.entry("dblog.scenario.mysql.hostname", "localhost"),
        Map.entry("dblog.scenario.mysql.port", "3306"),
        Map.entry("dblog.scenario.mysql.server-id", "223355"));
  }
}
