package io.github.aandreakis.dblog.integration.verification.scenario;

import static io.github.aandreakis.dblog.integration.verification.scenario.ScenarioRunnerTestSupport.assertNoPendingRequests;
import static io.github.aandreakis.dblog.integration.verification.scenario.ScenarioRunnerTestSupport.assertScenarioSucceeded;
import static io.github.aandreakis.dblog.integration.verification.scenario.ScenarioRunnerTestSupport.configurePostgresRuntimeUser;
import static io.github.aandreakis.dblog.integration.verification.scenario.ScenarioRunnerTestSupport.postgresContainer;
import static io.github.aandreakis.dblog.integration.verification.scenario.ScenarioRunnerTestSupport.postgresScenarioConfig;
import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;

import io.github.aandreakis.dblog.verification.scenario.postgres.PostgresScenarioConfig;
import io.github.aandreakis.dblog.verification.scenario.postgres.PostgresScenarioRunner;
import io.github.aandreakis.dblog.verification.scenario.postgres.PostgresScenarioSchema;
import java.nio.file.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Tag("integration-docker")
class PostgresScenarioRunnerSmokeIT {
  @TempDir Path tempDir;

  @Test
  void postgresScenarioRunnerExecutesEndToEndAndPersistsScenarioEvidence() throws Exception {
    assumeDockerIsAvailable();

    try (PostgreSQLContainer postgres = postgresContainer()) {
      postgres.start();
      configurePostgresRuntimeUser(postgres);

      Path statePath = tempDir.resolve("postgres-runner-state");
      Path sinkPath = tempDir.resolve("postgres-runner-sink");
      PostgresScenarioConfig config =
          postgresScenarioConfig(
              "postgres-scenario-smoke",
              statePath,
              sinkPath,
              postgres,
              true);

      new PostgresScenarioRunner(config).run();

      assertScenarioSucceeded(
          sinkPath,
          config.scenarioId(),
          PostgresScenarioSchema.forScenario(config).capturedTableNames(),
          false);
      assertNoPendingRequests(statePath);
    }
  }
}
