package io.github.aandreakis.dblog.integration.verification.scenario;

import static io.github.aandreakis.dblog.integration.verification.scenario.ScenarioRunnerTestSupport.assertNoPendingRequests;
import static io.github.aandreakis.dblog.integration.verification.scenario.ScenarioRunnerTestSupport.assertScenarioSucceeded;
import static io.github.aandreakis.dblog.integration.verification.scenario.ScenarioRunnerTestSupport.configureMySqlReplicationUser;
import static io.github.aandreakis.dblog.integration.verification.scenario.ScenarioRunnerTestSupport.mysqlContainer;
import static io.github.aandreakis.dblog.integration.verification.scenario.ScenarioRunnerTestSupport.mysqlScenarioConfig;
import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;

import io.github.aandreakis.dblog.verification.scenario.mysql.MySqlScenarioConfig;
import io.github.aandreakis.dblog.verification.scenario.mysql.MySqlScenarioRunner;
import io.github.aandreakis.dblog.verification.scenario.mysql.internal.MySqlScenarioSchema;
import java.nio.file.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.mysql.MySQLContainer;

@Tag("integration-docker")
class MySqlScenarioRunnerSmokeIT {
  @TempDir Path tempDir;

  @Test
  void mysqlScenarioRunnerExecutesEndToEndAndPersistsScenarioEvidence() throws Exception {
    assumeDockerIsAvailable();

    try (MySQLContainer mysql = mysqlContainer()) {
      mysql.start();
      configureMySqlReplicationUser(mysql);

      Path statePath = tempDir.resolve("mysql-runner-state");
      Path sinkPath = tempDir.resolve("mysql-runner-sink");
      MySqlScenarioConfig config =
          mysqlScenarioConfig(
              "mysql-scenario-smoke",
              statePath,
              sinkPath,
              mysql,
              true,
              null);

      new MySqlScenarioRunner(config).run();

      assertScenarioSucceeded(
          sinkPath,
          config.scenarioId(),
          MySqlScenarioSchema.forScenario(config.sourceId(), config.databaseName())
              .capturedTableNames(),
          false);
      assertNoPendingRequests(statePath);
    }
  }
}
