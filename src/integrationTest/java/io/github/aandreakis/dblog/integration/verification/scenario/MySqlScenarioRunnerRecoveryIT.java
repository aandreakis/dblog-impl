package io.github.aandreakis.dblog.integration.verification.scenario;

import static io.github.aandreakis.dblog.integration.verification.scenario.ScenarioRunnerTestSupport.assertNoPendingRequests;
import static io.github.aandreakis.dblog.integration.verification.scenario.ScenarioRunnerTestSupport.assertScenarioFailed;
import static io.github.aandreakis.dblog.integration.verification.scenario.ScenarioRunnerTestSupport.assertScenarioSucceeded;
import static io.github.aandreakis.dblog.integration.verification.scenario.ScenarioRunnerTestSupport.configureMySqlReplicationUser;
import static io.github.aandreakis.dblog.integration.verification.scenario.ScenarioRunnerTestSupport.mysqlContainer;
import static io.github.aandreakis.dblog.integration.verification.scenario.ScenarioRunnerTestSupport.mysqlScenarioConfig;
import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.verification.scenario.ScenarioExecutionException;
import io.github.aandreakis.dblog.verification.scenario.mysql.MySqlScenarioConfig;
import io.github.aandreakis.dblog.verification.scenario.mysql.MySqlScenarioRunner;
import io.github.aandreakis.dblog.verification.scenario.mysql.internal.MySqlScenarioSchema;
import java.nio.file.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.mysql.MySQLContainer;

@Tag("integration-docker")
class MySqlScenarioRunnerRecoveryIT {
  @TempDir Path tempDir;

  @Test
  void mysqlScenarioRunnerRecoversPendingRequestOnFollowUpRun() throws Exception {
    assumeDockerIsAvailable();

    try (MySQLContainer mysql = mysqlContainer()) {
      mysql.start();
      configureMySqlReplicationUser(mysql);

      Path statePath = tempDir.resolve("mysql-runner-recovery-state");
      Path sinkPath = tempDir.resolve("mysql-runner-recovery-sink");

      MySqlScenarioConfig failingConfig =
          mysqlScenarioConfig(
              "mysql-scenario-recovery-fail",
              statePath,
              sinkPath,
              mysql,
              true,
              1);

      assertThatThrownBy(() -> new MySqlScenarioRunner(failingConfig).run())
          .isInstanceOf(ScenarioExecutionException.class)
          .hasMessageContaining("request acknowledgement");

      MySqlScenarioConfig recoveryConfig =
          mysqlScenarioConfig(
              "mysql-scenario-recovery-resume",
              statePath,
              sinkPath,
              mysql,
              false,
              null);

      new MySqlScenarioRunner(recoveryConfig).run();

      assertScenarioFailed(sinkPath, failingConfig.scenarioId());
      assertScenarioSucceeded(
          sinkPath,
          recoveryConfig.scenarioId(),
          MySqlScenarioSchema.forScenario(recoveryConfig.sourceId(), recoveryConfig.databaseName())
              .capturedTableNames(),
          true);
      assertNoPendingRequests(statePath);
    }
  }
}
