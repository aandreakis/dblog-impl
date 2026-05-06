package io.github.aandreakis.dblog.integration.verification.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.config.ScenarioRequestMode;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import io.github.aandreakis.dblog.testsupport.LiveMySqlTestContainers;
import io.github.aandreakis.dblog.testsupport.LivePostgresTestContainers;
import io.github.aandreakis.dblog.testsupport.MySqlTestUserGrants;
import io.github.aandreakis.dblog.testsupport.PostgresTestUserGrants;
import io.github.aandreakis.dblog.verification.scenario.JdbcScenarioStore;
import io.github.aandreakis.dblog.verification.scenario.ScenarioEventRecord;
import io.github.aandreakis.dblog.verification.scenario.ScenarioFaultPlan;
import io.github.aandreakis.dblog.verification.scenario.ScenarioJdbcSupport;
import io.github.aandreakis.dblog.verification.scenario.ScenarioRowState;
import io.github.aandreakis.dblog.verification.scenario.ScenarioTelemetryRecord;
import io.github.aandreakis.dblog.verification.scenario.mysql.MySqlScenarioConfig;
import io.github.aandreakis.dblog.verification.scenario.postgres.PostgresScenarioConfig;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

final class ScenarioRunnerTestSupport {
  private static final String POSTGRES_RUNTIME_USERNAME = PostgresTestUserGrants.RUNTIME_USERNAME;
  private static final String POSTGRES_RUNTIME_PASSWORD = PostgresTestUserGrants.RUNTIME_PASSWORD;

  private ScenarioRunnerTestSupport() {}

  static MySQLContainer mysqlContainer() {
    return LiveMySqlTestContainers.newDefaultContainer();
  }

  static PostgreSQLContainer postgresContainer() {
    return LivePostgresTestContainers.newDefaultContainer();
  }

  static void configureMySqlReplicationUser(MySQLContainer mysql) throws Exception {
    MySqlTestUserGrants.applyDblogUserGrants(mysql, "appdb");
  }

  static void configurePostgresRuntimeUser(PostgreSQLContainer postgres) throws Exception {
    try (Connection connection =
            DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        Statement statement = connection.createStatement()) {
      PostgresTestUserGrants.applyDblogRuntimeRole(statement, postgres.getDatabaseName());
    }
  }

  static MySqlScenarioConfig mysqlScenarioConfig(
      String scenarioId,
      Path statePath,
      Path sinkPath,
      MySQLContainer mysql,
      boolean resetSource,
      Integer crashBeforeRequestAckBatchIndex) {
    return new MySqlScenarioConfig(
        scenarioId,
        "sourceA",
        "appdb",
        mysql.getJdbcUrl(),
        mysql.getUsername(),
        mysql.getPassword(),
        mysql.getHost(),
        mysql.getMappedPort(MySQLContainer.MYSQL_PORT),
        223355L,
        statePath,
        sinkPath,
        resetSource,
        2,
        4,
        // idleDrainTimeout: widened from 250ms to 2s to absorb MySQL binlog tail
        // latency on loaded CI runners. The final-drain phase terminates as soon as
        // the source goes idle, so the extra budget only gets used when the last
        // committed tx hasn't reached the binlog client yet.
        Duration.ofSeconds(2),
        Duration.ofMillis(10),
        Duration.ofSeconds(5),
        Duration.ZERO,
        Duration.ZERO,
        Duration.ZERO,
        Duration.ZERO,
        Duration.ZERO,
        null,
        crashBeforeRequestAckBatchIndex,
        ScenarioRequestMode.ALL_TABLES,
        ScenarioFaultPlan.none());
  }

  static PostgresScenarioConfig postgresScenarioConfig(
      String scenarioId,
      Path statePath,
      Path sinkPath,
      PostgreSQLContainer postgres,
      boolean resetSource) {
    return new PostgresScenarioConfig(
        scenarioId,
        "sourceA",
        postgres.getDatabaseName(),
        postgres.getJdbcUrl(),
        postgres.getJdbcUrl(),
        POSTGRES_RUNTIME_USERNAME,
        POSTGRES_RUNTIME_PASSWORD,
        statePath,
        sinkPath,
        resetSource,
        2,
        4,
        Duration.ofSeconds(1),
        Duration.ofMillis(250),
        Duration.ofMillis(10),
        Duration.ZERO,
        Duration.ZERO,
        Duration.ZERO,
        Duration.ZERO,
        Duration.ZERO,
        Duration.ZERO,
        null,
        null,
        ScenarioRequestMode.ALL_TABLES,
        ScenarioFaultPlan.none());
  }

  static void assertScenarioSucceeded(
      Path sinkPath,
      String scenarioId,
      Set<String> capturedTables,
      boolean expectRecoverySignal) {
    try (JdbcScenarioStore store =
        new JdbcScenarioStore("org.h2.Driver", ScenarioJdbcSupport.sinkJdbcUrl(sinkPath))) {
      List<ScenarioTelemetryRecord> telemetry = store.loadTelemetry(scenarioId);
      List<ScenarioEventRecord> events = store.loadEvents(scenarioId);
      List<ScenarioRowState> currentRows = store.loadCurrentRows(scenarioId);

      assertThat(telemetry).extracting(ScenarioTelemetryRecord::message).contains("scenario-success");
      assertThat(telemetry).extracting(ScenarioTelemetryRecord::message).contains("request-finished");
      assertThat(telemetry).extracting(ScenarioTelemetryRecord::message).doesNotContain("scenario-failure");
      if (expectRecoverySignal) {
        assertThat(telemetry)
            .extracting(ScenarioTelemetryRecord::message)
            .contains("startup-drain-skipped");
      }

      assertThat(events).isNotEmpty();
      assertThat(events).extracting(ScenarioEventRecord::captureOrigin).contains("SELECT", "LOG");
      assertThat(events)
          .extracting(ScenarioEventRecord::tableDisplayName)
          .anyMatch(capturedTables::contains);
      assertThat(currentRows)
          .filteredOn(ScenarioRowState::present)
          .extracting(ScenarioRowState::tableDisplayName)
          .anyMatch(capturedTables::contains);
    }
  }

  static void assertScenarioFailed(Path sinkPath, String scenarioId) {
    try (JdbcScenarioStore store =
        new JdbcScenarioStore("org.h2.Driver", ScenarioJdbcSupport.sinkJdbcUrl(sinkPath))) {
      assertThat(store.loadTelemetry(scenarioId))
          .extracting(ScenarioTelemetryRecord::message)
          .contains("scenario-failure");
    }
  }

  static void assertNoPendingRequests(Path statePath) {
    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(statePath)) {
      assertThat(stateStore.dumpRequests().loadPending()).isEmpty();
      assertThat(stateStore.dumpRequests().countPending()).isZero();
    }
  }
}
