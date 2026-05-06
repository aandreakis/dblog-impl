package io.github.aandreakis.dblog.verification.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.verification.scenario.mysql.MySqlScenarioConfig;
import io.github.aandreakis.dblog.verification.scenario.postgres.PostgresScenarioConfig;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class ScenarioMirrorSourceConfigTests {
  @Test
  void mysqlMirrorSourceConfigCarriesCapturedTablesForTargetMappings() {
    MySqlScenarioConfig config =
        new MySqlScenarioConfig(
            "scenario-1",
            "sourceA",
            "appdb",
            "jdbc:mysql://localhost:3306/appdb",
            "user",
            "password",
            "localhost",
            3306,
            223344L,
            Path.of("/tmp/state"),
            Path.of("/tmp/sink"),
            true,
            100,
            10,
            1,
            Duration.ofMillis(500),
            Duration.ofMillis(20),
            Duration.ofSeconds(5),
            Duration.ZERO,
            Duration.ZERO,
            Duration.ZERO,
            Duration.ZERO,
            Duration.ZERO,
            null,
            null,
            io.github.aandreakis.dblog.config.ScenarioRequestMode.ALL_TABLES,
            ScenarioFaultPlan.none());

    assertThat(config.mirrorSourceConfig().capturedTables())
        .containsExactly("appdb.widgets", "appdb.gadgets");
  }

  @Test
  void postgresMirrorSourceConfigCarriesScenarioSchemaTablesForTargetMappings() {
    PostgresScenarioConfig config =
        new PostgresScenarioConfig(
            "Scenario-2",
            "sourceB",
            "appdb",
            "jdbc:postgresql://localhost:5432/appdb",
            "jdbc:postgresql://localhost:5444/appdb",
            "postgres",
            "password",
            Path.of("/tmp/state"),
            Path.of("/tmp/sink"),
            true,
            100,
            10,
            1,
            Duration.ofSeconds(5),
            false,
            Duration.ofSeconds(3),
            Duration.ofMillis(500),
            Duration.ofMillis(20),
            Duration.ZERO,
            Duration.ZERO,
            Duration.ZERO,
            Duration.ZERO,
            Duration.ZERO,
            Duration.ZERO,
            null,
            null,
            io.github.aandreakis.dblog.config.ScenarioRequestMode.ALL_TABLES,
            ScenarioFaultPlan.none());

    assertThat(config.mirrorSourceConfig().capturedTables())
        .containsExactly("scn_scenario_2.widgets", "scn_scenario_2.gadgets");
  }
}
