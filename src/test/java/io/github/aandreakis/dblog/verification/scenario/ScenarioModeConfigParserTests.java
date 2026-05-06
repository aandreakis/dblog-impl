package io.github.aandreakis.dblog.verification.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.config.ScenarioRequestMode;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ScenarioModeConfigParserTests {
  @Test
  void parsesMysqlScenarioPropertiesIntoNextOwnedConfig() {
    ScenarioModeConfigParser.ScenarioModeConfig parsed =
        ScenarioModeConfigParser.parse(
            Map.ofEntries(
                Map.entry("dblog.scenario.id", "scenario-1"),
                Map.entry("dblog.scenario.adapter", "mysql"),
                Map.entry("dblog.scenario.source-id", "mysql-source"),
                Map.entry("dblog.scenario.state-path", "/tmp/state"),
                Map.entry("dblog.scenario.sink-path", "/tmp/sink"),
                Map.entry("dblog.chunk.size", "25"),
                Map.entry("dblog.scenario.request-mode", "primary_keys"),
                Map.entry("dblog.scenario.mysql.database-name", "appdb"),
                Map.entry("dblog.scenario.mysql.jdbc-url", "jdbc:mysql://localhost:3306/appdb"),
                Map.entry("dblog.scenario.mysql.username", "user"),
                Map.entry("dblog.scenario.mysql.password", "password"),
                Map.entry("dblog.scenario.mysql.hostname", "localhost"),
                Map.entry("dblog.scenario.mysql.port", "3306"),
                Map.entry("dblog.scenario.mysql.server-id", "223355"),
                Map.entry("dblog.scenario.mysql.connect-timeout", "PT0.5S"),
                Map.entry("dblog.scenario.mysql.retry-log-connection-loss", "true"),
                Map.entry("dblog.scenario.mysql.reconnect-backoff", "PT0.2S"),
                Map.entry("dblog.scenario.mysql.source-event-queue-capacity", "2048")));

    assertThat(parsed).isInstanceOf(ScenarioModeConfigParser.MySqlScenarioModeConfig.class);
    ScenarioModeConfigParser.MySqlScenarioModeConfig mysql =
        (ScenarioModeConfigParser.MySqlScenarioModeConfig) parsed;
    assertThat(mysql.common().scenarioId()).isEqualTo("scenario-1");
    assertThat(mysql.common().requestMode()).isEqualTo(ScenarioRequestMode.PRIMARY_KEYS);
    assertThat(mysql.common().chunkSize()).isEqualTo(25);
    assertThat(mysql.jdbcUrl()).contains("jdbc:mysql://");
    assertThat(mysql.sourceEventQueueCapacity()).isEqualTo(2048);
    assertThat(mysql.connectTimeout()).isEqualTo(Duration.ofMillis(500));
    assertThat(mysql.reconnectBackoff()).isEqualTo(Duration.ofMillis(200));
  }

  @Test
  void parsesPostgresScenarioPropertiesIntoNextOwnedConfig() {
    ScenarioModeConfigParser.ScenarioModeConfig parsed =
        ScenarioModeConfigParser.parse(
            Map.of(
                "dblog.scenario.id", "scenario-2",
                "dblog.scenario.adapter", "postgresql",
                "dblog.scenario.state-path", "/tmp/state",
                "dblog.scenario.sink-path", "/tmp/sink",
                "dblog.scenario.postgres.database-name", "appdb",
                "dblog.scenario.postgres.jdbc-url", "jdbc:postgresql://localhost:5432/appdb",
                "dblog.scenario.postgres.replication-jdbc-url", "jdbc:postgresql://localhost:5444/appdb",
                "dblog.scenario.postgres.username", "postgres",
                "dblog.scenario.postgres.password", "password",
                "dblog.scenario.postgres.status-interval", "PT5S"));

    assertThat(parsed).isInstanceOf(ScenarioModeConfigParser.PostgresScenarioModeConfig.class);
    ScenarioModeConfigParser.PostgresScenarioModeConfig postgres =
        (ScenarioModeConfigParser.PostgresScenarioModeConfig) parsed;
    assertThat(postgres.common().sourceId()).isEqualTo("scenario-2-source");
    assertThat(postgres.common().statePath()).isEqualTo(Path.of("/tmp/state"));
    assertThat(postgres.jdbcUrl()).contains("jdbc:postgresql://");
    assertThat(postgres.replicationJdbcUrl()).contains("5444");
    assertThat(postgres.statusInterval()).isEqualTo(Duration.ofSeconds(5));
  }

  @Test
  void failsClosedWhenRequiredScenarioPropertiesAreMissing() {
    assertThatThrownBy(() -> ScenarioModeConfigParser.parse(Map.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("dblog.scenario.adapter");
  }

  @Test
  void acceptsStreamingAliasForScenarioRequestMode() {
    ScenarioModeConfigParser.ScenarioModeConfig parsed =
        ScenarioModeConfigParser.parse(
            Map.ofEntries(
                Map.entry("dblog.scenario.id", "scenario-3"),
                Map.entry("dblog.scenario.adapter", "mysql"),
                Map.entry("dblog.scenario.state-path", "/tmp/state"),
                Map.entry("dblog.scenario.sink-path", "/tmp/sink"),
                Map.entry("dblog.scenario.request-mode", "streaming"),
                Map.entry("dblog.scenario.mysql.jdbc-url", "jdbc:mysql://localhost:3306/appdb"),
                Map.entry("dblog.scenario.mysql.username", "user")));

    assertThat(parsed.common().requestMode()).isEqualTo(ScenarioRequestMode.STREAMING_ONLY);
  }
}
