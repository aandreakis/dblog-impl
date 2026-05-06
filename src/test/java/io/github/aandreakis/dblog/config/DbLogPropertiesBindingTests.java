package io.github.aandreakis.dblog.config;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.config.PostgresResourceOwnership;
import io.github.aandreakis.dblog.config.ScenarioRequestMode;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

class DbLogPropertiesBindingTests {
  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner().withUserConfiguration(PropertiesConfiguration.class);

  @Test
  void bindsTypedRuntimeScenarioAndBenchmarkProperties() {
    contextRunner
        .withPropertyValues(
            propertyValues(
                Map.ofEntries(
                    Map.entry("dblog.boot-mode", "runtime"),
                    Map.entry("dblog.chunk.size", "17"),
                    Map.entry("dblog.checkpoint.max-events", "33"),
                    Map.entry("dblog.checkpoint.max-interval", "7s"),
                    Map.entry("dblog.heartbeat.interval", "9s"),
                    Map.entry("dblog.sink.ndjson.stdout", "true"),
                    Map.entry("dblog.sink.ndjson.path", "/tmp/out.ndjson"),
                    Map.entry("dblog.sink.typed-h2.path", "/tmp/typed-h2"),
                    Map.entry("dblog.sink.noop.enabled", "true"),
                    Map.entry("dblog.target.enabled", "true"),
                    Map.entry("dblog.target.dialect", "postgres"),
                    Map.entry("dblog.target.jdbc-url", "jdbc:postgresql://target:5432/targetdb"),
                    Map.entry("dblog.target.username", "target-user"),
                    Map.entry("dblog.target.password", "target-pass"),
                    Map.entry("dblog.target.connection-timeout", "4s"),
                    Map.entry("dblog.target.retry-backoff", "9s"),
                    Map.entry("dblog.target.maximum-pool-size", "6"),
                    Map.entry("dblog.target.table-mappings[0].source-schema", "public"),
                    Map.entry("dblog.target.table-mappings[0].source-table", "widgets"),
                    Map.entry("dblog.target.table-mappings[0].target-schema", "mirror"),
                    Map.entry("dblog.target.table-mappings[0].target-table", "widgets_copy"),
                    Map.entry("dblog.observability.metrics-enabled", "false"),
                    Map.entry("dblog.control-plane.enabled", "true"),
                    Map.entry("dblog.control-plane.host", "127.0.0.1"),
                    Map.entry("dblog.control-plane.port", "9090"),
                    Map.entry("dblog.control-plane.executor-max-threads", "6"),
                    Map.entry("dblog.control-plane.executor-queue-capacity", "48"),
                    Map.entry("dblog.control-plane.max-request-body-bytes", "32768"),
                    Map.entry("dblog.runtime.state-path", "/tmp/runtime-state"),
                    Map.entry("dblog.source.adapter", "postgres"),
                    Map.entry("dblog.source.id", "source-1"),
                    Map.entry("dblog.source.tables[0]", "public.widgets"),
                    Map.entry("dblog.source.tables[1]", "public.gadgets"),
                    Map.entry("dblog.source.mysql.jdbc-url", "jdbc:mysql://localhost:3306/runtime_db"),
                    Map.entry("dblog.source.mysql.username", "runtime-mysql-user"),
                    Map.entry("dblog.source.mysql.password", "runtime-mysql-pass"),
                    Map.entry("dblog.source.mysql.hostname", "runtime-mysql-host"),
                    Map.entry("dblog.source.mysql.port", "23306"),
                    Map.entry("dblog.source.mysql.server-id", "223355"),
                    Map.entry("dblog.source.mysql.connect-timeout", "8s"),
                    Map.entry("dblog.source.mysql.heartbeat-interval", "6s"),
                    Map.entry("dblog.source.mysql.keep-alive-interval", "300s"),
                    Map.entry("dblog.source.mysql.net-write-timeout", "10m"),
                    Map.entry("dblog.source.mysql.source-event-queue-capacity", "4096"),
                    Map.entry(
                        "dblog.source.postgres.jdbc-url",
                        "jdbc:postgresql://localhost:5432/runtime_db"),
                    Map.entry(
                        "dblog.source.postgres.replication-jdbc-url",
                        "jdbc:postgresql://replica:5433/runtime_db"),
                    Map.entry("dblog.source.postgres.database-name", "runtime_db"),
                    Map.entry("dblog.source.postgres.username", "runtime-postgres-user"),
                    Map.entry("dblog.source.postgres.password", "runtime-postgres-pass"),
                    Map.entry("dblog.source.postgres.publication-name", "runtime_pub"),
                    Map.entry("dblog.source.postgres.publication-ownership", "EXTERNALLY_MANAGED"),
                    Map.entry("dblog.source.postgres.slot-name", "runtime_slot"),
                    Map.entry("dblog.source.postgres.slot-ownership", "DBLOG_MANAGED"),
                    Map.entry("dblog.source.postgres.status-interval", "13s"),
                    Map.entry("dblog.scenario.enabled", "true"),
                    Map.entry("dblog.scenario.id", "scenario-1"),
                    Map.entry("dblog.scenario.adapter", "postgres"),
                    Map.entry("dblog.scenario.source-id", "scenario-1-source"),
                    Map.entry("dblog.scenario.state-path", "/tmp/state"),
                    Map.entry("dblog.scenario.sink-path", "/tmp/sink"),
                    Map.entry("dblog.scenario.reset-source", "false"),
                    Map.entry("dblog.scenario.mutation-count", "12"),
                    Map.entry("dblog.scenario.idle-drain-timeout", "750ms"),
                    Map.entry("dblog.scenario.mutation-pause", "30ms"),
                    Map.entry("dblog.scenario.fail-sink-after-append-count", "7"),
                    Map.entry("dblog.scenario.crash-before-request-ack-batch-index", "2"),
                    Map.entry("dblog.scenario.request-mode", "PRIMARY_KEYS"),
                    Map.entry("dblog.scenario.fault.sink-delay", "25ms"),
                    Map.entry("dblog.scenario.fault.fail-runtime-read-after-count", "3"),
                    Map.entry("dblog.scenario.mysql.jdbc-url", "jdbc:mysql://localhost:3306/appdb"),
                    Map.entry("dblog.scenario.mysql.username", "mysql-user"),
                    Map.entry("dblog.scenario.mysql.password", "mysql-pass"),
                    Map.entry("dblog.scenario.mysql.hostname", "mysql-host"),
                    Map.entry("dblog.scenario.mysql.port", "13306"),
                    Map.entry("dblog.scenario.mysql.server-id", "556677"),
                    Map.entry("dblog.scenario.mysql.connect-timeout", "9s"),
                    Map.entry("dblog.scenario.mysql.heartbeat-interval", "7s"),
                    Map.entry("dblog.scenario.mysql.keep-alive-interval", "300s"),
                    Map.entry("dblog.scenario.mysql.net-write-timeout", "10m"),
                    Map.entry("dblog.scenario.postgres.jdbc-url", "jdbc:postgresql://localhost:5432/appdb"),
                    Map.entry(
                        "dblog.scenario.postgres.replication-jdbc-url",
                        "jdbc:postgresql://replica:5433/appdb"),
                    Map.entry("dblog.scenario.postgres.database-name", "appdb"),
                    Map.entry("dblog.scenario.postgres.username", "postgres-user"),
                    Map.entry("dblog.scenario.postgres.password", "postgres-pass"),
                    Map.entry("dblog.scenario.postgres.status-interval", "11s"))))
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              DbLogProperties properties = context.getBean(DbLogProperties.class);

              assertThat(properties.getBootMode()).isEqualTo(DbLogProperties.BootMode.RUNTIME);
              assertThat(properties.getChunk().getSize()).isEqualTo(17);
              assertThat(properties.getCheckpoint().getMaxEvents()).isEqualTo(33);
              assertThat(properties.getCheckpoint().getMaxInterval()).isEqualTo(Duration.ofSeconds(7));
              assertThat(properties.getHeartbeat().getInterval()).isEqualTo(Duration.ofSeconds(9));
              assertThat(properties.getSink().getNdjson().isStdout()).isTrue();
              assertThat(properties.getSink().getNdjson().getPath())
                  .isEqualTo(Path.of("/tmp/out.ndjson"));
              assertThat(properties.getSink().getTypedH2().getPath())
                  .isEqualTo(Path.of("/tmp/typed-h2"));
              assertThat(properties.getSink().getNoop().isEnabled()).isTrue();
              assertThat(properties.getTarget().isEnabled()).isTrue();
              assertThat(properties.getTarget().getDialect())
                  .isEqualTo(DbLogProperties.TargetDialect.POSTGRES);
              assertThat(properties.getTarget().getJdbcUrl())
                  .isEqualTo("jdbc:postgresql://target:5432/targetdb");
              assertThat(properties.getTarget().getUsername()).isEqualTo("target-user");
              assertThat(properties.getTarget().getPassword()).isEqualTo("target-pass");
              assertThat(properties.getTarget().getConnectionTimeout())
                  .isEqualTo(Duration.ofSeconds(4));
              assertThat(properties.getTarget().getRetryBackoff()).isEqualTo(Duration.ofSeconds(9));
              assertThat(properties.getTarget().getMaximumPoolSize()).isEqualTo(6);
              assertThat(properties.getTarget().getTableMappings()).hasSize(1);
              assertThat(properties.getTarget().getTableMappings().get(0).getSourceSchema())
                  .isEqualTo("public");
              assertThat(properties.getTarget().getTableMappings().get(0).getSourceTable())
                  .isEqualTo("widgets");
              assertThat(properties.getTarget().getTableMappings().get(0).getTargetSchema())
                  .isEqualTo("mirror");
              assertThat(properties.getTarget().getTableMappings().get(0).getTargetTable())
                  .isEqualTo("widgets_copy");
              assertThat(properties.getObservability().isMetricsEnabled()).isFalse();
              assertThat(properties.getControlPlane().isEnabled()).isTrue();
              assertThat(properties.getControlPlane().getHost()).isEqualTo("127.0.0.1");
              assertThat(properties.getControlPlane().getPort()).isEqualTo(9090);
              assertThat(properties.getControlPlane().getExecutorMaxThreads()).isEqualTo(6);
              assertThat(properties.getControlPlane().getExecutorQueueCapacity()).isEqualTo(48);
              assertThat(properties.getControlPlane().getMaxRequestBodyBytes()).isEqualTo(32768);
              assertThat(properties.getRuntime().getStatePath()).isEqualTo(Path.of("/tmp/runtime-state"));
              assertThat(properties.getSource().getAdapter()).isEqualTo("postgres");
              assertThat(properties.getSource().getId()).isEqualTo("source-1");
              assertThat(properties.getSource().getTables())
                  .containsExactly("public.widgets", "public.gadgets");
              assertThat(properties.getSource().getMysql().getJdbcUrl())
                  .isEqualTo("jdbc:mysql://localhost:3306/runtime_db");
              assertThat(properties.getSource().getMysql().getUsername())
                  .isEqualTo("runtime-mysql-user");
              assertThat(properties.getSource().getMysql().getPassword())
                  .isEqualTo("runtime-mysql-pass");
              assertThat(properties.getSource().getMysql().getHostname())
                  .isEqualTo("runtime-mysql-host");
              assertThat(properties.getSource().getMysql().getPort()).isEqualTo(23306);
              assertThat(properties.getSource().getMysql().getServerId()).isEqualTo(223355L);
              assertThat(properties.getSource().getMysql().getConnectTimeout())
                  .isEqualTo(Duration.ofSeconds(8));
              assertThat(properties.getSource().getMysql().getHeartbeatInterval())
                  .isEqualTo(Duration.ofSeconds(6));
              assertThat(properties.getSource().getMysql().getKeepAliveInterval())
                  .isEqualTo(Duration.ofSeconds(300));
              assertThat(properties.getSource().getMysql().getNetWriteTimeout())
                  .isEqualTo(Duration.ofMinutes(10));
              assertThat(properties.getSource().getMysql().getSourceEventQueueCapacity())
                  .isEqualTo(4096);
              assertThat(properties.getSource().getPostgres().getJdbcUrl())
                  .isEqualTo("jdbc:postgresql://localhost:5432/runtime_db");
              assertThat(properties.getSource().getPostgres().getReplicationJdbcUrl())
                  .isEqualTo("jdbc:postgresql://replica:5433/runtime_db");
              assertThat(properties.getSource().getPostgres().getDatabaseName())
                  .isEqualTo("runtime_db");
              assertThat(properties.getSource().getPostgres().getUsername())
                  .isEqualTo("runtime-postgres-user");
              assertThat(properties.getSource().getPostgres().getPassword())
                  .isEqualTo("runtime-postgres-pass");
              assertThat(properties.getSource().getPostgres().getPublicationName())
                  .isEqualTo("runtime_pub");
              assertThat(properties.getSource().getPostgres().getPublicationOwnership())
                  .isEqualTo(PostgresResourceOwnership.EXTERNALLY_MANAGED);
              assertThat(properties.getSource().getPostgres().getSlotName())
                  .isEqualTo("runtime_slot");
              assertThat(properties.getSource().getPostgres().getSlotOwnership())
                  .isEqualTo(PostgresResourceOwnership.DBLOG_MANAGED);
              assertThat(properties.getSource().getPostgres().getStatusInterval())
                  .isEqualTo(Duration.ofSeconds(13));
              assertThat(properties.getScenario().isEnabled()).isTrue();
              assertThat(properties.getScenario().getId()).isEqualTo("scenario-1");
              assertThat(properties.getScenario().getAdapter()).isEqualTo("postgres");
              assertThat(properties.getScenario().getSourceId()).isEqualTo("scenario-1-source");
              assertThat(properties.getScenario().getStatePath()).isEqualTo(Path.of("/tmp/state"));
              assertThat(properties.getScenario().getSinkPath()).isEqualTo(Path.of("/tmp/sink"));
              assertThat(properties.getScenario().isResetSource()).isFalse();
              assertThat(properties.getScenario().getMutationCount()).isEqualTo(12);
              assertThat(properties.getScenario().getIdleDrainTimeout())
                  .isEqualTo(Duration.ofMillis(750));
              assertThat(properties.getScenario().getMutationPause())
                  .isEqualTo(Duration.ofMillis(30));
              assertThat(properties.getScenario().getFailSinkAfterAppendCount()).isEqualTo(7);
              assertThat(properties.getScenario().getCrashBeforeRequestAckBatchIndex())
                  .isEqualTo(2);
              assertThat(properties.getScenario().getRequestMode())
                  .isEqualTo(ScenarioRequestMode.PRIMARY_KEYS);
              assertThat(properties.getScenario().getFault().getSinkDelay())
                  .isEqualTo(Duration.ofMillis(25));
              assertThat(properties.getScenario().getFault().getFailRuntimeReadAfterCount())
                  .isEqualTo(3);
              assertThat(properties.getScenario().getMysql().getJdbcUrl())
                  .isEqualTo("jdbc:mysql://localhost:3306/appdb");
              assertThat(properties.getScenario().getMysql().getUsername()).isEqualTo("mysql-user");
              assertThat(properties.getScenario().getMysql().getPassword()).isEqualTo("mysql-pass");
              assertThat(properties.getScenario().getMysql().getHostname()).isEqualTo("mysql-host");
              assertThat(properties.getScenario().getMysql().getPort()).isEqualTo(13306);
              assertThat(properties.getScenario().getMysql().getServerId()).isEqualTo(556677L);
              assertThat(properties.getScenario().getMysql().getConnectTimeout())
                  .isEqualTo(Duration.ofSeconds(9));
              assertThat(properties.getScenario().getMysql().getHeartbeatInterval())
                  .isEqualTo(Duration.ofSeconds(7));
              assertThat(properties.getScenario().getMysql().getKeepAliveInterval())
                  .isEqualTo(Duration.ofSeconds(300));
              assertThat(properties.getScenario().getMysql().getNetWriteTimeout())
                  .isEqualTo(Duration.ofMinutes(10));
              assertThat(properties.getScenario().getPostgres().getJdbcUrl())
                  .isEqualTo("jdbc:postgresql://localhost:5432/appdb");
              assertThat(properties.getScenario().getPostgres().getReplicationJdbcUrl())
                  .isEqualTo("jdbc:postgresql://replica:5433/appdb");
              assertThat(properties.getScenario().getPostgres().getDatabaseName())
                  .isEqualTo("appdb");
              assertThat(properties.getScenario().getPostgres().getUsername())
                  .isEqualTo("postgres-user");
              assertThat(properties.getScenario().getPostgres().getPassword())
                  .isEqualTo("postgres-pass");
              assertThat(properties.getScenario().getPostgres().getStatusInterval())
                  .isEqualTo(Duration.ofSeconds(11));
            });
  }

  private static String[] propertyValues(Map<String, String> properties) {
    return properties.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .map(entry -> entry.getKey() + "=" + entry.getValue())
        .toArray(String[]::new);
  }

  @Configuration(proxyBeanMethods = false)
  @EnableConfigurationProperties(DbLogProperties.class)
  static class PropertiesConfiguration {}
}
