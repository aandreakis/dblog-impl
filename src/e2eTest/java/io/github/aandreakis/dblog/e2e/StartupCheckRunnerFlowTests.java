package io.github.aandreakis.dblog.e2e;

import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.adapter.api.HeartbeatMetadataWriter;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.api.WatermarkMetadataWriter;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourceAdapter;
import io.github.aandreakis.dblog.boot.SourceAdapterRegistry;
import io.github.aandreakis.dblog.runtime.bootstrap.StartupCheckRunner;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import io.github.aandreakis.dblog.testsupport.LiveMySqlTestContainers;
import io.github.aandreakis.dblog.testsupport.MySqlTestUserGrants;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.mysql.MySQLContainer;

class StartupCheckRunnerFlowTests {
  @TempDir Path tempDir;

  @Test
  @Tag("integration-docker")
  void passesForAValidMysqlBootstrapPathAndReportsLoadedCheckpoint() throws Exception {
    assumeDockerIsAvailable();

    try (MySQLContainer mysql = LiveMySqlTestContainers.newDefaultContainer()) {
      mysql.start();
      configureReplicationUser(mysql);

      try (Connection connection =
          DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
        try (Statement statement = connection.createStatement()) {
          statement.execute(
              "CREATE TABLE appdb.customers (id BIGINT PRIMARY KEY, name VARCHAR(255))");
        }
      }

      MySqlSourceAdapter adapter = syntheticAdapter(mysql);
      StartupCheckRunner runner =
          new StartupCheckRunner(new SourceAdapterRegistry(List.of(adapter)));

      try (H2RuntimeStateStore stateStore =
          new H2RuntimeStateStore(tempDir.resolve("phase30-startup-check-state"))) {
        stateStore
            .streamPositions()
            .saveCheckpoint(
                "sourceA",
                new io.github.aandreakis.dblog.adapter.mysql.MySqlSourcePosition(
                    "mysql-bin.000001", 42L, null));

        StartupCheckRunner.StartupCheckResult result =
            runner.run(
                new StartupCheckRunner.StartupCheckRequest(
                    "mysql",
                    new RelationalSourceConfig(
                        "sourceA",
                        mysql.getJdbcUrl(),
                        mysql.getUsername(),
                        mysql.getPassword(),
                        "appdb",
                        List.of("appdb.customers"),
                        java.util.Map.of(),
                        false),
                    stateStore,
                    events -> {}));

        assertThat(result.adapterKey()).isEqualTo("mysql");
        assertThat(result.adapterDisplayName()).isEqualTo("MySQL");
        assertThat(result.sourceId()).isEqualTo("sourceA");
        assertThat(result.loadedCheckpointDisplayValue()).isEqualTo("mysql-bin.000001:42");
        assertThat(result.liveSchemaCount()).isEqualTo(1);
        assertThat(result.contractSchemaCount()).isEqualTo(1);
      }
    }
  }

  @Test
  void failsClosedForInvalidSourceConfiguration() throws Exception {
    StartupCheckRunner runner =
        new StartupCheckRunner(new SourceAdapterRegistry(List.of(new MySqlSourceAdapter())));

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("phase30-startup-check-fail-state"))) {
      assertThatThrownBy(
              () ->
                  runner.run(
                      new StartupCheckRunner.StartupCheckRequest(
                          "mysql",
                          new RelationalSourceConfig(
                              "sourceA",
                              "jdbc:postgresql://127.0.0.1:5432/appdb",
                              "dblog",
                              "secret",
                              "appdb",
                              List.of("appdb.customers"),
                              java.util.Map.of(),
                              false),
                          stateStore,
                          events -> {})))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("jdbc:mysql://");
    }
  }

  private static MySqlSourceAdapter syntheticAdapter(MySQLContainer mysql) {
    return new MySqlSourceAdapter(
        new MySqlSourceAdapter.Dependencies() {
          private final io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlChunkReader
              chunkReader = new io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlChunkReader();

          @Override
          public Connection openSqlConnection(RelationalSourceConfig config) {
            try {
              return DriverManager.getConnection(
                  mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
            } catch (java.sql.SQLException failure) {
              throw new io.github.aandreakis.dblog.DbLogRuntimeException(failure);
            }
          }

          @Override
          public List<io.github.aandreakis.dblog.core.schema.TableSchema> inspectSchemas(
              Connection connection, RelationalSourceConfig config) {
            return List.of(
                io.github.aandreakis.dblog.core.schema.TableSchema.create(
                    new io.github.aandreakis.dblog.core.model.TableId(
                        "sourceA", "appdb", "customers"),
                    List.of(
                        new io.github.aandreakis.dblog.core.schema.ColumnDefinition(
                            "id",
                            "bigint",
                            io.github.aandreakis.dblog.core.schema.NeutralColumnType.INTEGER,
                            true,
                            false),
                        new io.github.aandreakis.dblog.core.schema.ColumnDefinition(
                            "name",
                            "varchar(255)",
                            io.github.aandreakis.dblog.core.schema.NeutralColumnType.STRING,
                            false,
                            true)),
                    Instant.parse("2026-04-10T00:00:00Z")));
          }

          @Override
          public SourceChunkReader chunkReader() {
            return chunkReader;
          }

          @Override
          public WatermarkMetadataWriter watermarkWriter() {
            return new WatermarkMetadataWriter() {
              @Override
              public void ensureMetadataTable(Connection connection) {}

              @Override
              public void writeWatermark(
                  Connection sqlConnection,
                  String runId,
                  io.github.aandreakis.dblog.core.model.WatermarkToken token) {}
            };
          }

          @Override
          public HeartbeatMetadataWriter heartbeatWriter() {
            return new HeartbeatMetadataWriter() {
              @Override
              public void ensureHeartbeatTable(Connection connection) {}

              @Override
              public boolean writeHeartbeatIfDue(
                  Connection connection,
                  String runId,
                  String sourceStreamId,
                  Instant heartbeatTime,
                  Duration minimumInterval) {
                return false;
              }
            };
          }
        });
  }

  private static void configureReplicationUser(MySQLContainer mysql) throws Exception {
    MySqlTestUserGrants.applyDblogUserGrants(mysql, "appdb");
  }

}
