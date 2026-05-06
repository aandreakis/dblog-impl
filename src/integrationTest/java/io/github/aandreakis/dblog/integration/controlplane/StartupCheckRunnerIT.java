package io.github.aandreakis.dblog.integration.controlplane;

import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.DbLogRuntimeException;
import io.github.aandreakis.dblog.adapter.api.HeartbeatMetadataWriter;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.api.WatermarkMetadataWriter;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourceAdapter;
import io.github.aandreakis.dblog.adapter.postgres.PostgresSourceAdapter;
import io.github.aandreakis.dblog.boot.SourceAdapterRegistry;
import io.github.aandreakis.dblog.runtime.bootstrap.StartupCheckRunner;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import io.github.aandreakis.dblog.testsupport.LiveMySqlTestContainers;
import io.github.aandreakis.dblog.testsupport.LivePostgresTestContainers;
import io.github.aandreakis.dblog.testsupport.MySqlTestUserGrants;
import io.github.aandreakis.dblog.testsupport.PostgresTestUserGrants;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Tag("integration-core")
class StartupCheckRunnerIT {
  private static final String POSTGRES_RUNTIME_USERNAME = PostgresTestUserGrants.RUNTIME_USERNAME;
  private static final String POSTGRES_RUNTIME_PASSWORD = PostgresTestUserGrants.RUNTIME_PASSWORD;

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
          new H2RuntimeStateStore(tempDir.resolve("next-startup-check-it-state"))) {
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
  @Tag("integration-docker")
  void passesForAValidPostgresStartupCheckPathWithDedicatedRuntimeRole() throws Exception {
    assumeDockerIsAvailable();

    try (PostgreSQLContainer postgres = LivePostgresTestContainers.newDefaultContainer()) {
      postgres.start();
      configurePostgresRuntimeUser(postgres);
      createPostgresRuntimeOwnedCustomerTable(postgres);

      StartupCheckRunner runner =
          new StartupCheckRunner(new SourceAdapterRegistry(List.of(new PostgresSourceAdapter())));

      try (H2RuntimeStateStore stateStore =
          new H2RuntimeStateStore(tempDir.resolve("postgres-startup-check-it-state"))) {
        StartupCheckRunner.StartupCheckResult result =
            runner.run(
                new StartupCheckRunner.StartupCheckRequest(
                    "postgres",
                    postgresSourceConfig(postgres),
                    stateStore,
                    events -> {}));

        assertThat(result.adapterKey()).isEqualTo("postgres");
        assertThat(result.adapterDisplayName()).isEqualTo("PostgreSQL");
        assertThat(result.sourceId()).isEqualTo("postgresStartupCheckSource");
        assertThat(result.loadedCheckpointDisplayValue()).isNull();
        assertThat(result.liveSchemaCount()).isEqualTo(1);
        assertThat(result.contractSchemaCount()).isEqualTo(1);
      }

      try (Connection connection = openPostgresAdminConnection(postgres)) {
        assertThat(
                countRows(
                    connection,
                    "SELECT COUNT(*) FROM pg_catalog.pg_publication "
                        + "WHERE pubname = 'startup_check_pub'"))
            .isEqualTo(1);
        assertThat(
                countRows(
                    connection,
                    "SELECT COUNT(*) FROM pg_catalog.pg_replication_slots "
                        + "WHERE slot_name = 'startup_check_slot'"))
            .isEqualTo(1);
        assertThat(
                countRows(
                    connection,
                    "SELECT COUNT(*) FROM pg_catalog.pg_publication_tables "
                        + "WHERE pubname = 'startup_check_pub'"))
            .isEqualTo(3);
      }
    }
  }

  @Test
  void failsClosedForInvalidSourceConfiguration() throws Exception {
    StartupCheckRunner runner =
        new StartupCheckRunner(new SourceAdapterRegistry(List.of(new MySqlSourceAdapter())));

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("next-startup-check-it-fail-state"))) {
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
              throw new DbLogRuntimeException(failure);
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

  private static RelationalSourceConfig postgresSourceConfig(PostgreSQLContainer postgres) {
    return new RelationalSourceConfig(
        "postgresStartupCheckSource",
        postgres.getJdbcUrl(),
        POSTGRES_RUNTIME_USERNAME,
        POSTGRES_RUNTIME_PASSWORD,
        postgres.getDatabaseName(),
        List.of("public.customers"),
        Map.of(
            "postgres.publicationName",
            "startup_check_pub",
            "postgres.slotName",
            "startup_check_slot",
            "postgres.statusInterval",
            "PT1S"),
        false);
  }

  private static void configurePostgresRuntimeUser(PostgreSQLContainer postgres)
      throws Exception {
    try (Connection connection = openPostgresAdminConnection(postgres);
        Statement statement = connection.createStatement()) {
      PostgresTestUserGrants.applyDblogRuntimeRole(statement, postgres.getDatabaseName());
    }
  }

  private static void createPostgresRuntimeOwnedCustomerTable(PostgreSQLContainer postgres)
      throws Exception {
    try (Connection connection = openPostgresAdminConnection(postgres);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TABLE public.customers (id BIGINT PRIMARY KEY, name TEXT)");
      statement.execute("ALTER TABLE public.customers OWNER TO " + POSTGRES_RUNTIME_USERNAME);
      statement.execute("ALTER TABLE public.customers REPLICA IDENTITY FULL");
    }
  }

  private static Connection openPostgresAdminConnection(PostgreSQLContainer postgres)
      throws Exception {
    return DriverManager.getConnection(
        postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
  }

  private static int countRows(Connection connection, String sql) throws Exception {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      assertThat(resultSet.next()).isTrue();
      return resultSet.getInt(1);
    }
  }

}
