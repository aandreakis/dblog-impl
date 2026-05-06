package io.github.aandreakis.dblog.integration.adapter.postgres;

import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.adapter.postgres.internal.JdbcPostgresPublicationManager;
import io.github.aandreakis.dblog.adapter.postgres.internal.JdbcPostgresReplicationSlotManager;
import io.github.aandreakis.dblog.adapter.postgres.internal.PostgresPublicationConfig;
import io.github.aandreakis.dblog.adapter.postgres.internal.PostgresPublicationState;
import io.github.aandreakis.dblog.adapter.postgres.internal.PostgresReplicationResourcesPreflight;
import io.github.aandreakis.dblog.adapter.postgres.internal.PostgresReplicationResourcesRequest;
import io.github.aandreakis.dblog.adapter.postgres.internal.PostgresReplicationResourcesResult;
import io.github.aandreakis.dblog.adapter.postgres.internal.PostgresReplicationSlotConfig;
import io.github.aandreakis.dblog.config.PostgresResourceOwnership;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.testsupport.LivePostgresTestContainers;
import io.github.aandreakis.dblog.testsupport.PostgresTestUserGrants;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Tag("integration-docker")
class PostgresReplicationResourcesIT {
  private static final String RUNTIME_USERNAME = PostgresTestUserGrants.RUNTIME_USERNAME;
  private static final String RUNTIME_PASSWORD = PostgresTestUserGrants.RUNTIME_PASSWORD;

  @Test
  void ensuresPublicationAndLogicalSlotAndRepairsManagedPublicationDrift() throws Exception {
    assumeDockerIsAvailable();

    try (PostgreSQLContainer postgres = LivePostgresTestContainers.newDefaultContainer()) {
      postgres.start();
      try (Connection adminConnection = openAdminConnection(postgres);
          Statement statement = adminConnection.createStatement()) {
        configureSqlConnection(adminConnection);
        configureRuntimeRole(statement, postgres.getDatabaseName());
        statement.execute("CREATE TABLE public.widgets (id BIGINT PRIMARY KEY, name TEXT)");
        statement.execute("CREATE TABLE public.gadgets (id BIGINT PRIMARY KEY, name TEXT)");
        statement.execute("ALTER TABLE public.widgets OWNER TO " + RUNTIME_USERNAME);
        statement.execute("ALTER TABLE public.gadgets OWNER TO " + RUNTIME_USERNAME);
      }

      PostgresPublicationConfig publicationConfig =
          new PostgresPublicationConfig(
              postgres.getDatabaseName(),
              "dblog_pub",
              List.of(
                  new TableId(postgres.getDatabaseName(), "public", "widgets"),
                  new TableId(postgres.getDatabaseName(), "public", "gadgets")),
              PostgresResourceOwnership.DBLOG_MANAGED);
      PostgresReplicationSlotConfig slotConfig =
          new PostgresReplicationSlotConfig(
              "dblog_slot",
              postgres.getDatabaseName(),
              "pgoutput",
              false,
              false,
              false,
              PostgresResourceOwnership.DBLOG_MANAGED);

      try (Connection connection = openRuntimeConnection(postgres);
          Statement statement = connection.createStatement()) {
        configureSqlConnection(connection);
        PostgresReplicationResourcesResult result =
            new PostgresReplicationResourcesPreflight()
                .inspectAndEnsure(
                    connection,
                    new PostgresReplicationResourcesRequest(
                        postgres.getDatabaseName(), publicationConfig, slotConfig));

        assertThat(result.publication().publicationName()).isEqualTo("dblog_pub");
        assertThat(result.publication().publishesInsert()).isTrue();
        assertThat(result.publication().publishesUpdate()).isTrue();
        assertThat(result.publication().publishesDelete()).isTrue();
        assertThat(result.publication().publishesTruncate()).isFalse();
        assertThat(result.publication().tableIds())
            .containsExactly(
                new TableId(postgres.getDatabaseName(), "public", "gadgets"),
                new TableId(postgres.getDatabaseName(), "public", "widgets"));
        assertThat(result.slot().slotName()).isEqualTo("dblog_slot");
        assertThat(result.slot().isLogical()).isTrue();
        assertThat(result.slot().pluginName()).isEqualTo("pgoutput");
        assertThat(result.slot().databaseName()).isEqualTo(postgres.getDatabaseName());
        assertThat(result.slot().active()).isFalse();
        assertThat(result.slot().failover()).isFalse();

        statement.execute("DROP PUBLICATION dblog_pub");
        statement.execute(
            "CREATE PUBLICATION dblog_pub FOR TABLE public.widgets WHERE (id > 0), public.gadgets WITH (publish = 'insert, update, delete, truncate')");

        PostgresPublicationState repairedPublication =
            new JdbcPostgresPublicationManager().ensurePublication(connection, publicationConfig);

        assertThat(repairedPublication.publishesInsert()).isTrue();
        assertThat(repairedPublication.publishesUpdate()).isTrue();
        assertThat(repairedPublication.publishesDelete()).isTrue();
        assertThat(repairedPublication.publishesTruncate()).isFalse();
        assertThat(repairedPublication.tables())
            .allMatch(table -> !table.rowFilterPresent() && !table.columnListPresent());
        assertThat(repairedPublication.tableIds())
            .containsExactly(
                new TableId(postgres.getDatabaseName(), "public", "gadgets"),
                new TableId(postgres.getDatabaseName(), "public", "widgets"));
      }
    }
  }

  @Test
  void failsClosedWhenExistingLogicalSlotPluginDoesNotMatchExpectedPlugin() throws Exception {
    assumeDockerIsAvailable();

    try (PostgreSQLContainer postgres = LivePostgresTestContainers.newDefaultContainer()) {
      postgres.start();
      try (Connection connection =
              DriverManager.getConnection(
                  postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
          Statement statement = connection.createStatement()) {
        configureSqlConnection(connection);
        statement.execute("SELECT * FROM pg_create_logical_replication_slot('dblog_slot', 'pgoutput')");

        PostgresReplicationSlotConfig mismatchedConfig =
            new PostgresReplicationSlotConfig(
                "dblog_slot",
                postgres.getDatabaseName(),
                "test_decoding",
                false,
                false,
                false,
                PostgresResourceOwnership.EXTERNALLY_MANAGED);

        assertThatThrownBy(
                () ->
                    new JdbcPostgresReplicationSlotManager()
                        .ensureLogicalSlot(connection, mismatchedConfig))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("plugin does not match expected plugin");
      }
    }
  }

  @Test
  void failsClosedWhenExistingLogicalSlotDatabaseDoesNotMatchExpectedDatabase() throws Exception {
    assumeDockerIsAvailable();

    try (PostgreSQLContainer postgres = LivePostgresTestContainers.newDefaultContainer()) {
      postgres.start();
      try (Connection connection =
              DriverManager.getConnection(
                  postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
          Statement statement = connection.createStatement()) {
        configureSqlConnection(connection);
        statement.execute("SELECT * FROM pg_create_logical_replication_slot('dblog_slot', 'pgoutput')");

        PostgresReplicationSlotConfig mismatchedConfig =
            new PostgresReplicationSlotConfig(
                "dblog_slot",
                postgres.getDatabaseName() + "_wrong",
                "pgoutput",
                false,
                false,
                false,
                PostgresResourceOwnership.EXTERNALLY_MANAGED);

        assertThatThrownBy(
                () ->
                    new JdbcPostgresReplicationSlotManager()
                        .ensureLogicalSlot(connection, mismatchedConfig))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("database does not match expected database");
      }
    }
  }

  @Test
  void failsClosedWhenExternallyManagedPublicationIsMissingRequiredTables() throws Exception {
    assumeDockerIsAvailable();

    try (PostgreSQLContainer postgres = LivePostgresTestContainers.newDefaultContainer()) {
      postgres.start();
      try (Connection connection =
              DriverManager.getConnection(
                  postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
          Statement statement = connection.createStatement()) {
        configureSqlConnection(connection);
        statement.execute("CREATE TABLE public.widgets (id BIGINT PRIMARY KEY, name TEXT)");
        statement.execute("CREATE TABLE public.gadgets (id BIGINT PRIMARY KEY, name TEXT)");
        statement.execute(
            "CREATE PUBLICATION dblog_pub_ext FOR TABLE public.widgets WITH (publish = 'insert, update, delete')");

        PostgresPublicationConfig expectedPublication =
            new PostgresPublicationConfig(
                postgres.getDatabaseName(),
                "dblog_pub_ext",
                List.of(
                    new TableId(postgres.getDatabaseName(), "public", "widgets"),
                    new TableId(postgres.getDatabaseName(), "public", "gadgets")),
                PostgresResourceOwnership.EXTERNALLY_MANAGED);

        assertThatThrownBy(
                () ->
                    new JdbcPostgresPublicationManager()
                        .ensurePublication(connection, expectedPublication))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("publication table set does not match captured tables");
      }
    }
  }

  private static void configureSqlConnection(Connection connection) throws Exception {
    connection.setAutoCommit(true);
    connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
  }

  private static void configureRuntimeRole(Statement statement, String databaseName)
      throws Exception {
    PostgresTestUserGrants.applyDblogRuntimeRole(statement, databaseName);
    statement.execute("CREATE SCHEMA dblog_meta AUTHORIZATION " + RUNTIME_USERNAME);
  }

  private static Connection openAdminConnection(PostgreSQLContainer postgres) throws Exception {
    return DriverManager.getConnection(
        postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
  }

  private static Connection openRuntimeConnection(PostgreSQLContainer postgres) throws Exception {
    return DriverManager.getConnection(
        postgres.getJdbcUrl(), RUNTIME_USERNAME, RUNTIME_PASSWORD);
  }
}
