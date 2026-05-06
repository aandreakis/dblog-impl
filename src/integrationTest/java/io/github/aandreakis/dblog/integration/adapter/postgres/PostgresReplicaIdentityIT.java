package io.github.aandreakis.dblog.integration.adapter.postgres;

import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.adapter.postgres.internal.JdbcPostgresReplicaIdentityInspector;
import io.github.aandreakis.dblog.adapter.postgres.internal.PostgresReplicaIdentity;
import io.github.aandreakis.dblog.adapter.postgres.internal.PostgresReplicaIdentityPolicy;
import io.github.aandreakis.dblog.adapter.postgres.internal.PostgresReplicaIdentityState;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.testsupport.LivePostgresTestContainers;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Optional;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Tag("integration-docker")
class PostgresReplicaIdentityIT {
  @Test
  void readsReplicaIdentityAndEnforcesFullForCapturedTables() throws Exception {
    assumeDockerIsAvailable();

    try (PostgreSQLContainer postgres = LivePostgresTestContainers.newBaseContainer()) {
      postgres.start();
      try (Connection connection =
              DriverManager.getConnection(
                  postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
          Statement statement = connection.createStatement()) {
        configureSqlConnection(connection);
        statement.execute("CREATE TABLE public.widgets (id BIGINT PRIMARY KEY, name TEXT)");

        TableId tableId = new TableId(postgres.getDatabaseName(), "public", "widgets");
        JdbcPostgresReplicaIdentityInspector inspector = new JdbcPostgresReplicaIdentityInspector();
        PostgresReplicaIdentityPolicy policy = new PostgresReplicaIdentityPolicy();

        Optional<PostgresReplicaIdentityState> defaultState =
            inspector.readReplicaIdentity(connection, tableId);
        assertThat(defaultState)
            .contains(new PostgresReplicaIdentityState(tableId, PostgresReplicaIdentity.DEFAULT));
        assertThatThrownBy(() -> policy.requireReplicaIdentityFull(defaultState.orElseThrow()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("REPLICA IDENTITY FULL");

        statement.execute("ALTER TABLE public.widgets REPLICA IDENTITY FULL");

        Optional<PostgresReplicaIdentityState> fullState =
            inspector.readReplicaIdentity(connection, tableId);
        assertThat(fullState)
            .contains(new PostgresReplicaIdentityState(tableId, PostgresReplicaIdentity.FULL));
        assertThatCode(() -> policy.requireReplicaIdentityFull(fullState.orElseThrow()))
            .doesNotThrowAnyException();
      }
    }
  }

  @Test
  void failsClosedWhenReplicaIdentityFullIsRemovedAfterStartup() throws Exception {
    assumeDockerIsAvailable();

    try (PostgreSQLContainer postgres = LivePostgresTestContainers.newBaseContainer()) {
      postgres.start();
      try (Connection connection =
              DriverManager.getConnection(
                  postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
          Statement statement = connection.createStatement()) {
        configureSqlConnection(connection);
        statement.execute("CREATE TABLE public.widgets (id BIGINT PRIMARY KEY, name TEXT)");
        statement.execute("ALTER TABLE public.widgets REPLICA IDENTITY FULL");

        TableId tableId = new TableId(postgres.getDatabaseName(), "public", "widgets");
        JdbcPostgresReplicaIdentityInspector inspector = new JdbcPostgresReplicaIdentityInspector();
        PostgresReplicaIdentityPolicy policy = new PostgresReplicaIdentityPolicy();

        assertThatCode(
                () ->
                    policy.requireReplicaIdentityFull(
                        inspector.readReplicaIdentity(connection, tableId).orElseThrow()))
            .doesNotThrowAnyException();

        statement.execute("ALTER TABLE public.widgets REPLICA IDENTITY DEFAULT");

        assertThatThrownBy(
                () ->
                    policy.requireReplicaIdentityFull(
                        inspector.readReplicaIdentity(connection, tableId).orElseThrow()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("REPLICA IDENTITY FULL");
      }
    }
  }

  private static void configureSqlConnection(Connection connection) throws Exception {
    connection.setAutoCommit(true);
    connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
  }

}
