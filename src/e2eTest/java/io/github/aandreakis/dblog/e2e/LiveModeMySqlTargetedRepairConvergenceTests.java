package io.github.aandreakis.dblog.e2e;

import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourceAdapter;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.core.request.DumpRequestState;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.runtime.bootstrap.DbLogApplication;
import io.github.aandreakis.dblog.sink.jdbc.JdbcApplyChangeEventSink;
import io.github.aandreakis.dblog.sink.jdbc.JdbcApplyTargetDialect;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Tag("integration-docker")
class LiveModeMySqlTargetedRepairConvergenceTests {
  @TempDir Path tempDir;

  @BeforeEach
  void resetSharedContainers() throws Exception {
    assumeDockerIsAvailable();
    SharedLiveE2eContainers.reset();
  }

  @Test
  void targetedPrimaryKeyRepairHealsDriftedPostgresTargetRows() throws Exception {
    MySQLContainer source = SharedLiveE2eContainers.mysql();
    PostgreSQLContainer target = SharedLiveE2eContainers.postgres();
    initializeMySqlSource(source);
    initializePostgresTarget(target);

    RelationalSourceConfig config =
        new RelationalSourceConfig(
            "sourceA",
            source.getJdbcUrl(),
            source.getUsername(),
            source.getPassword(),
            "appdb",
            List.of("appdb.customers"),
            java.util.Map.of("mysql.serverId", "223357"),
            false);

    try (H2RuntimeStateStore stateStore =
            new H2RuntimeStateStore(tempDir.resolve("mysql-targeted-repair-state"));
        JdbcApplyChangeEventSink sink =
            JdbcApplyChangeEventSink.forTarget(
                JdbcApplyTargetDialect.POSTGRES,
                target.getJdbcUrl(),
                target.getUsername(),
                target.getPassword(),
                4);
        DbLogApplication<?> app =
            DbLogApplication.open(
                new MySqlSourceAdapter(), config, stateStore, null, sink, 1)) {
      TableId tableId = new TableId("sourceA", "appdb", "customers");

      DumpRequest initialDump = app.submit(DumpScope.TABLE, tableId, List.of());
      app.processPendingRequests(Duration.ofMillis(20));
      LiveModeMySqlAllTablesConvergenceTests.drainUntilIdle(app);

      assertThat(stateStore.dumpRequests().loadStatus(initialDump.requestId()).orElseThrow().state())
          .isEqualTo(DumpRequestState.COMPLETED);
      assertThat(loadMySqlRows(source)).isEqualTo(loadPostgresRows(target));

      applySourceLiveChanges(source);
      LiveModeMySqlAllTablesConvergenceTests.drainUntilIdle(app);
      assertThat(loadMySqlRows(source)).isEqualTo(loadPostgresRows(target));

      driftPostgresTarget(target);
      assertThat(loadPostgresRows(target))
          .containsExactly("1|customer-1|DRIFTED");

      DumpRequest repair =
          app.submitFromLiterals(DumpScope.PRIMARY_KEYS, tableId, List.of("1", "3"));
      app.processPendingRequests(Duration.ofMillis(20));
      LiveModeMySqlAllTablesConvergenceTests.drainUntilIdle(app);

      assertThat(stateStore.dumpRequests().loadStatus(repair.requestId()).orElseThrow().state())
          .isEqualTo(DumpRequestState.COMPLETED);
      assertThat(
              stateStore.dumpRequests().loadStatus(repair.requestId()).orElseThrow()
                  .missingPrimaryKeyLiterals())
          .isEmpty();
      assertThat(loadPostgresRows(target)).isEqualTo(loadMySqlRows(source));

      DumpRequest missingRepair =
          app.submitFromLiterals(DumpScope.PRIMARY_KEYS, tableId, List.of("999"));
      app.processPendingRequests(Duration.ofMillis(20));
      LiveModeMySqlAllTablesConvergenceTests.drainUntilIdle(app);

      assertThat(
              stateStore.dumpRequests().loadStatus(missingRepair.requestId()).orElseThrow()
                  .state())
          .isEqualTo(DumpRequestState.COMPLETED);
      assertThat(
              stateStore.dumpRequests().loadStatus(missingRepair.requestId()).orElseThrow()
                  .missingPrimaryKeyLiterals())
          .containsExactly("999");
      assertThat(loadPostgresRows(target)).isEqualTo(loadMySqlRows(source));
    }
  }

  private static void initializeMySqlSource(MySQLContainer source) throws Exception {
    try (Connection connection =
        DriverManager.getConnection(source.getJdbcUrl(), source.getUsername(), source.getPassword());
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE appdb.customers (id BIGINT PRIMARY KEY, name VARCHAR(255) NOT NULL, status VARCHAR(64) NOT NULL)");
      statement.execute(
          "INSERT INTO appdb.customers (id, name, status) VALUES (1, 'customer-1', 'PENDING')");
      statement.execute(
          "INSERT INTO appdb.customers (id, name, status) VALUES (2, 'customer-2', 'READY')");
    }
  }

  private static void initializePostgresTarget(PostgreSQLContainer target) throws Exception {
    try (Connection connection =
        DriverManager.getConnection(
            target.getJdbcUrl(), target.getUsername(), target.getPassword());
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE SCHEMA IF NOT EXISTS appdb");
      statement.execute(
          "CREATE TABLE appdb.customers (id BIGINT PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL)");
    }
  }

  private static void applySourceLiveChanges(MySQLContainer source) throws Exception {
    try (Connection connection =
        DriverManager.getConnection(source.getJdbcUrl(), source.getUsername(), source.getPassword());
        Statement statement = connection.createStatement()) {
      statement.execute("UPDATE appdb.customers SET status = 'SYNCED' WHERE id = 1");
      statement.execute(
          "INSERT INTO appdb.customers (id, name, status) VALUES (3, 'customer-3', 'NEW')");
      statement.execute("DELETE FROM appdb.customers WHERE id = 2");
    }
  }

  private static void driftPostgresTarget(PostgreSQLContainer target) throws Exception {
    try (Connection connection =
        DriverManager.getConnection(
            target.getJdbcUrl(), target.getUsername(), target.getPassword());
        Statement statement = connection.createStatement()) {
      statement.execute("UPDATE appdb.customers SET status = 'DRIFTED' WHERE id = 1");
      statement.execute("DELETE FROM appdb.customers WHERE id = 3");
    }
  }

  private static List<String> loadMySqlRows(MySQLContainer source) throws Exception {
    try (Connection connection =
        DriverManager.getConnection(source.getJdbcUrl(), source.getUsername(), source.getPassword());
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("SELECT id, name, status FROM appdb.customers ORDER BY id")) {
      return rows(resultSet);
    }
  }

  private static List<String> loadPostgresRows(PostgreSQLContainer target) throws Exception {
    try (Connection connection =
        DriverManager.getConnection(
            target.getJdbcUrl(), target.getUsername(), target.getPassword());
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("SELECT id, name, status FROM appdb.customers ORDER BY id")) {
      return rows(resultSet);
    }
  }

  private static List<String> rows(ResultSet resultSet) throws Exception {
    List<String> rows = new ArrayList<>();
    while (resultSet.next()) {
      rows.add(
          resultSet.getString(1)
              + "|"
              + resultSet.getString(2)
              + "|"
              + resultSet.getString(3));
    }
    return List.copyOf(rows);
  }

}
