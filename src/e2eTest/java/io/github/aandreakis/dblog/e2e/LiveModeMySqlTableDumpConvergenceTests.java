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
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Tag("integration-docker")
class LiveModeMySqlTableDumpConvergenceTests {
  @TempDir Path tempDir;

  @BeforeEach
  void resetSharedContainers() throws Exception {
    assumeDockerIsAvailable();
    SharedLiveE2eContainers.reset();
  }

  @Test
  void tableScopedDumpConvergesOnTargetUnderActiveWrites() throws Exception {
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
            java.util.Map.of("mysql.serverId", "223356"),
            false);

    try (H2RuntimeStateStore stateStore =
            new H2RuntimeStateStore(tempDir.resolve("mysql-table-dump-state"));
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
      DumpRequest request =
          app.submit(DumpScope.TABLE, new TableId("sourceA", "appdb", "customers"), List.of());

      AtomicReference<Throwable> writerFailure = new AtomicReference<>();
      Thread writer =
          new Thread(
              () -> {
                try {
                  LiveModeMySqlAllTablesConvergenceTests.awaitRequestActive(
                      stateStore, request.requestId(), Duration.ofSeconds(5));
                  try (Connection connection =
                      DriverManager.getConnection(
                          source.getJdbcUrl(), source.getUsername(), source.getPassword());
                      Statement statement = connection.createStatement()) {
                    statement.execute(
                        "UPDATE appdb.customers SET name = 'customer-5-updated' WHERE id = 5");
                    statement.execute(
                        "INSERT INTO appdb.customers (id, name) VALUES (21, 'customer-21')");
                  }
                } catch (Throwable ex) {
                  writerFailure.set(ex);
                }
              },
              "mysql-table-active-writes");
      writer.start();

      app.processPendingRequests(Duration.ofMillis(20));
      writer.join();
      if (writerFailure.get() != null) {
        throw new AssertionError("concurrent source writer failed", writerFailure.get());
      }
      LiveModeMySqlAllTablesConvergenceTests.drainUntilIdle(app);

      assertThat(stateStore.dumpRequests().loadStatus(request.requestId()).orElseThrow().state())
          .isEqualTo(DumpRequestState.COMPLETED);
      assertThat(loadMySqlRows(source)).isEqualTo(loadPostgresRows(target));
    }
  }

  private static void initializeMySqlSource(MySQLContainer source) throws Exception {
    try (Connection connection =
        DriverManager.getConnection(source.getJdbcUrl(), source.getUsername(), source.getPassword());
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TABLE appdb.customers (id BIGINT PRIMARY KEY, name VARCHAR(255))");
      for (int id = 1; id <= 20; id++) {
        statement.execute(
            "INSERT INTO appdb.customers (id, name) VALUES (" + id + ", 'customer-" + id + "')");
      }
    }
  }

  private static void initializePostgresTarget(PostgreSQLContainer target) throws Exception {
    try (Connection connection =
        DriverManager.getConnection(
            target.getJdbcUrl(), target.getUsername(), target.getPassword());
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE SCHEMA IF NOT EXISTS appdb");
      statement.execute("CREATE TABLE appdb.customers (id BIGINT PRIMARY KEY, name TEXT NOT NULL)");
    }
  }

  private static List<String> loadMySqlRows(MySQLContainer source) throws Exception {
    try (Connection connection =
        DriverManager.getConnection(source.getJdbcUrl(), source.getUsername(), source.getPassword());
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("SELECT * FROM appdb.customers ORDER BY id")) {
      return rows(resultSet);
    }
  }

  private static List<String> loadPostgresRows(PostgreSQLContainer target) throws Exception {
    try (Connection connection =
        DriverManager.getConnection(
            target.getJdbcUrl(), target.getUsername(), target.getPassword());
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("SELECT * FROM appdb.customers ORDER BY id")) {
      return rows(resultSet);
    }
  }

  private static List<String> rows(ResultSet resultSet) throws Exception {
    List<String> rows = new ArrayList<>();
    while (resultSet.next()) {
      rows.add(resultSet.getString(1) + "|" + resultSet.getString(2));
    }
    return List.copyOf(rows);
  }

}
