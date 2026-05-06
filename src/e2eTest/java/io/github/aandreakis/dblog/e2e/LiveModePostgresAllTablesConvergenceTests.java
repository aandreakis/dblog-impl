package io.github.aandreakis.dblog.e2e;

import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.postgres.PostgresSourceAdapter;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.core.request.DumpRequestState;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.runtime.bootstrap.DbLogApplication;
import io.github.aandreakis.dblog.sink.jdbc.JdbcApplyChangeEventSink;
import io.github.aandreakis.dblog.sink.jdbc.JdbcApplyTargetDialect;
import io.github.aandreakis.dblog.sink.jdbc.TargetTableResolver;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Tag("integration-docker")
class LiveModePostgresAllTablesConvergenceTests {
  @TempDir Path tempDir;

  @BeforeEach
  void resetSharedContainers() throws Exception {
    assumeDockerIsAvailable();
    SharedLiveE2eContainers.reset();
  }

  @Test
  void allTablesDumpConvergesOnTargetUnderActiveWrites() throws Exception {
    PostgreSQLContainer source = SharedLiveE2eContainers.postgres();
    MySQLContainer target = SharedLiveE2eContainers.mysql();
    initializePostgresSource(source);
    initializeMySqlTarget(target);

    RelationalSourceConfig config =
        PostgresLiveE2eSupport.sourceConfig(
            source,
            "sourceA",
            List.of("public.customers", "public.orders"),
            "dblog_runtime_pub",
            "dblog_runtime_slot");

    TargetTableResolver resolver =
        TargetTableResolver.of(
            Map.of(
                new TableId("appdb", "public", "customers"),
                new TableId("appdb", "appdb", "customers"),
                new TableId("appdb", "public", "orders"),
                new TableId("appdb", "appdb", "orders")));

    try (H2RuntimeStateStore stateStore =
            new H2RuntimeStateStore(tempDir.resolve("postgres-all-tables-state"));
        JdbcApplyChangeEventSink sink =
            JdbcApplyChangeEventSink.forTarget(
                JdbcApplyTargetDialect.MYSQL,
                target.getJdbcUrl(),
                target.getUsername(),
                target.getPassword(),
                4,
                Duration.ofSeconds(2),
                resolver);
        DbLogApplication<?> app =
            DbLogApplication.open(
                new PostgresSourceAdapter(), config, stateStore, null, sink, 1)) {
      DumpRequest request = app.submit(DumpScope.ALL_TABLES, null, List.of());

      AtomicReference<Throwable> writerFailure = new AtomicReference<>();
      Thread writer =
          new Thread(
              () -> {
                try {
                  awaitRequestActive(stateStore, request.requestId(), Duration.ofSeconds(5));
                  try (Connection connection =
                      PostgresLiveE2eSupport.openRuntimeConnection(source)) {
                    try (Statement statement = connection.createStatement()) {
                      statement.execute(
                          "UPDATE public.customers SET name = 'customer-5-updated' WHERE id = 5");
                      statement.execute(
                          "INSERT INTO public.customers (id, name) VALUES (21, 'customer-21')");
                      statement.execute(
                          "UPDATE public.orders SET status = 'shipped' WHERE id = 3");
                      statement.execute(
                          "INSERT INTO public.orders (id, customer_id, status) VALUES (21, 21, 'new')");
                    }
                  }
                } catch (Throwable ex) {
                  writerFailure.set(ex);
                }
              },
              "postgres-active-writes");
      writer.start();

      app.processPendingRequests(Duration.ofMillis(20));
      writer.join();
      if (writerFailure.get() != null) {
        throw new AssertionError("concurrent source writer failed", writerFailure.get());
      }
      drainUntilIdle(app);

      assertThat(stateStore.dumpRequests().loadStatus(request.requestId()).orElseThrow().state())
          .isEqualTo(DumpRequestState.COMPLETED);
      assertThat(loadPostgresRows(source, "customers"))
          .isEqualTo(loadMySqlRows(target, "customers"));
      assertThat(loadPostgresRows(source, "orders"))
          .isEqualTo(loadMySqlRows(target, "orders"));
    }
  }

  private static void initializePostgresSource(PostgreSQLContainer source) throws Exception {
    try (Connection connection =
        PostgresLiveE2eSupport.openAdminConnection(source);
        Statement statement = connection.createStatement()) {
      PostgresLiveE2eSupport.createRuntimeRole(source, statement);
      statement.execute("CREATE TABLE public.customers (id BIGINT PRIMARY KEY, name TEXT NOT NULL)");
      statement.execute(
          "CREATE TABLE public.orders (id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL, status TEXT NOT NULL)");
      statement.execute("ALTER TABLE public.customers REPLICA IDENTITY FULL");
      statement.execute("ALTER TABLE public.orders REPLICA IDENTITY FULL");
      for (int id = 1; id <= 20; id++) {
        statement.execute(
            "INSERT INTO public.customers (id, name) VALUES (" + id + ", 'customer-" + id + "')");
        statement.execute(
            "INSERT INTO public.orders (id, customer_id, status) VALUES ("
                + id
                + ", "
                + id
                + ", 'open')");
      }
      PostgresLiveE2eSupport.transferPublicTableOwnership(statement, "customers", "orders");
    }
  }

  private static void initializeMySqlTarget(MySQLContainer target) throws Exception {
    try (Connection connection =
        DriverManager.getConnection(target.getJdbcUrl(), target.getUsername(), target.getPassword());
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TABLE appdb.customers (id BIGINT PRIMARY KEY, name VARCHAR(255) NOT NULL)");
      statement.execute(
          "CREATE TABLE appdb.orders (id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL, status VARCHAR(64) NOT NULL)");
    }
  }

  private static List<String> loadPostgresRows(PostgreSQLContainer source, String tableName)
      throws Exception {
    try (Connection connection =
        DriverManager.getConnection(
            source.getJdbcUrl(), source.getUsername(), source.getPassword());
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "SELECT * FROM public." + tableName + " ORDER BY id")) {
      return rows(resultSet);
    }
  }

  private static List<String> loadMySqlRows(MySQLContainer target, String tableName) throws Exception {
    try (Connection connection =
        DriverManager.getConnection(target.getJdbcUrl(), target.getUsername(), target.getPassword());
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "SELECT * FROM appdb." + tableName + " ORDER BY id")) {
      return rows(resultSet);
    }
  }

  private static List<String> rows(ResultSet resultSet) throws Exception {
    List<String> rows = new ArrayList<>();
    int columnCount = resultSet.getMetaData().getColumnCount();
    while (resultSet.next()) {
      StringBuilder row = new StringBuilder();
      for (int index = 1; index <= columnCount; index++) {
        if (index > 1) {
          row.append('|');
        }
        row.append(resultSet.getString(index));
      }
      rows.add(row.toString());
    }
    return List.copyOf(rows);
  }

  static void drainUntilIdle(DbLogApplication<?> app) throws Exception {
    int consecutiveIdle = 0;
    for (int attempt = 0; attempt < 10 && consecutiveIdle < 2; attempt++) {
      int drained = app.drainStreaming(Duration.ofMillis(100));
      if (drained == 0) {
        consecutiveIdle++;
      } else {
        consecutiveIdle = 0;
      }
    }
  }

  static void awaitRequestActive(H2RuntimeStateStore stateStore, String requestId, Duration timeout)
      throws InterruptedException {
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadlineNanos) {
      if (stateStore
          .dumpRequests()
          .loadStatus(requestId)
          .filter(status -> status.state() == DumpRequestState.ACTIVE)
          .isPresent()) {
        return;
      }
      Thread.sleep(10L);
    }
    throw new AssertionError(
        "timed out waiting for request " + requestId + " to reach ACTIVE state");
  }

}
