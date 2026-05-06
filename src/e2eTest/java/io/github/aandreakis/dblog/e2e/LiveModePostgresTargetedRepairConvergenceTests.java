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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Tag("integration-docker")
class LiveModePostgresTargetedRepairConvergenceTests {
  private static final Pattern REQUEST_ID_PATTERN =
      Pattern.compile("\"requestId\"\\s*:\\s*\"([^\"]+)\"");

  @TempDir Path tempDir;

  @BeforeEach
  void resetSharedContainers() throws Exception {
    assumeDockerIsAvailable();
    SharedLiveE2eContainers.reset();
  }

  @Test
  void targetedPrimaryKeyRepairHealsDriftedMysqlTargetRows() throws Exception {
    PostgreSQLContainer source = SharedLiveE2eContainers.postgres();
    MySQLContainer target = SharedLiveE2eContainers.mysql();
    initializePostgresSource(source);
    initializeMySqlTarget(target);

    RelationalSourceConfig config =
        PostgresLiveE2eSupport.sourceConfig(
            source,
            "sourceA",
            List.of("public.customers"),
            "dblog_runtime_pub",
            "dblog_runtime_slot");

    TargetTableResolver resolver =
        TargetTableResolver.of(
            Map.of(
                new TableId("appdb", "public", "customers"),
                new TableId("appdb", "appdb", "customers")));

    try (H2RuntimeStateStore stateStore =
            new H2RuntimeStateStore(tempDir.resolve("postgres-targeted-repair-state"));
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
      TableId tableId = new TableId("appdb", "public", "customers");

      DumpRequest initialDump = app.submit(DumpScope.TABLE, tableId, List.of());
      app.processPendingRequests(Duration.ofMillis(20));
      LiveModePostgresAllTablesConvergenceTests.drainUntilIdle(app);

      assertThat(stateStore.dumpRequests().loadStatus(initialDump.requestId()).orElseThrow().state())
          .isEqualTo(DumpRequestState.COMPLETED);
      assertThat(loadPostgresRows(source)).isEqualTo(loadMySqlRows(target));

      applySourceLiveChanges(source);
      LiveModePostgresAllTablesConvergenceTests.drainUntilIdle(app);
      assertThat(loadPostgresRows(source)).isEqualTo(loadMySqlRows(target));

      driftMySqlTarget(target);
      assertThat(loadMySqlRows(target))
          .containsExactly("1|customer-1|DRIFTED");

      DumpRequest repair =
          app.submitFromLiterals(DumpScope.PRIMARY_KEYS, tableId, List.of("1", "3"));
      app.processPendingRequests(Duration.ofMillis(20));
      LiveModePostgresAllTablesConvergenceTests.drainUntilIdle(app);

      assertThat(stateStore.dumpRequests().loadStatus(repair.requestId()).orElseThrow().state())
          .isEqualTo(DumpRequestState.COMPLETED);
      assertThat(
              stateStore.dumpRequests().loadStatus(repair.requestId()).orElseThrow()
                  .missingPrimaryKeyLiterals())
          .isEmpty();
      assertThat(loadMySqlRows(target)).isEqualTo(loadPostgresRows(source));
    }
  }

  @Test
  void targetedPrimaryKeyRepairHealsDriftedMysqlTargetRowsThroughRuntimeLoopAndHttpControlPlane()
      throws Exception {
    PostgreSQLContainer source = SharedLiveE2eContainers.postgres();
    MySQLContainer target = SharedLiveE2eContainers.mysql();
    initializePostgresSource(source);
    initializeMySqlTarget(target);

    RelationalSourceConfig config =
        PostgresLiveE2eSupport.sourceConfig(
            source,
            "sourceA",
            List.of("public.customers"),
            "dblog_runtime_http_pub",
            "dblog_runtime_http_slot");

    TargetTableResolver resolver =
        TargetTableResolver.of(
            Map.of(
                new TableId("appdb", "public", "customers"),
                new TableId("appdb", "appdb", "customers")));

    try (H2RuntimeStateStore stateStore =
            new H2RuntimeStateStore(tempDir.resolve("postgres-targeted-repair-http-state"));
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
      var server = app.controlPlaneServer("127.0.0.1", 0);
      AtomicReference<Throwable> runtimeFailure = new AtomicReference<>();
      Thread runtimeThread =
          new Thread(
              () -> {
                try {
                  runRuntimeLoop(app);
                } catch (Throwable ex) {
                  runtimeFailure.set(ex);
                }
              },
              "postgres-targeted-repair-runtime-loop");
      runtimeThread.start();
      server.start();
      try {
        String baseUrl = "http://127.0.0.1:" + server.boundPort();
        String tableBody =
            postJson(
                baseUrl + "/api/v1/requests",
                """
                {"scope":"TABLE","table":{"databaseName":"appdb","schemaName":"public","tableName":"customers"}}
                """);
        String tableRequestId = extractRequestId(tableBody);
        waitForRequestState(stateStore, tableRequestId, DumpRequestState.COMPLETED, Duration.ofSeconds(20));
        waitForTargetRows(target, loadPostgresRows(source), Duration.ofSeconds(20));

        applySourceLiveChanges(source);
        waitForTargetRows(target, loadPostgresRows(source), Duration.ofSeconds(20));

        driftMySqlTarget(target);
        assertThat(loadMySqlRows(target))
            .containsExactly("1|customer-1|DRIFTED");

        String repairBody =
            postJson(
                baseUrl + "/api/v1/requests",
                """
                {"scope":"PRIMARY_KEYS","table":{"databaseName":"appdb","schemaName":"public","tableName":"customers"},"primaryKeyLiterals":["1","3"]}
                """);
        String repairRequestId = extractRequestId(repairBody);
        waitForRequestState(stateStore, repairRequestId, DumpRequestState.COMPLETED, Duration.ofSeconds(20));
        assertThat(
                stateStore.dumpRequests().loadStatus(repairRequestId).orElseThrow()
                    .missingPrimaryKeyLiterals())
            .isEmpty();
        waitForTargetRows(target, loadPostgresRows(source), Duration.ofSeconds(20));

        assertThat(runtimeFailure.get()).isNull();
      } finally {
        app.stack().requestPump().requestStop();
        runtimeThread.join(Duration.ofSeconds(10).toMillis());
        server.stop();
        if (runtimeFailure.get() != null) {
          throw new AssertionError("runtime loop failed", runtimeFailure.get());
        }
      }
    }
  }

  private static void initializePostgresSource(PostgreSQLContainer source) throws Exception {
    try (Connection connection =
        PostgresLiveE2eSupport.openAdminConnection(source);
        Statement statement = connection.createStatement()) {
      PostgresLiveE2eSupport.createRuntimeRole(source, statement);
      statement.execute(
          "CREATE TABLE public.customers (id BIGINT PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL)");
      statement.execute("ALTER TABLE public.customers REPLICA IDENTITY FULL");
      statement.execute(
          "INSERT INTO public.customers (id, name, status) VALUES (1, 'customer-1', 'PENDING')");
      statement.execute(
          "INSERT INTO public.customers (id, name, status) VALUES (2, 'customer-2', 'READY')");
      PostgresLiveE2eSupport.transferPublicTableOwnership(statement, "customers");
    }
  }

  private static void initializeMySqlTarget(MySQLContainer target) throws Exception {
    try (Connection connection =
        DriverManager.getConnection(target.getJdbcUrl(), target.getUsername(), target.getPassword());
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE appdb.customers (id BIGINT PRIMARY KEY, name VARCHAR(255) NOT NULL, status VARCHAR(64) NOT NULL)");
    }
  }

  private static void applySourceLiveChanges(PostgreSQLContainer source) throws Exception {
    try (Connection connection =
        PostgresLiveE2eSupport.openRuntimeConnection(source);
        Statement statement = connection.createStatement()) {
      statement.execute("UPDATE public.customers SET status = 'SYNCED' WHERE id = 1");
      statement.execute(
          "INSERT INTO public.customers (id, name, status) VALUES (3, 'customer-3', 'NEW')");
      statement.execute("DELETE FROM public.customers WHERE id = 2");
    }
  }

  private static void driftMySqlTarget(MySQLContainer target) throws Exception {
    try (Connection connection =
        DriverManager.getConnection(target.getJdbcUrl(), target.getUsername(), target.getPassword());
        Statement statement = connection.createStatement()) {
      statement.execute("UPDATE appdb.customers SET status = 'DRIFTED' WHERE id = 1");
      statement.execute("DELETE FROM appdb.customers WHERE id = 3");
    }
  }

  private static List<String> loadPostgresRows(PostgreSQLContainer source) throws Exception {
    try (Connection connection =
        DriverManager.getConnection(
            source.getJdbcUrl(), source.getUsername(), source.getPassword());
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("SELECT id, name, status FROM public.customers ORDER BY id")) {
      return rows(resultSet);
    }
  }

  private static List<String> loadMySqlRows(MySQLContainer target) throws Exception {
    try (Connection connection =
        DriverManager.getConnection(target.getJdbcUrl(), target.getUsername(), target.getPassword());
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

  private static void waitForRequestState(
      H2RuntimeStateStore stateStore,
      String requestId,
      DumpRequestState expectedState,
      Duration timeout)
      throws Exception {
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadlineNanos) {
      if (stateStore.dumpRequests().loadStatus(requestId).isPresent()
          && stateStore.dumpRequests().loadStatus(requestId).orElseThrow().state() == expectedState) {
        return;
      }
      Thread.sleep(100L);
    }
    throw new AssertionError(
        "Timed out waiting for request " + requestId + " to reach state " + expectedState);
  }

  private static void waitForTargetRows(
      MySQLContainer target, List<String> expectedRows, Duration timeout) throws Exception {
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadlineNanos) {
      if (loadMySqlRows(target).equals(expectedRows)) {
        return;
      }
      Thread.sleep(100L);
    }
    throw new AssertionError(
        "Timed out waiting for target rows. expected="
            + expectedRows
            + " actual="
            + loadMySqlRows(target));
  }

  private static String postJson(String url, String body) throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(url))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
    return HttpClient.newHttpClient()
        .send(request, HttpResponse.BodyHandlers.ofString())
        .body();
  }

  private static String extractRequestId(String jsonBody) {
    Matcher matcher = REQUEST_ID_PATTERN.matcher(jsonBody);
    if (!matcher.find()) {
      throw new IllegalArgumentException("requestId not found in response: " + jsonBody);
    }
    return matcher.group(1);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void runRuntimeLoop(DbLogApplication<?> app) throws Exception {
    var stack = (io.github.aandreakis.dblog.runtime.bootstrap.RelationalRuntimeStack) app.stack();
    var requestPump = (io.github.aandreakis.dblog.runtime.loop.RuntimeRequestPump) stack.requestPump();
    var coordinator = (io.github.aandreakis.dblog.core.request.DumpRequestCoordinator) stack.coordinator();
    requestPump.runUntilStopped(coordinator, "runtime-streaming");
  }

}
