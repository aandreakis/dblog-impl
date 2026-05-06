package io.github.aandreakis.dblog.e2e;

import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.DbLogRuntimeException;
import io.github.aandreakis.dblog.adapter.api.AdapterConnectionSupport;
import io.github.aandreakis.dblog.adapter.api.HeartbeatMetadataWriter;
import io.github.aandreakis.dblog.adapter.api.OpenedSourceRuntime;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfigValidator;
import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.api.SourceConnections;
import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.adapter.api.WatermarkMetadataWriter;
import io.github.aandreakis.dblog.adapter.mysql.MySqlDialect;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourceAdapter;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourceCheckpointStore;
import io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlChunkReader;
import io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlHeartbeatTableHelper;
import io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlSourceSchemaInspector;
import io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlWatermarkTableHelper;
import io.github.aandreakis.dblog.adapter.mysql.internal.MySqlLiveRuntimeFactory;
import io.github.aandreakis.dblog.adapter.postgres.PostgresSourceAdapter;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.request.CoreRequestExecutionException;
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.core.request.DumpRequestState;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.runtime.bootstrap.DbLogApplication;
import io.github.aandreakis.dblog.sink.jdbc.JdbcApplyChangeEventSink;
import io.github.aandreakis.dblog.sink.jdbc.JdbcApplyTargetDialect;
import io.github.aandreakis.dblog.sink.jdbc.TargetTableResolver;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import io.github.aandreakis.dblog.tap.Tap;
import io.github.aandreakis.dblog.testsupport.LiveMySqlTestContainers;
import io.github.aandreakis.dblog.testsupport.LivePostgresTestContainers;
import io.github.aandreakis.dblog.testsupport.MySqlTestUserGrants;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@Tag("integration-docker")
class LiveModeSourceOutageReconnectTests {
  @TempDir Path tempDir;

  @Test
  void mysqlSourceContainerRestartReconnectsInPlaceAndContinuesStreaming() throws Exception {
    assumeDockerIsAvailable();

    try (MySQLContainer source = restartableMySqlSourceContainer("223721");
        PostgreSQLContainer target = LivePostgresTestContainers.newBaseContainer()) {
      Startables.deepStart(source, target).join();
      MySqlTestUserGrants.applyDblogUserGrants(source, "appdb");
      initializeMySqlSource(source);
      initializePostgresTarget(target);

      RelationalSourceConfig config =
          new RelationalSourceConfig(
              "sourceA",
              source.getJdbcUrl(),
              source.getUsername(),
              source.getPassword(),
              "appdb",
              List.of("appdb.widgets"),
              Map.of(
                  "mysql.serverId", "223722",
                  "mysql.retryLogConnectionLoss", "true",
                  "mysql.reconnectBackoff", "PT0.1S"),
              false);

      try (H2RuntimeStateStore stateStore =
              new H2RuntimeStateStore(tempDir.resolve("mysql-source-outage-state"));
          JdbcApplyChangeEventSink sink =
              JdbcApplyChangeEventSink.forTarget(
                  JdbcApplyTargetDialect.POSTGRES,
                  target.getJdbcUrl(),
                  target.getUsername(),
                  target.getPassword(),
                  4);
          DbLogApplication<?> app =
              DbLogApplication.open(new MySqlSourceAdapter(), config, stateStore, null, sink, 1)) {
        DumpRequest request = app.submit(DumpScope.ALL_TABLES, null, List.of());
        app.processPendingRequests(Duration.ofMillis(20));
        LiveModeMySqlAllTablesConvergenceTests.drainUntilIdle(app);

        assertThat(stateStore.dumpRequests().loadStatus(request.requestId()).orElseThrow().state())
            .isEqualTo(DumpRequestState.COMPLETED);
        assertThat(loadPostgresRows(target)).isEqualTo(List.of("1|mysql-one|PENDING"));

        stopContainer(source.getContainerId());
        Thread.sleep(Duration.ofMillis(500).toMillis());
        startMySqlContainer(source);
        try (Connection connection =
                DriverManager.getConnection(
                    source.getJdbcUrl(), source.getUsername(), source.getPassword());
            Statement statement = connection.createStatement()) {
          statement.execute("UPDATE appdb.widgets SET status = 'RECONNECTED' WHERE id = 1");
          statement.execute(
              "INSERT INTO appdb.widgets (id, name, status) VALUES (2, 'mysql-two', 'NEW')");
        }

        LiveModeMySqlAllTablesConvergenceTests.drainUntilIdle(app);
        assertThat(loadPostgresRows(target))
            .isEqualTo(List.of("1|mysql-one|RECONNECTED", "2|mysql-two|NEW"));
      }
    }
  }

  @Test
  void mysqlSourceOutageDuringActiveWatermarkWindowReconnectsAndRetriesRequest()
      throws Exception {
    assumeDockerIsAvailable();

    try (MySQLContainer source = restartableMySqlSourceContainer("223726");
        PostgreSQLContainer target = LivePostgresTestContainers.newBaseContainer()) {
      Startables.deepStart(source, target).join();
      MySqlTestUserGrants.applyDblogUserGrants(source, "appdb");
      initializeMySqlSource(source);
      initializePostgresTarget(target);

      AtomicBoolean outageTriggered = new AtomicBoolean();
      SourceChunkReader outageChunkReader =
          interruptingChunkReader(
              new JdbcMySqlChunkReader(),
              () -> {
                if (outageTriggered.compareAndSet(false, true)) {
                  stopContainer(source.getContainerId());
                  Thread restarter =
                      new Thread(
                          () -> {
                            try {
                              Thread.sleep(Duration.ofMillis(500).toMillis());
                              startMySqlContainer(source);
                            } catch (Exception failure) {
                              throw new AssertionError(
                                  "failed to restart MySQL source after active-window outage",
                                  failure);
                            }
                          },
                          "mysql-active-window-source-restarter");
                  restarter.start();
                  throw new SQLTransientConnectionException(
                      "simulated MySQL source outage during active watermark window");
                }
              });

      RelationalSourceConfig config =
          new RelationalSourceConfig(
              "sourceA",
              source.getJdbcUrl(),
              source.getUsername(),
              source.getPassword(),
              "appdb",
              List.of("appdb.widgets"),
              Map.of(
                  "mysql.serverId", "223727",
                  "mysql.retryLogConnectionLoss", "true",
                  "mysql.reconnectBackoff", "PT0.1S"),
              false);

      try (H2RuntimeStateStore stateStore =
              new H2RuntimeStateStore(tempDir.resolve("mysql-active-window-outage-state"));
          JdbcApplyChangeEventSink sink =
              JdbcApplyChangeEventSink.forTarget(
                  JdbcApplyTargetDialect.POSTGRES,
                  target.getJdbcUrl(),
                  target.getUsername(),
                  target.getPassword(),
                  4);
          DbLogApplication<?> app =
              DbLogApplication.open(
                  mySqlAdapter(outageChunkReader), config, stateStore, null, sink, 1)) {
        DumpRequest request = app.submit(DumpScope.ALL_TABLES, null, List.of());

        assertThatThrownBy(() -> app.processPendingRequests(Duration.ofMillis(20)))
            .isInstanceOf(CoreRequestExecutionException.class)
            .hasRootCauseMessage("simulated MySQL source outage during active watermark window");
        assertThat(stateStore.dumpRequests().loadStatus(request.requestId()).orElseThrow().state())
            .isEqualTo(DumpRequestState.ACTIVE);
        assertThat(loadPostgresRows(target)).isEmpty();

        app.processPendingRequests(Duration.ofMillis(20));
        LiveModeMySqlAllTablesConvergenceTests.drainUntilIdle(app);

        assertThat(stateStore.dumpRequests().loadStatus(request.requestId()).orElseThrow().state())
            .isEqualTo(DumpRequestState.COMPLETED);
        assertThat(loadPostgresRows(target)).isEqualTo(List.of("1|mysql-one|PENDING"));

        try (Connection connection =
                DriverManager.getConnection(
                    source.getJdbcUrl(), source.getUsername(), source.getPassword());
            Statement statement = connection.createStatement()) {
          statement.execute("UPDATE appdb.widgets SET status = 'AFTER_WINDOW_RETRY' WHERE id = 1");
        }
        LiveModeMySqlAllTablesConvergenceTests.drainUntilIdle(app);
        assertThat(loadPostgresRows(target)).isEqualTo(List.of("1|mysql-one|AFTER_WINDOW_RETRY"));
      }
    }
  }

  @Test
  void mysqlSourceOutageFailsClosedWhenRetryIsDisabled() throws Exception {
    assumeDockerIsAvailable();

    try (MySQLContainer source =
            LiveMySqlTestContainers.newContainer(
                DockerImageName.parse(LiveMySqlTestContainers.DEFAULT_IMAGE), "223724");
        PostgreSQLContainer target = LivePostgresTestContainers.newBaseContainer()) {
      Startables.deepStart(source, target).join();
      MySqlTestUserGrants.applyDblogUserGrants(source, "appdb");
      initializeMySqlSource(source);
      initializePostgresTarget(target);

      RelationalSourceConfig config =
          new RelationalSourceConfig(
              "sourceA",
              source.getJdbcUrl(),
              source.getUsername(),
              source.getPassword(),
              "appdb",
              List.of("appdb.widgets"),
              Map.of("mysql.serverId", "223725"),
              false);

      try (H2RuntimeStateStore stateStore =
              new H2RuntimeStateStore(tempDir.resolve("mysql-source-outage-no-retry-state"));
          JdbcApplyChangeEventSink sink =
              JdbcApplyChangeEventSink.forTarget(
                  JdbcApplyTargetDialect.POSTGRES,
                  target.getJdbcUrl(),
                  target.getUsername(),
                  target.getPassword(),
                  4);
          DbLogApplication<?> app =
              DbLogApplication.open(new MySqlSourceAdapter(), config, stateStore, null, sink, 1)) {
        DumpRequest request = app.submit(DumpScope.ALL_TABLES, null, List.of());
        app.processPendingRequests(Duration.ofMillis(20));
        LiveModeMySqlAllTablesConvergenceTests.drainUntilIdle(app);

        assertThat(stateStore.dumpRequests().loadStatus(request.requestId()).orElseThrow().state())
            .isEqualTo(DumpRequestState.COMPLETED);
        assertThat(loadPostgresRows(target)).isEqualTo(List.of("1|mysql-one|PENDING"));

        source.stop();

        assertThatThrownBy(() -> app.drainStreaming(Duration.ofSeconds(2)))
            .isInstanceOf(SQLTransientConnectionException.class)
            .hasMessage("MySQL binlog stream disconnected unexpectedly");
      }
    }
  }

  @Test
  void postgresSourceContainerRestartReconnectsInPlaceAndContinuesStreaming()
      throws Exception {
    assumeDockerIsAvailable();

    try (PostgreSQLContainer source = restartablePostgresSourceContainer();
        MySQLContainer target =
            LiveMySqlTestContainers.newContainer(
                DockerImageName.parse(LiveMySqlTestContainers.DEFAULT_IMAGE), "223723")) {
      Startables.deepStart(source, target).join();
      initializePostgresSource(source);
      initializeMySqlTarget(target);

      RelationalSourceConfig config =
          new RelationalSourceConfig(
              "sourceA",
              source.getJdbcUrl(),
              PostgresLiveE2eSupport.RUNTIME_USERNAME,
              PostgresLiveE2eSupport.RUNTIME_PASSWORD,
              source.getDatabaseName(),
              List.of("public.widgets"),
              Map.of(
                  "postgres.publicationName", "dblog_outage_pub",
                  "postgres.slotName", "dblog_outage_slot",
                  "postgres.retryLogConnectionLoss", "true",
                  "postgres.reconnectBackoff", "PT3S"),
              false);
      TargetTableResolver resolver =
          TargetTableResolver.of(
              Map.of(
                  new TableId(source.getDatabaseName(), "public", "widgets"),
                  new TableId("appdb", "appdb", "widgets")));

      try (H2RuntimeStateStore stateStore =
              new H2RuntimeStateStore(tempDir.resolve("postgres-source-outage-state"));
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
              DbLogApplication.open(new PostgresSourceAdapter(), config, stateStore, null, sink, 1)) {
        DumpRequest request = app.submit(DumpScope.ALL_TABLES, null, List.of());
        app.processPendingRequests(Duration.ofMillis(20));
        LiveModePostgresAllTablesConvergenceTests.drainUntilIdle(app);

        assertThat(stateStore.dumpRequests().loadStatus(request.requestId()).orElseThrow().state())
            .isEqualTo(DumpRequestState.COMPLETED);
        assertThat(loadMySqlRows(target)).isEqualTo(List.of("1|postgres-one|PENDING"));

        Thread.sleep(Duration.ofSeconds(6).toMillis());
        stopContainer(source.getContainerId());
        Thread.sleep(Duration.ofMillis(500).toMillis());
        startPostgresContainer(source);
        try (Connection connection = PostgresLiveE2eSupport.openRuntimeConnection(source);
            Statement statement = connection.createStatement()) {
          statement.execute("UPDATE public.widgets SET status = 'RECONNECTED' WHERE id = 1");
          statement.execute(
              "INSERT INTO public.widgets (id, name, status) VALUES (2, 'postgres-two', 'NEW')");
        }

        LiveModePostgresAllTablesConvergenceTests.drainUntilIdle(app);
        assertThat(loadMySqlRows(target))
            .isEqualTo(List.of("1|postgres-one|RECONNECTED", "2|postgres-two|NEW"));
      }
    }
  }

  @Test
  void postgresSourceOutageFailsClosedWhenRetryIsDisabled() throws Exception {
    assumeDockerIsAvailable();

    try (PostgreSQLContainer source = LivePostgresTestContainers.newDefaultContainer();
        MySQLContainer target =
            LiveMySqlTestContainers.newContainer(
                DockerImageName.parse(LiveMySqlTestContainers.DEFAULT_IMAGE), "223728")) {
      Startables.deepStart(source, target).join();
      initializePostgresSource(source);
      initializeMySqlTarget(target);

      RelationalSourceConfig config =
          new RelationalSourceConfig(
              "sourceA",
              source.getJdbcUrl(),
              PostgresLiveE2eSupport.RUNTIME_USERNAME,
              PostgresLiveE2eSupport.RUNTIME_PASSWORD,
              source.getDatabaseName(),
              List.of("public.widgets"),
              Map.of(
                  "postgres.publicationName", "dblog_no_retry_pub",
                  "postgres.slotName", "dblog_no_retry_slot"),
              false);
      TargetTableResolver resolver =
          TargetTableResolver.of(
              Map.of(
                  new TableId(source.getDatabaseName(), "public", "widgets"),
                  new TableId("appdb", "appdb", "widgets")));

      try (H2RuntimeStateStore stateStore =
              new H2RuntimeStateStore(tempDir.resolve("postgres-source-outage-no-retry-state"));
          JdbcApplyChangeEventSink sink =
              JdbcApplyChangeEventSink.forTarget(
                  JdbcApplyTargetDialect.MYSQL,
                  target.getJdbcUrl(),
                  target.getUsername(),
                  target.getPassword(),
                  4,
                  Duration.ofSeconds(2),
                  resolver)) {
        DbLogApplication<?> app =
            DbLogApplication.open(new PostgresSourceAdapter(), config, stateStore, null, sink, 1);
        try {
          DumpRequest request = app.submit(DumpScope.ALL_TABLES, null, List.of());
          app.processPendingRequests(Duration.ofMillis(20));
          LiveModePostgresAllTablesConvergenceTests.drainUntilIdle(app);

          assertThat(stateStore.dumpRequests().loadStatus(request.requestId()).orElseThrow().state())
              .isEqualTo(DumpRequestState.COMPLETED);
          assertThat(loadMySqlRows(target)).isEqualTo(List.of("1|postgres-one|PENDING"));

          source.stop();

          assertThatThrownBy(() -> app.drainStreaming(Duration.ofSeconds(5)))
              .isInstanceOf(SQLException.class);
        } finally {
          closeIgnoringSqlFailure(app);
        }
      }
    }
  }

  private static void initializeMySqlSource(MySQLContainer source) throws Exception {
    try (Connection connection =
            DriverManager.getConnection(
                source.getJdbcUrl(), source.getUsername(), source.getPassword());
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE appdb.widgets (id BIGINT PRIMARY KEY, name VARCHAR(255) NOT NULL, status VARCHAR(64) NOT NULL)");
      statement.execute(
          "INSERT INTO appdb.widgets (id, name, status) VALUES (1, 'mysql-one', 'PENDING')");
    }
  }

  private static MySQLContainer restartableMySqlSourceContainer(String serverId) throws Exception {
    MySQLContainer container =
        LiveMySqlTestContainers
            .newContainer(DockerImageName.parse(LiveMySqlTestContainers.DEFAULT_IMAGE), serverId)
            .withCreateContainerCmdModifier(
                command -> command.getHostConfig().withAutoRemove(false));
    // Random Docker port mappings did not remain reachable after raw restart on Docker Desktop.
    container.setPortBindings(List.of(availableHostPort() + ":3306"));
    return container;
  }

  private static PostgreSQLContainer restartablePostgresSourceContainer() throws Exception {
    PostgreSQLContainer container =
        LivePostgresTestContainers
            .newDefaultContainer()
            .withCreateContainerCmdModifier(
                command -> command.getHostConfig().withAutoRemove(false));
    // Random Docker port mappings did not remain reachable after raw restart on Docker Desktop.
    container.setPortBindings(List.of(availableHostPort() + ":5432"));
    return container;
  }

  private static void closeIgnoringSqlFailure(AutoCloseable closeable) throws Exception {
    try {
      closeable.close();
    } catch (Exception failure) {
      if (!hasSqlCause(failure)) {
        throw failure;
      }
    }
  }

  private static boolean hasSqlCause(Throwable failure) {
    Throwable current = failure;
    while (current != null) {
      if (current instanceof SQLException) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private static int availableHostPort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(false);
      return socket.getLocalPort();
    }
  }

  private static void initializePostgresTarget(PostgreSQLContainer target) throws Exception {
    try (Connection connection =
            DriverManager.getConnection(
                target.getJdbcUrl(), target.getUsername(), target.getPassword());
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE SCHEMA IF NOT EXISTS appdb");
      statement.execute(
          "CREATE TABLE appdb.widgets (id BIGINT PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL)");
    }
  }

  private static void initializePostgresSource(PostgreSQLContainer source) throws Exception {
    try (Connection connection = PostgresLiveE2eSupport.openAdminConnection(source);
        Statement statement = connection.createStatement()) {
      PostgresLiveE2eSupport.createRuntimeRole(source, statement);
      statement.execute(
          "CREATE TABLE public.widgets (id BIGINT PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL)");
      statement.execute("ALTER TABLE public.widgets REPLICA IDENTITY FULL");
      statement.execute(
          "INSERT INTO public.widgets (id, name, status) VALUES (1, 'postgres-one', 'PENDING')");
      PostgresLiveE2eSupport.transferPublicTableOwnership(statement, "widgets");
    }
  }

  private static void initializeMySqlTarget(MySQLContainer target) throws Exception {
    try (Connection connection =
            DriverManager.getConnection(
                target.getJdbcUrl(), target.getUsername(), target.getPassword());
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE appdb.widgets (id BIGINT PRIMARY KEY, name VARCHAR(255) NOT NULL, status VARCHAR(64) NOT NULL)");
    }
  }

  private static void stopContainer(String containerId) {
    DockerClientFactory.instance().client().stopContainerCmd(containerId).exec();
  }

  private static void startMySqlContainer(MySQLContainer source) throws Exception {
    DockerClientFactory.instance().client().startContainerCmd(source.getContainerId()).exec();
    awaitMySqlReachable(source, Duration.ofSeconds(60));
  }

  private static void startPostgresContainer(PostgreSQLContainer source) throws Exception {
    DockerClientFactory.instance().client().startContainerCmd(source.getContainerId()).exec();
    awaitPostgresReachable(source, Duration.ofSeconds(60));
  }

  private static void awaitMySqlReachable(MySQLContainer source, Duration timeout)
      throws Exception {
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    Exception lastFailure = null;
    while (System.nanoTime() < deadlineNanos) {
      try (Connection connection =
              DriverManager.getConnection(
                  source.getJdbcUrl(), source.getUsername(), source.getPassword());
          Statement statement = connection.createStatement()) {
        statement.execute("SELECT 1");
        return;
      } catch (Exception failure) {
        lastFailure = failure;
        Thread.sleep(100L);
      }
    }
    throw new AssertionError("timed out waiting for restarted MySQL container", lastFailure);
  }

  private static void awaitPostgresReachable(PostgreSQLContainer source, Duration timeout)
      throws Exception {
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    Exception lastFailure = null;
    while (System.nanoTime() < deadlineNanos) {
      try (Connection connection = PostgresLiveE2eSupport.openAdminConnection(source);
          Statement statement = connection.createStatement()) {
        statement.execute("SELECT 1");
        return;
      } catch (Exception failure) {
        lastFailure = failure;
        Thread.sleep(100L);
      }
    }
    throw new AssertionError("timed out waiting for restarted PostgreSQL container", lastFailure);
  }

  private static MySqlSourceAdapter mySqlAdapter(SourceChunkReader chunkReader) {
    return new MySqlSourceAdapter(
        new MySqlSourceAdapter.Dependencies() {
          private final JdbcMySqlSourceSchemaInspector inspector =
              new JdbcMySqlSourceSchemaInspector();
          private final JdbcMySqlWatermarkTableHelper watermarkWriter =
              new JdbcMySqlWatermarkTableHelper();
          private final JdbcMySqlHeartbeatTableHelper heartbeatWriter =
              new JdbcMySqlHeartbeatTableHelper();
          private final MySqlLiveRuntimeFactory liveRuntimeFactory = new MySqlLiveRuntimeFactory();

          @Override
          public Connection openSqlConnection(RelationalSourceConfig config) {
            try {
              return AdapterConnectionSupport.openConfiguredSqlConnection(
                  config, MySqlDialect.DISPLAY_NAME);
            } catch (SQLException failure) {
              throw new DbLogRuntimeException(failure);
            }
          }

          @Override
          public List<TableSchema> inspectSchemas(
              Connection connection, RelationalSourceConfig config) {
            String jdbcDatabase =
                RelationalSourceConfigValidator.databaseNameFromJdbcUrl(
                    config.jdbcUrl(), MySqlDialect.JDBC_PREFIX, MySqlDialect.DISPLAY_NAME);
            String effectiveDatabase =
                config.databaseName() == null ? jdbcDatabase : config.databaseName();
            try {
              return inspector.inspectCapturedSchemas(
                  connection, config.sourceId(), effectiveDatabase, config.capturedTables());
            } catch (SQLException failure) {
              throw new DbLogRuntimeException(failure);
            }
          }

          @Override
          public SourceChunkReader chunkReader() {
            return chunkReader;
          }

          @Override
          public WatermarkMetadataWriter watermarkWriter() {
            return watermarkWriter;
          }

          @Override
          public HeartbeatMetadataWriter heartbeatWriter() {
            return heartbeatWriter;
          }

          @Override
          public Optional<OpenedSourceRuntime<? extends SourceTransaction<?>>> maybeOpenLiveRuntime(
              RelationalSourceConfig config,
              RuntimeStateStore stateStore,
              List<TableSchema> contractSchemas,
              MySqlSourceCheckpointStore checkpointStore,
              SourceConnections connections,
              SourceChunkReader chunkReader,
              Tap tap) {
            try {
              return Optional.of(
                  liveRuntimeFactory.open(
                      config,
                      connections.sql(),
                      contractSchemas,
                      chunkReader,
                      watermarkWriter(),
                      heartbeatWriter(),
                      checkpointStore,
                      tap));
            } catch (RuntimeException | Error failure) {
              throw failure;
            } catch (Exception failure) {
              throw new DbLogRuntimeException(failure);
            }
          }
        });
  }

  private static SourceChunkReader interruptingChunkReader(
      SourceChunkReader delegate, SqlInterruption interruption) {
    return new SourceChunkReader() {
      @Override
      public Optional<PrimaryKeyTuple> tableScanUpperBoundPrimaryKeyTuple(
          Connection connection, TableSchema schema) throws SQLException {
        return delegate.tableScanUpperBoundPrimaryKeyTuple(connection, schema);
      }

      @Override
      public Optional<Chunk> nextTableChunk(
          Connection connection,
          String jobId,
          TableSchema schema,
          PrimaryKeyTuple startAfterPrimaryKey,
          PrimaryKeyTuple stopAtPrimaryKey,
          int chunkSize)
          throws SQLException {
        interruption.run();
        return delegate.nextTableChunk(
            connection, jobId, schema, startAfterPrimaryKey, stopAtPrimaryKey, chunkSize);
      }

      @Override
      public Optional<Chunk> targetedPrimaryKeyTuples(
          Connection connection,
          String jobId,
          TableSchema schema,
          List<PrimaryKeyTuple> requestedPrimaryKeys)
          throws SQLException {
        return delegate.targetedPrimaryKeyTuples(connection, jobId, schema, requestedPrimaryKeys);
      }
    };
  }

  private static List<String> loadPostgresRows(PostgreSQLContainer target) throws Exception {
    try (Connection connection =
            DriverManager.getConnection(
                target.getJdbcUrl(), target.getUsername(), target.getPassword());
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "SELECT id, name, status FROM appdb.widgets ORDER BY id")) {
      return rows(resultSet);
    }
  }

  private static List<String> loadMySqlRows(MySQLContainer target) throws Exception {
    try (Connection connection =
            DriverManager.getConnection(
                target.getJdbcUrl(), target.getUsername(), target.getPassword());
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "SELECT id, name, status FROM appdb.widgets ORDER BY id")) {
      return rows(resultSet);
    }
  }

  private static List<String> rows(ResultSet resultSet) throws Exception {
    java.util.ArrayList<String> rows = new java.util.ArrayList<>();
    while (resultSet.next()) {
      rows.add(
          resultSet.getLong("id")
              + "|"
              + resultSet.getString("name")
              + "|"
              + resultSet.getString("status"));
    }
    return List.copyOf(rows);
  }

  @FunctionalInterface
  private interface SqlInterruption {
    void run() throws SQLException;
  }
}
