package io.github.aandreakis.dblog.integration.adapter.mysql;

import static io.github.aandreakis.dblog.integration.adapter.LiveRuntimeTestSupport.TRANSACTION_AWAIT_TIMEOUT;
import static io.github.aandreakis.dblog.integration.adapter.LiveRuntimeTestSupport.awaitTransaction;
import static io.github.aandreakis.dblog.integration.adapter.LiveRuntimeTestSupport.drainEvents;
import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.adapter.api.OpenedSourceRuntime;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.SourceRuntime;
import io.github.aandreakis.dblog.adapter.mysql.MySqlBinlogTransaction;
import io.github.aandreakis.dblog.adapter.mysql.MySqlLiveStreamingRuntime;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourceAdapter;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourceCheckpointStore;
import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.reconcile.HeartbeatMetadata;
import io.github.aandreakis.dblog.core.reconcile.WatermarkMetadata;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.runtime.bootstrap.DbLogApplication;
import io.github.aandreakis.dblog.sink.api.NoOpChangeEventSink;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import io.github.aandreakis.dblog.testsupport.LiveMySqlTestContainers;
import io.github.aandreakis.dblog.testsupport.MySqlTestUserGrants;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

@Tag("integration-docker")
class MySqlSourceAdapterLiveRuntimeIT {
  private static SharedMySQLContainer sharedMysql;

  @TempDir Path tempDir;

  @BeforeAll
  static void startSharedMysql() throws Exception {
    assumeDockerIsAvailable();
    sharedMysql = new SharedMySQLContainer(DockerImageName.parse("mysql:8.4"));
    sharedMysql.start();
    configureReplicationUser(sharedMysql);
  }

  @AfterAll
  static void stopSharedMysql() {
    if (sharedMysql != null) {
      sharedMysql.stopShared();
    }
  }

  @BeforeEach
  void resetSharedMysql() throws Exception {
    if (sharedMysql == null || !sharedMysql.isRunning()) {
      return;
    }
    try (Connection connection =
            DriverManager.getConnection(
                rootJdbcUrl(sharedMysql), "root", sharedMysql.getPassword());
        Statement statement = connection.createStatement()) {
      configureSqlConnection(connection);
      statement.execute("DROP DATABASE IF EXISTS appdb");
      statement.execute("CREATE DATABASE appdb");
      statement.execute("DROP DATABASE IF EXISTS dblog_meta");
      statement.execute("CREATE DATABASE dblog_meta");
    }
  }

  @Test
  void streamsCommittedTransactionsAndResumesFromStoredCheckpointThroughDefaultLiveAdapterPath()
      throws Exception {
    assumeDockerIsAvailable();

    try (MySQLContainer mysql =
        defaultMysqlContainer()) {
      mysql.start();
      configureReplicationUser(mysql);

      try (Connection connection =
          DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
        try (Statement statement = connection.createStatement()) {
          statement.execute("CREATE TABLE appdb.widgets (id BIGINT PRIMARY KEY, name VARCHAR(255))");
        }
      }

      TableSchema schema =
          TableSchema.create(
              new io.github.aandreakis.dblog.core.model.TableId("sourceA", "appdb", "widgets"),
              List.of(
                  new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                  new ColumnDefinition(
                      "name", "varchar(255)", NeutralColumnType.STRING, false, true)),
              Instant.parse("2026-04-11T00:00:00Z"));
      RelationalSourceConfig config =
          new RelationalSourceConfig(
              "sourceA",
              mysql.getJdbcUrl(),
              mysql.getUsername(),
              mysql.getPassword(),
              "appdb",
              List.of("appdb.widgets"),
              java.util.Map.of("mysql.serverId", "223355"),
              false);
      MySqlSourceAdapter adapter = new MySqlSourceAdapter();

      try (H2RuntimeStateStore stateStore =
          new H2RuntimeStateStore(tempDir.resolve("mysql-live-adapter-state"))) {
        OpenedSourceRuntime<?> firstOpen = adapter.openRuntime(config, stateStore, List.of(schema));
        try {
          assertThat(firstOpen.runtime()).isInstanceOf(MySqlLiveStreamingRuntime.class);
          assertThat(firstOpen.loadedCheckpointDisplayValue()).isNull();

          try (Connection writer =
              DriverManager.getConnection(
                  mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
            insertWidget(writer, 1L, "one");
          }

          MySqlLiveStreamingRuntime runtime = (MySqlLiveStreamingRuntime) firstOpen.runtime();
          MySqlBinlogTransaction firstTransaction = awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
          assertThat(firstTransaction.events()).hasSize(1);
          assertThat(firstTransaction.events().getFirst().afterRow().asMap())
              .containsEntry("id", 1L)
              .containsEntry("name", "one");
          runtime.acknowledge(firstTransaction);
          assertThat(runtime.lastAcknowledgedCheckpointDisplayValue()).isNotBlank();
        } finally {
          firstOpen.runtime().close();
        }
      }

      try (Connection writer =
          DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
        insertWidget(writer, 2L, "two");
      }

      try (H2RuntimeStateStore stateStore =
          new H2RuntimeStateStore(tempDir.resolve("mysql-live-adapter-state"))) {
        OpenedSourceRuntime<?> reopened = adapter.openRuntime(config, stateStore, List.of(schema));
        try {
          assertThat(reopened.runtime()).isInstanceOf(MySqlLiveStreamingRuntime.class);
          assertThat(reopened.loadedCheckpointDisplayValue()).isNotBlank();

          MySqlLiveStreamingRuntime runtime = (MySqlLiveStreamingRuntime) reopened.runtime();
          MySqlBinlogTransaction resumedTransaction =
              awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
          assertThat(resumedTransaction.events()).hasSize(1);
          assertThat(resumedTransaction.events().getFirst().afterRow().asMap())
              .containsEntry("id", 2L)
              .containsEntry("name", "two");
        } finally {
          reopened.runtime().close();
        }
      }
    }
  }

  @Test
  void replaysFirstTransactionAfterRestartBeforeAnyCheckpointIsAcknowledgedOnDefaultLivePath()
      throws Exception {
    assumeDockerIsAvailable();

    try (MySQLContainer mysql =
        defaultMysqlContainer()) {
      mysql.start();
      configureReplicationUser(mysql);

      try (Connection connection =
          DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
        try (Statement statement = connection.createStatement()) {
          statement.execute("CREATE TABLE appdb.widgets (id BIGINT PRIMARY KEY, name VARCHAR(255))");
        }
      }

      TableSchema schema =
          TableSchema.create(
              new io.github.aandreakis.dblog.core.model.TableId("sourceA", "appdb", "widgets"),
              List.of(
                  new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                  new ColumnDefinition(
                      "name", "varchar(255)", NeutralColumnType.STRING, false, true)),
              Instant.parse("2026-04-11T00:00:00Z"));
      RelationalSourceConfig config =
          new RelationalSourceConfig(
              "sourceA",
              mysql.getJdbcUrl(),
              mysql.getUsername(),
              mysql.getPassword(),
              "appdb",
              List.of("appdb.widgets"),
              java.util.Map.of("mysql.serverId", "223355"),
              false);
      MySqlSourceAdapter adapter = new MySqlSourceAdapter();

      try (H2RuntimeStateStore stateStore =
          new H2RuntimeStateStore(tempDir.resolve("mysql-live-adapter-replay-state"))) {
        OpenedSourceRuntime<?> firstOpen = adapter.openRuntime(config, stateStore, List.of(schema));
        try {
          try (Connection writer =
              DriverManager.getConnection(
                  mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
            insertWidget(writer, 1L, "one");
          }

          MySqlLiveStreamingRuntime runtime = (MySqlLiveStreamingRuntime) firstOpen.runtime();
          MySqlBinlogTransaction firstSeen = awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
          assertThat(firstSeen.events()).hasSize(1);
          assertThat(firstSeen.events().getFirst().afterRow().asMap())
              .containsEntry("id", 1L)
              .containsEntry("name", "one");
          assertThat(new MySqlSourceCheckpointStore(stateStore).load("sourceA")).isEmpty();
        } finally {
          firstOpen.runtime().close();
        }

        OpenedSourceRuntime<?> reopened = adapter.openRuntime(config, stateStore, List.of(schema));
        try {
          assertThat(reopened.loadedCheckpointDisplayValue()).isNull();

          MySqlLiveStreamingRuntime runtime = (MySqlLiveStreamingRuntime) reopened.runtime();
          MySqlBinlogTransaction replayed = awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
          assertThat(replayed.events()).hasSize(1);
          assertThat(replayed.events().getFirst().afterRow().asMap())
              .containsEntry("id", 1L)
              .containsEntry("name", "one");
          runtime.acknowledge(replayed);

          assertThat(new MySqlSourceCheckpointStore(stateStore).load("sourceA"))
              .contains(replayed.checkpointPosition());
        } finally {
          reopened.runtime().close();
        }
      }
    }
  }

  @Test
  void writesExplicitWatermarkWindowAndStreamsInternalWatermarkTransactionsOnDefaultLivePath()
      throws Exception {
    assumeDockerIsAvailable();

    try (MySQLContainer mysql =
        defaultMysqlContainer()) {
      mysql.start();
      configureReplicationUser(mysql);

      try (Connection connection =
          DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
        try (Statement statement = connection.createStatement()) {
          statement.execute("CREATE TABLE appdb.widgets (id BIGINT PRIMARY KEY, name VARCHAR(255))");
        }
      }

      TableSchema schema =
          TableSchema.create(
              new io.github.aandreakis.dblog.core.model.TableId("sourceA", "appdb", "widgets"),
              List.of(
                  new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                  new ColumnDefinition(
                      "name", "varchar(255)", NeutralColumnType.STRING, false, true)),
              Instant.parse("2026-04-11T00:00:00Z"));
      RelationalSourceConfig config =
          new RelationalSourceConfig(
              "sourceA",
              mysql.getJdbcUrl(),
              mysql.getUsername(),
              mysql.getPassword(),
              "appdb",
              List.of("appdb.widgets"),
              java.util.Map.of("mysql.serverId", "223355"),
              false);
      MySqlSourceAdapter adapter = new MySqlSourceAdapter();

      try (H2RuntimeStateStore stateStore =
          new H2RuntimeStateStore(tempDir.resolve("mysql-live-adapter-watermark-window"))) {
        MySqlSourceCheckpointStore checkpointStore = new MySqlSourceCheckpointStore(stateStore);
        OpenedSourceRuntime<?> opened = adapter.openRuntime(config, stateStore, List.of(schema));
        try {
          MySqlLiveStreamingRuntime runtime = (MySqlLiveStreamingRuntime) opened.runtime();
          var result =
              runtime.executeWithinWatermarkWindow(
                  (connection, window) -> {
                    assertThat(currentMySqlToken(connection)).isEqualTo(window.low().value());
                    return widgetCount(connection);
                  });

          assertThat(result.value()).isZero();
          assertThat(result.window().low()).isNotEqualTo(result.window().high());
          try (Connection observer =
              DriverManager.getConnection(
                  mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
            assertThat(currentMySqlToken(observer)).isEqualTo(result.window().high().value());
          }

          MySqlBinlogTransaction lowTransaction = awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
          assertThat(lowTransaction.events()).hasSize(1);
          assertThat(lowTransaction.events().getFirst().tableId())
              .isEqualTo(WatermarkMetadata.tableIdFor("sourceA"));
          assertThat(lowTransaction.events().getFirst().operationType())
              .isEqualTo(OperationType.WATERMARK);
          assertThat(lowTransaction.events().getFirst().captureOrigin()).isEqualTo(CaptureOrigin.LOG);
          assertThat(lowTransaction.events().getFirst().afterRow().asMap())
              .containsEntry(WatermarkMetadata.TOKEN_COLUMN, result.window().low().value());
          runtime.acknowledge(lowTransaction);

          MySqlBinlogTransaction highTransaction = awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
          assertThat(highTransaction.events()).hasSize(1);
          assertThat(highTransaction.events().getFirst().tableId())
              .isEqualTo(WatermarkMetadata.tableIdFor("sourceA"));
          assertThat(highTransaction.events().getFirst().operationType())
              .isEqualTo(OperationType.WATERMARK);
          assertThat(highTransaction.events().getFirst().captureOrigin())
              .isEqualTo(CaptureOrigin.LOG);
          assertThat(highTransaction.events().getFirst().afterRow().asMap())
              .containsEntry(WatermarkMetadata.TOKEN_COLUMN, result.window().high().value());
          runtime.acknowledge(highTransaction);

          assertThat(checkpointStore.load("sourceA")).contains(highTransaction.checkpointPosition());
        } finally {
          opened.runtime().close();
        }
      }
    }
  }

  @Test
  void preservesHighBitVarbinaryPayloadsExactlyOnDefaultLivePath() throws Exception {
    assumeDockerIsAvailable();

    try (MySQLContainer mysql =
        defaultMysqlContainer()) {
      mysql.start();
      configureReplicationUser(mysql);

      try (Connection connection =
          DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
        try (Statement statement = connection.createStatement()) {
          statement.execute(
              "CREATE TABLE appdb.binary_widgets (id BIGINT PRIMARY KEY, payload VARBINARY(255))");
        }
      }

      TableSchema schema =
          TableSchema.create(
              new io.github.aandreakis.dblog.core.model.TableId("sourceA", "appdb", "binary_widgets"),
              List.of(
                  new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                  new ColumnDefinition("payload", "varbinary(255)", NeutralColumnType.BINARY, false, true)),
              Instant.parse("2026-04-11T00:00:00Z"));
      RelationalSourceConfig config =
          new RelationalSourceConfig(
              "sourceA",
              mysql.getJdbcUrl(),
              mysql.getUsername(),
              mysql.getPassword(),
              "appdb",
              List.of("appdb.binary_widgets"),
              java.util.Map.of("mysql.serverId", "223355"),
              false);
      MySqlSourceAdapter adapter = new MySqlSourceAdapter();

      try (H2RuntimeStateStore stateStore =
              new H2RuntimeStateStore(tempDir.resolve("mysql-live-adapter-binary"));
          Connection writer =
              DriverManager.getConnection(
                  mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
          PreparedStatement insert =
              writer.prepareStatement(
                  "INSERT INTO appdb.binary_widgets (id, payload) VALUES (?, ?)")) {
        OpenedSourceRuntime<?> opened = adapter.openRuntime(config, stateStore, List.of(schema));
        try {
          byte[] expected = new byte[] {(byte) 0xAA, 0x01};
          insert.setLong(1, 1L);
          insert.setBytes(2, expected);
          insert.executeUpdate();

          MySqlLiveStreamingRuntime runtime = (MySqlLiveStreamingRuntime) opened.runtime();
          MySqlBinlogTransaction transaction = awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
          assertThat(transaction.events()).hasSize(1);
          assertThat((byte[]) transaction.events().getFirst().afterRow().get("payload"))
              .containsExactly(expected);
        } finally {
          opened.runtime().close();
        }
      }
    }
  }

  @Test
  void ignoresForeignRunWatermarkAndHeartbeatEventsAndContinuesStreamingOnDefaultLivePath()
      throws Exception {
    assumeDockerIsAvailable();

    try (MySQLContainer mysql =
        defaultMysqlContainer()) {
      mysql.start();
      configureReplicationUser(mysql);

      try (Connection connection =
          DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
        try (Statement statement = connection.createStatement()) {
          statement.execute("CREATE TABLE appdb.widgets (id BIGINT PRIMARY KEY, name VARCHAR(255))");
        }
      }

      TableSchema schema =
          TableSchema.create(
              new io.github.aandreakis.dblog.core.model.TableId("sourceA", "appdb", "widgets"),
              List.of(
                  new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                  new ColumnDefinition(
                      "name", "varchar(255)", NeutralColumnType.STRING, false, true)),
              Instant.parse("2026-04-11T00:00:00Z"));
      RelationalSourceConfig config =
          new RelationalSourceConfig(
              "sourceA",
              mysql.getJdbcUrl(),
              mysql.getUsername(),
              mysql.getPassword(),
              "appdb",
              List.of("appdb.widgets"),
              java.util.Map.of("mysql.serverId", "223355"),
              false);
      MySqlSourceAdapter adapter = new MySqlSourceAdapter();

      try (H2RuntimeStateStore stateStore =
          new H2RuntimeStateStore(tempDir.resolve("mysql-live-adapter-foreign-metadata"))) {
        OpenedSourceRuntime<?> opened = adapter.openRuntime(config, stateStore, List.of(schema));
        try {
          MySqlLiveStreamingRuntime runtime = (MySqlLiveStreamingRuntime) opened.runtime();
          runtime.executeWithinWatermarkWindow((connection, window) -> 0);
          runtime.acknowledge(awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT));
          runtime.acknowledge(awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT));

          assertThat(
                  runtime.emitHeartbeatIfDue(
                      Instant.parse("2026-03-21T00:04:00Z"), Duration.ofMillis(1)))
              .isTrue();
          MySqlBinlogTransaction heartbeatTransaction =
              awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
          assertThat(heartbeatTransaction.events()).hasSize(1);
          assertThat(heartbeatTransaction.events().getFirst().operationType())
              .isEqualTo(OperationType.HEARTBEAT);
          runtime.acknowledge(heartbeatTransaction);

          try (Connection writer =
                  DriverManager.getConnection(
                      mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
              Statement statement = writer.createStatement()) {
            statement.execute(
                "UPDATE dblog_meta.watermarks SET run_id = 'other-run', token = 'foreign-lw' WHERE id = 1");
            statement.execute(
                "UPDATE dblog_meta.heartbeats "
                    + "SET run_id = 'other-run', source_stream_id = 'other-stream', "
                    + "last_beat_at = '2026-03-21 00:05:00' WHERE id = 1");
          }

          assertThat(drainEvents(runtime, Duration.ofMillis(500))).isEmpty();

          try (Connection writer =
              DriverManager.getConnection(
                  mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
            insertWidget(writer, 1L, "one");
          }

          MySqlBinlogTransaction transaction = awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
          assertThat(transaction.events()).hasSize(1);
          assertThat(transaction.events().getFirst().tableId()).isEqualTo(schema.tableId());
          assertThat(transaction.events().getFirst().operationType()).isEqualTo(OperationType.INSERT);
        } finally {
          opened.runtime().close();
        }
      }
    }
  }

  @Test
  void failsClosedWhenForeignHeartbeatAppearsOnSameSourceStreamAfterOwnHeartbeatObservedOnDefaultLivePath()
      throws Exception {
    assumeDockerIsAvailable();

    try (MySQLContainer mysql =
        defaultMysqlContainer()) {
      mysql.start();
      configureReplicationUser(mysql);

      try (Connection connection =
          DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
        try (Statement statement = connection.createStatement()) {
          statement.execute("CREATE TABLE appdb.widgets (id BIGINT PRIMARY KEY, name VARCHAR(255))");
        }
      }

      TableSchema schema =
          TableSchema.create(
              new io.github.aandreakis.dblog.core.model.TableId("sourceA", "appdb", "widgets"),
              List.of(
                  new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                  new ColumnDefinition(
                      "name", "varchar(255)", NeutralColumnType.STRING, false, true)),
              Instant.parse("2026-04-11T00:00:00Z"));
      RelationalSourceConfig config =
          new RelationalSourceConfig(
              "sourceA",
              mysql.getJdbcUrl(),
              mysql.getUsername(),
              mysql.getPassword(),
              "appdb",
              List.of("appdb.widgets"),
              java.util.Map.of("mysql.serverId", "223355"),
              false);
      MySqlSourceAdapter adapter = new MySqlSourceAdapter();

      try (H2RuntimeStateStore stateStore =
          new H2RuntimeStateStore(tempDir.resolve("mysql-live-adapter-foreign-heartbeat"))) {
        OpenedSourceRuntime<?> opened = adapter.openRuntime(config, stateStore, List.of(schema));
        try {
          MySqlLiveStreamingRuntime runtime = (MySqlLiveStreamingRuntime) opened.runtime();
          assertThat(
                  runtime.emitHeartbeatIfDue(
                      Instant.parse("2026-03-21T00:06:00Z"), Duration.ofMillis(1)))
              .isTrue();
          MySqlBinlogTransaction heartbeatTransaction =
              awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
          assertThat(heartbeatTransaction.events()).hasSize(1);
          assertThat(heartbeatTransaction.events().getFirst().operationType())
              .isEqualTo(OperationType.HEARTBEAT);
          runtime.acknowledge(heartbeatTransaction);

          try (Connection writer =
                  DriverManager.getConnection(
                      mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
              Statement statement = writer.createStatement()) {
            statement.execute(
                "UPDATE dblog_meta.heartbeats SET run_id = 'other-run', "
                    + "source_stream_id = 'mysql-binlog', last_beat_at = '2026-03-21 00:07:00' "
                    + "WHERE id = 1");
          }

          assertThatThrownBy(() -> awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining("same source stream");
        } finally {
          opened.runtime().close();
        }
      }
    }
  }

  @Test
  void failsClosedWhenCheckpointedBinlogHistoryIsUnavailableBeforeReopen() throws Exception {
    assumeDockerIsAvailable();

    try (MySQLContainer mysql =
        defaultMysqlContainer()) {
      mysql.start();
      configureReplicationUser(mysql);

      try (Connection connection =
          DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
        try (Statement statement = connection.createStatement()) {
          statement.execute("CREATE TABLE appdb.widgets (id BIGINT PRIMARY KEY, name VARCHAR(255))");
        }
      }

      TableSchema schema =
          TableSchema.create(
              new io.github.aandreakis.dblog.core.model.TableId("sourceA", "appdb", "widgets"),
              List.of(
                  new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                  new ColumnDefinition(
                      "name", "varchar(255)", NeutralColumnType.STRING, false, true)),
              Instant.parse("2026-04-11T00:00:00Z"));
      RelationalSourceConfig config =
          new RelationalSourceConfig(
              "sourceA",
              mysql.getJdbcUrl(),
              mysql.getUsername(),
              mysql.getPassword(),
              "appdb",
              List.of("appdb.widgets"),
              java.util.Map.of("mysql.serverId", "223355"),
              false);
      MySqlSourceAdapter adapter = new MySqlSourceAdapter();

      try (H2RuntimeStateStore stateStore =
          new H2RuntimeStateStore(tempDir.resolve("mysql-live-adapter-missing-history"))) {
        MySqlSourceCheckpointStore checkpointStore = new MySqlSourceCheckpointStore(stateStore);

        OpenedSourceRuntime<?> firstOpen = adapter.openRuntime(config, stateStore, List.of(schema));
        io.github.aandreakis.dblog.adapter.mysql.MySqlSourcePosition checkpoint;
        try {
          try (Connection writer =
              DriverManager.getConnection(
                  mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
            insertWidget(writer, 1L, "one");
          }

          MySqlLiveStreamingRuntime runtime = (MySqlLiveStreamingRuntime) firstOpen.runtime();
          MySqlBinlogTransaction transaction = awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
          runtime.acknowledge(transaction);
          checkpoint = transaction.checkpointPosition();
          assertThat(checkpointStore.load("sourceA")).contains(checkpoint);
        } finally {
          firstOpen.runtime().close();
        }

        io.github.aandreakis.dblog.adapter.mysql.MySqlSourcePosition unavailableHistoryCheckpoint =
            new io.github.aandreakis.dblog.adapter.mysql.MySqlSourcePosition(
                checkpoint.binlogFilename() + ".missing",
                checkpoint.binlogPosition(),
                null);
        checkpointStore.save("sourceA", unavailableHistoryCheckpoint);

        assertThatThrownBy(() -> adapter.openRuntime(config, stateStore, List.of(schema)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("MySQL binlog history no longer contains required checkpoint file");
        assertThat(checkpointStore.load("sourceA")).contains(unavailableHistoryCheckpoint);
        assertThat(stateStore.schemas().loadFullDumpRequiredSignals())
            .anySatisfy(
                signal -> {
                  assertThat(signal.sourceId()).isEqualTo("sourceA");
                  assertThat(signal.tableId()).isNull();
                  assertThat(signal.reason()).contains("full dump required because data loss was detected");
                });
      }
    }
  }

  @Test
  void pausesAndResumesSourceFetchingWhenBoundedQueueBackpressureBuildsOnDefaultLivePath()
      throws Exception {
    assumeDockerIsAvailable();

    try (MySQLContainer mysql =
        defaultMysqlContainer()) {
      mysql.start();
      configureReplicationUser(mysql);

      try (Connection sqlConnection =
              DriverManager.getConnection(
                  mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
          Connection controlConnection =
              DriverManager.getConnection(
                  mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
        try (Statement statement = sqlConnection.createStatement()) {
          statement.execute("CREATE TABLE appdb.widgets (id BIGINT PRIMARY KEY, name VARCHAR(255))");
        }

        TableSchema schema =
            TableSchema.create(
                new io.github.aandreakis.dblog.core.model.TableId("sourceA", "appdb", "widgets"),
                List.of(
                    new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                    new ColumnDefinition(
                        "name", "varchar(255)", NeutralColumnType.STRING, false, true)),
                Instant.parse("2026-04-11T00:00:00Z"));
        RelationalSourceConfig config =
            new RelationalSourceConfig(
                "sourceA",
                mysql.getJdbcUrl(),
                mysql.getUsername(),
                mysql.getPassword(),
                "appdb",
                List.of("appdb.widgets"),
                java.util.Map.of(
                    "mysql.serverId", "223355",
                    "mysql.sourceEventQueueCapacity", "1"),
                false);
        MySqlSourceAdapter adapter = new MySqlSourceAdapter();

        try (H2RuntimeStateStore stateStore =
            new H2RuntimeStateStore(tempDir.resolve("mysql-live-adapter-backpressure"))) {
          OpenedSourceRuntime<?> opened = adapter.openRuntime(config, stateStore, List.of(schema));
          try {
            MySqlLiveStreamingRuntime runtime = (MySqlLiveStreamingRuntime) opened.runtime();
            for (long id = 1L; id <= 60L; id++) {
              insertWidget(controlConnection, id, "bulk-" + id);
            }

            awaitSourcePause(runtime);
            assertThat(runtime.sourceFlowControlSnapshot().queueFull()).isTrue();
            assertThat(runtime.sourceFlowControlSnapshot().queueCapacity()).isEqualTo(1);

            List<MySqlBinlogTransaction> drained = new ArrayList<>();
            drainUntilIdleAndResumed(runtime, drained);
            assertThat(drained).isNotEmpty();

            insertWidget(controlConnection, 201L, "post-drain");
            MySqlBinlogTransaction resumedTransaction =
                awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
            assertThat(resumedTransaction.events()).hasSize(1);
            assertThat(resumedTransaction.events().getFirst().afterRow().asMap())
                .containsEntry("id", 201L)
                .containsEntry("name", "post-drain");
            runtime.acknowledge(resumedTransaction);

            assertThat(runtime.sourceFlowControlSnapshot().sourceFetchPaused()).isFalse();
            assertThat(runtime.sourceFlowControlSnapshot().totalPauseCount()).isGreaterThanOrEqualTo(1L);
          } finally {
            opened.runtime().close();
          }
        }
      }
    }
  }

  @Test
  void surfacesVeryLargeSingleCommittedTransactionOnDefaultLivePath() throws Exception {
    assumeDockerIsAvailable();

    try (MySQLContainer mysql =
        defaultMysqlContainer()) {
      mysql.start();
      configureReplicationUser(mysql);

      try (Connection sqlConnection =
              DriverManager.getConnection(
                  mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
          Connection writerConnection =
              DriverManager.getConnection(
                  mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
        try (Statement statement = sqlConnection.createStatement()) {
          statement.execute("CREATE TABLE appdb.widgets (id BIGINT PRIMARY KEY, name VARCHAR(255))");
        }

        TableSchema schema =
            TableSchema.create(
                new io.github.aandreakis.dblog.core.model.TableId("sourceA", "appdb", "widgets"),
                List.of(
                    new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                    new ColumnDefinition(
                        "name", "varchar(255)", NeutralColumnType.STRING, false, true)),
                Instant.parse("2026-04-11T00:00:00Z"));
        RelationalSourceConfig config =
            new RelationalSourceConfig(
                "sourceA",
                mysql.getJdbcUrl(),
                mysql.getUsername(),
                mysql.getPassword(),
                "appdb",
                List.of("appdb.widgets"),
                java.util.Map.of("mysql.serverId", "223355"),
                false);
        MySqlSourceAdapter adapter = new MySqlSourceAdapter();

        try (H2RuntimeStateStore stateStore =
            new H2RuntimeStateStore(tempDir.resolve("mysql-live-adapter-large-tx"))) {
          OpenedSourceRuntime<?> opened = adapter.openRuntime(config, stateStore, List.of(schema));
          try {
            writerConnection.setAutoCommit(false);
            try (Statement statement = writerConnection.createStatement()) {
              for (int id = 1; id <= 200; id++) {
                statement.addBatch(
                    "INSERT INTO appdb.widgets (id, name) VALUES (" + id + ", 'bulk-" + id + "')");
              }
              statement.executeBatch();
              writerConnection.commit();
            }

            MySqlLiveStreamingRuntime runtime = (MySqlLiveStreamingRuntime) opened.runtime();
            MySqlBinlogTransaction transaction = awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
            assertThat(transaction.events()).hasSize(200);
            assertThat(transaction.events())
                .allSatisfy(
                    event -> {
                      assertThat(event.tableId()).isEqualTo(schema.tableId());
                      assertThat(event.operationType()).isEqualTo(OperationType.INSERT);
                    });
            assertThat(
                    transaction.events().stream()
                        .map(event -> String.valueOf(event.primaryKey().get("id")))
                        .distinct()
                        .count())
                .isEqualTo(200L);
          } finally {
            opened.runtime().close();
          }
        }
      }
    }
  }

  @Test
  void surfacesManySmallCommittedTransactionsIndividuallyOnDefaultLivePath() throws Exception {
    assumeDockerIsAvailable();

    try (MySQLContainer mysql =
        defaultMysqlContainer()) {
      mysql.start();
      configureReplicationUser(mysql);

      try (Connection sqlConnection =
              DriverManager.getConnection(
                  mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
          Connection writerConnection =
              DriverManager.getConnection(
                  mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
        configureSqlConnection(writerConnection);
        try (Statement statement = sqlConnection.createStatement()) {
          statement.execute("CREATE TABLE appdb.widgets (id BIGINT PRIMARY KEY, name VARCHAR(255))");
        }

        TableSchema schema =
            TableSchema.create(
                new io.github.aandreakis.dblog.core.model.TableId("sourceA", "appdb", "widgets"),
                List.of(
                    new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                    new ColumnDefinition(
                        "name", "varchar(255)", NeutralColumnType.STRING, false, true)),
                Instant.parse("2026-04-11T00:00:00Z"));
        RelationalSourceConfig config =
            new RelationalSourceConfig(
                "sourceA",
                mysql.getJdbcUrl(),
                mysql.getUsername(),
                mysql.getPassword(),
                "appdb",
                List.of("appdb.widgets"),
                java.util.Map.of("mysql.serverId", "223355"),
                false);
        MySqlSourceAdapter adapter = new MySqlSourceAdapter();

        try (H2RuntimeStateStore stateStore =
            new H2RuntimeStateStore(tempDir.resolve("mysql-live-adapter-many-small"))) {
          OpenedSourceRuntime<?> opened = adapter.openRuntime(config, stateStore, List.of(schema));
          try {
            MySqlLiveStreamingRuntime runtime = (MySqlLiveStreamingRuntime) opened.runtime();
            for (int id = 1; id <= 25; id++) {
              insertWidget(writerConnection, id, "small-" + id);
            }

            for (int id = 1; id <= 25; id++) {
              MySqlBinlogTransaction transaction = awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
              assertThat(transaction.events()).hasSize(1);
              assertThat(transaction.events().getFirst().tableId()).isEqualTo(schema.tableId());
              assertThat(transaction.events().getFirst().operationType()).isEqualTo(OperationType.INSERT);
              assertThat(String.valueOf(transaction.events().getFirst().primaryKey().get("id")))
                  .isEqualTo(Integer.toString(id));
              runtime.acknowledge(transaction);
            }
          } finally {
            opened.runtime().close();
          }
        }
      }
    }
  }

  @Test
  void failsClosedWhenBinlogConnectionIsKilledFromTheServerSideOnDefaultLivePath()
      throws Exception {
    assumeDockerIsAvailable();

    try (MySQLContainer mysql =
        defaultMysqlContainer()) {
      mysql.start();
      configureReplicationUser(mysql);

      try (Connection sqlConnection =
              DriverManager.getConnection(
                  mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
          Connection controlConnection =
              DriverManager.getConnection(
                  mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
        configureSqlConnection(sqlConnection);
        configureSqlConnection(controlConnection);
        try (Statement statement = sqlConnection.createStatement()) {
          statement.execute("CREATE TABLE appdb.widgets (id BIGINT PRIMARY KEY, name VARCHAR(255))");
        }

        TableSchema schema =
            TableSchema.create(
                new io.github.aandreakis.dblog.core.model.TableId("sourceA", "appdb", "widgets"),
                List.of(
                    new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                    new ColumnDefinition(
                        "name", "varchar(255)", NeutralColumnType.STRING, false, true)),
                Instant.parse("2026-04-11T00:00:00Z"));
        RelationalSourceConfig config =
            new RelationalSourceConfig(
                "sourceA",
                mysql.getJdbcUrl(),
                mysql.getUsername(),
                mysql.getPassword(),
                "appdb",
                List.of("appdb.widgets"),
                java.util.Map.of("mysql.serverId", "223355"),
                false);
        MySqlSourceAdapter adapter = new MySqlSourceAdapter();

        try (H2RuntimeStateStore stateStore =
            new H2RuntimeStateStore(tempDir.resolve("mysql-live-adapter-kill-binlog"))) {
          OpenedSourceRuntime<?> opened = adapter.openRuntime(config, stateStore, List.of(schema));
          try {
            MySqlLiveStreamingRuntime runtime = (MySqlLiveStreamingRuntime) opened.runtime();

            insertWidget(controlConnection, 1L, "one");
            MySqlBinlogTransaction firstTransaction =
                awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
            runtime.acknowledge(firstTransaction);

            killOwnBinlogDumpConnection(controlConnection);
            insertWidget(controlConnection, 2L, "two");

            awaitBinlogFailure(runtime);
          } finally {
            opened.runtime().close();
          }
        }
      }
    }
  }

  @Test
  void applicationRuntimeRecoversHeartbeatSqlConnectionWhenRetryIsEnabled() throws Exception {
    assumeDockerIsAvailable();

    try (MySQLContainer mysql =
        defaultMysqlContainer()) {
      mysql.start();
      configureReplicationUser(mysql);

      try (Connection setupConnection =
          DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
        configureSqlConnection(setupConnection);
        try (Statement statement = setupConnection.createStatement()) {
          statement.execute("CREATE TABLE appdb.widgets (id BIGINT PRIMARY KEY, name VARCHAR(255))");
        }
      }

      RelationalSourceConfig config =
          new RelationalSourceConfig(
              "sourceA",
              mysql.getJdbcUrl(),
              mysql.getUsername(),
              mysql.getPassword(),
              "appdb",
              List.of("appdb.widgets"),
              java.util.Map.of(
                  "mysql.serverId", "223355",
                  "mysql.retryLogConnectionLoss", "true",
                  "mysql.reconnectBackoff", "PT0.1S"),
              false);
      MySqlSourceAdapter adapter = new MySqlSourceAdapter();

      try (H2RuntimeStateStore stateStore =
              new H2RuntimeStateStore(tempDir.resolve("mysql-application-heartbeat-retry"));
          DbLogApplication<?> application =
              DbLogApplication.open(
                  adapter,
                  config,
                  stateStore,
                  null,
                  NoOpChangeEventSink.instance(),
                  10,
                  new SimpleMeterRegistry(),
                  DbLogApplication.ControlPlaneEventCaptureOptions.disabled());
          Connection controlConnection =
              DriverManager.getConnection(
                  mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
        configureSqlConnection(controlConnection);
        SourceRuntime<?> runtime = application.stack().session().runtime();

        killOwnSqlConnection(controlConnection);

        assertThatCode(
                () ->
                    runtime.emitHeartbeatIfDue(
                        Instant.parse("2026-03-21T00:08:00Z"), Duration.ofMillis(1)))
            .doesNotThrowAnyException();
      }
    }
  }

  private static void configureReplicationUser(MySQLContainer mysql) throws Exception {
    MySqlTestUserGrants.applyDblogUserGrants(mysql, "appdb");
  }

  private static MySQLContainer defaultMysqlContainer() {
    return sharedMysql;
  }

  private static MySQLContainer configureDefaultMysqlContainer(MySQLContainer mysql) {
    return LiveMySqlTestContainers.configureLiveCdcContainer(mysql);
  }

  private static String rootJdbcUrl(MySQLContainer mysql) {
    return "jdbc:mysql://"
        + mysql.getHost()
        + ":"
        + mysql.getMappedPort(MySQLContainer.MYSQL_PORT)
        + "/mysql";
  }

  private static final class SharedMySQLContainer extends MySQLContainer {
    private SharedMySQLContainer(DockerImageName imageName) {
      super(imageName);
      configureDefaultMysqlContainer(this);
    }

    @Override
    public void close() {}

    private void stopShared() {
      super.close();
    }
  }

  private static void insertWidget(Connection connection, long id, String name) throws Exception {
    try (Statement statement = connection.createStatement()) {
      statement.execute(
          "INSERT INTO appdb.widgets (id, name) VALUES (" + id + ", '" + name + "')");
    }
  }


  private static long widgetCount(Connection connection) throws java.sql.SQLException {
    try (Statement statement = connection.createStatement();
        var resultSet = statement.executeQuery("SELECT COUNT(*) FROM appdb.widgets")) {
      assertThat(resultSet.next()).isTrue();
      return resultSet.getLong(1);
    }
  }

  private static String currentMySqlToken(Connection connection) throws java.sql.SQLException {
    try (Statement statement = connection.createStatement();
        var resultSet =
            statement.executeQuery("SELECT token FROM dblog_meta.watermarks WHERE id = 1")) {
      assertThat(resultSet.next()).isTrue();
      return resultSet.getString(1);
    }
  }

  private static void killOwnBinlogDumpConnection(Connection connection) throws Exception {
    try (Statement statement = connection.createStatement();
        var resultSet = statement.executeQuery("SHOW PROCESSLIST")) {
      while (resultSet.next()) {
        String user = resultSet.getString("User");
        String command = resultSet.getString("Command");
        long id = resultSet.getLong("Id");
        if ("dblog".equalsIgnoreCase(user)
            && command != null
            && command.toLowerCase().startsWith("binlog dump")) {
          statement.execute("KILL " + id);
          return;
        }
      }
    }
    throw new AssertionError("Did not find a MySQL binlog dump connection to kill");
  }

  private static void killOwnSqlConnection(Connection connection) throws Exception {
    long currentConnectionId = currentConnectionId(connection);
    try (Statement query = connection.createStatement();
        var resultSet = query.executeQuery("SHOW PROCESSLIST")) {
      while (resultSet.next()) {
        String user = resultSet.getString("User");
        String command = resultSet.getString("Command");
        long id = resultSet.getLong("Id");
        if ("dblog".equalsIgnoreCase(user)
            && id != currentConnectionId
            && (command == null || !command.toLowerCase().startsWith("binlog dump"))) {
          try (Statement kill = connection.createStatement()) {
            kill.execute("KILL " + id);
          }
          return;
        }
      }
    }
    throw new AssertionError("Did not find a MySQL SQL connection to kill");
  }

  private static long currentConnectionId(Connection connection) throws Exception {
    try (Statement statement = connection.createStatement();
        var resultSet = statement.executeQuery("SELECT CONNECTION_ID()")) {
      assertThat(resultSet.next()).isTrue();
      return resultSet.getLong(1);
    }
  }

  private static void awaitBinlogFailure(MySqlLiveStreamingRuntime runtime) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    while (System.nanoTime() < deadline) {
      try {
        runtime.readPendingTransaction();
      } catch (java.sql.SQLTransientConnectionException ex) {
        assertThat(ex.getMessage()).contains("disconnected unexpectedly");
        return;
      }
      Thread.sleep(50L);
    }
    throw new AssertionError(
        "Timed out waiting for the MySQL live runtime to fail closed after killing the binlog connection");
  }

  private static void awaitSourcePause(MySqlLiveStreamingRuntime runtime) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
    while (System.nanoTime() < deadline) {
      if (runtime.sourceFlowControlSnapshot().sourceFetchPaused()) {
        return;
      }
      Thread.sleep(25L);
    }
    throw new AssertionError("Timed out waiting for MySQL runtime source backpressure pause");
  }

  private static void drainUntilIdleAndResumed(
      MySqlLiveStreamingRuntime runtime, List<MySqlBinlogTransaction> drained) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
    long idleSince = -1L;
    while (System.nanoTime() < deadline) {
      var maybeTransaction = runtime.readPendingTransaction();
      if (maybeTransaction.isPresent()) {
        MySqlBinlogTransaction transaction = maybeTransaction.orElseThrow();
        drained.add(transaction);
        runtime.acknowledge(transaction);
        idleSince = -1L;
        continue;
      }

      if (runtime.sourceFlowControlSnapshot().sourceFetchPaused()
          || runtime.sourceFlowControlSnapshot().queueDepth() > 0) {
        idleSince = -1L;
        Thread.sleep(25L);
        continue;
      }

      if (idleSince < 0L) {
        idleSince = System.nanoTime();
      } else if (System.nanoTime() - idleSince >= Duration.ofMillis(250).toNanos()) {
        return;
      }
      Thread.sleep(25L);
    }
    throw new AssertionError(
        "Timed out waiting for MySQL runtime to drain backlog and resume steady-state streaming");
  }

  private static void configureSqlConnection(Connection connection) throws Exception {
    connection.setAutoCommit(true);
    connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
  }

}
