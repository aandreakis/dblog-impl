package io.github.aandreakis.dblog.integration.adapter.postgres;

import static io.github.aandreakis.dblog.integration.adapter.LiveRuntimeTestSupport.TRANSACTION_AWAIT_TIMEOUT;
import static io.github.aandreakis.dblog.integration.adapter.LiveRuntimeTestSupport.awaitTransaction;
import static io.github.aandreakis.dblog.integration.adapter.LiveRuntimeTestSupport.drainEvents;
import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.adapter.api.OpenedSourceRuntime;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.postgres.PostgresLiveStreamingRuntime;
import io.github.aandreakis.dblog.adapter.postgres.PostgresPgoutputTransaction;
import io.github.aandreakis.dblog.adapter.postgres.PostgresSourceAdapter;
import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.reconcile.WatermarkMetadata;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import io.github.aandreakis.dblog.testsupport.LivePostgresTestContainers;
import io.github.aandreakis.dblog.testsupport.PostgresTestUserGrants;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@Tag("integration-docker")
class PostgresSourceAdapterLiveRuntimeIT {
  private static final String DATABASE_NAME = "appdb";
  private static final String SOURCE_ID = "sourceA";
  private static final String RUNTIME_USERNAME = PostgresTestUserGrants.RUNTIME_USERNAME;
  private static final String RUNTIME_PASSWORD = PostgresTestUserGrants.RUNTIME_PASSWORD;

  private static SharedPostgreSQLContainer sharedPostgres;

  @TempDir Path tempDir;

  @BeforeAll
  static void startSharedPostgres() {
    assumeDockerIsAvailable();
    sharedPostgres = new SharedPostgreSQLContainer(DockerImageName.parse("postgres:18"));
    sharedPostgres.start();
  }

  @AfterAll
  static void stopSharedPostgres() {
    if (sharedPostgres != null) {
      sharedPostgres.stopShared();
    }
  }

  @BeforeEach
  void resetSharedPostgres() throws Exception {
    if (sharedPostgres == null || !sharedPostgres.isRunning()) {
      return;
    }
    try (Connection connection = openAdminConnection(sharedPostgres);
        Statement statement = connection.createStatement()) {
      configureSqlConnection(connection);
      dropReplicationSlots(connection);
      for (String publicationName :
          scalarStrings(connection, "SELECT pubname FROM pg_catalog.pg_publication")) {
        statement.execute("DROP PUBLICATION IF EXISTS " + quoteIdentifier(publicationName));
      }
      statement.execute("DROP SCHEMA IF EXISTS dblog_meta CASCADE");
      statement.execute("DROP SCHEMA IF EXISTS public CASCADE");
      statement.execute("CREATE SCHEMA public");
    }
  }

  @Test
  void managedRuntimeWorksWithDedicatedPostgresRole() throws Exception {
    assumeDockerIsAvailable();

    try (PostgreSQLContainer postgres = postgresContainer()) {
      postgres.start();
      configurePostgresRuntimeUser(postgres);
      createRuntimeOwnedWidgetsTable(postgres);

      try (Connection dblogSetup = openRuntimeConnection(postgres);
          Statement statement = dblogSetup.createStatement()) {
        statement.execute(
            "CREATE PUBLICATION runtime_pub_dedicated FOR TABLE public.widgets "
                + "WITH (publish = 'insert, update, delete, truncate')");
      }

      TableSchema schema = widgetsSchema();
      RelationalSourceConfig config =
          sourceConfig(postgres, "runtime_pub_dedicated", "runtime_slot_dedicated");
      PostgresSourceAdapter adapter = new PostgresSourceAdapter();

      try (H2RuntimeStateStore stateStore =
          new H2RuntimeStateStore(tempDir.resolve("postgres-live-dedicated-role"))) {
        OpenedSourceRuntime<?> opened = adapter.openRuntime(config, stateStore, List.of(schema));
        try {
          assertThat(opened.runtime()).isInstanceOf(PostgresLiveStreamingRuntime.class);

          try (Connection observer = openAdminConnection(postgres);
              Statement statement = observer.createStatement();
              ResultSet publication =
                  statement.executeQuery(
                      "SELECT pg_get_userbyid(pubowner), pubinsert, pubupdate, pubdelete, pubtruncate "
                          + "FROM pg_catalog.pg_publication "
                          + "WHERE pubname = 'runtime_pub_dedicated'")) {
            assertThat(publication.next()).isTrue();
            assertThat(publication.getString(1)).isEqualTo(RUNTIME_USERNAME);
            assertThat(publication.getBoolean(2)).isTrue();
            assertThat(publication.getBoolean(3)).isTrue();
            assertThat(publication.getBoolean(4)).isTrue();
            assertThat(publication.getBoolean(5)).isFalse();
          }

          try (Connection observer = openAdminConnection(postgres)) {
            assertThat(tableOwner(observer, "public", "widgets")).isEqualTo(RUNTIME_USERNAME);
            assertThat(tableOwner(observer, "dblog_meta", "watermarks")).isEqualTo(RUNTIME_USERNAME);
            assertThat(tableOwner(observer, "dblog_meta", "heartbeats")).isEqualTo(RUNTIME_USERNAME);
            assertThat(publicationTables(observer, "runtime_pub_dedicated"))
                .containsExactly(
                    "dblog_meta.heartbeats", "dblog_meta.watermarks", "public.widgets");
            assertThat(slotCount(observer, "runtime_slot_dedicated")).isEqualTo(1);
          }

          try (Connection writer = openRuntimeConnection(postgres)) {
            insertWidget(writer, 1L, "one");
          }

          PostgresLiveStreamingRuntime runtime = (PostgresLiveStreamingRuntime) opened.runtime();
          PostgresPgoutputTransaction transaction =
              awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
          assertThat(transaction.events()).hasSize(1);
          assertThat(transaction.events().getFirst().afterRow().asMap())
              .containsEntry("id", 1L)
              .containsEntry("name", "one");
          runtime.acknowledge(transaction);
        } finally {
          opened.runtime().close();
        }
      }
    }
  }

  @Test
  void streamsCommittedTransactionsAndResumesFromStoredCheckpointThroughDefaultLiveAdapterPath()
      throws Exception {
    assumeDockerIsAvailable();

    try (PostgreSQLContainer postgres = postgresContainer()) {
      postgres.start();
      configurePostgresRuntimeUser(postgres);
      createRuntimeOwnedWidgetsTable(postgres);

      TableSchema schema = widgetsSchema();
      RelationalSourceConfig config = sourceConfig(postgres, "runtime_pub", "runtime_slot");
      PostgresSourceAdapter adapter = new PostgresSourceAdapter();

      try (H2RuntimeStateStore stateStore =
          new H2RuntimeStateStore(tempDir.resolve("postgres-live-adapter-state"))) {
        OpenedSourceRuntime<?> firstOpen = adapter.openRuntime(config, stateStore, List.of(schema));
        try {
          assertThat(firstOpen.runtime()).isInstanceOf(PostgresLiveStreamingRuntime.class);
          assertThat(firstOpen.loadedCheckpointDisplayValue()).isNull();
          try (Connection observer = openAdminConnection(postgres);
              Statement publicationStatement = observer.createStatement();
              ResultSet publication =
                  publicationStatement.executeQuery(
                      "SELECT COUNT(*) FROM pg_catalog.pg_publication WHERE pubname = 'runtime_pub'");
              Statement slotStatement = observer.createStatement();
              ResultSet slot =
                  slotStatement.executeQuery(
                      "SELECT COUNT(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = 'runtime_slot'")) {
            assertThat(publication.next()).isTrue();
            assertThat(publication.getInt(1)).isEqualTo(1);
            assertThat(slot.next()).isTrue();
            assertThat(slot.getInt(1)).isEqualTo(1);
          }

          try (Connection writer = openRuntimeConnection(postgres)) {
            insertWidget(writer, 1L, "one");
          }

          PostgresLiveStreamingRuntime runtime =
              (PostgresLiveStreamingRuntime) firstOpen.runtime();
          PostgresPgoutputTransaction firstTransaction =
              awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
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

      try (Connection writer = openRuntimeConnection(postgres)) {
        insertWidget(writer, 2L, "two");
      }

      try (H2RuntimeStateStore stateStore =
          new H2RuntimeStateStore(tempDir.resolve("postgres-live-adapter-state"))) {
        OpenedSourceRuntime<?> reopened = adapter.openRuntime(config, stateStore, List.of(schema));
        try {
          assertThat(reopened.runtime()).isInstanceOf(PostgresLiveStreamingRuntime.class);
          assertThat(reopened.loadedCheckpointDisplayValue()).isNotBlank();

          PostgresLiveStreamingRuntime runtime =
              (PostgresLiveStreamingRuntime) reopened.runtime();
          PostgresPgoutputTransaction resumedTransaction =
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
  void openRuntimeRecreatesManagedPublicationAndSlotAfterOutOfBandDrop() throws Exception {
    assumeDockerIsAvailable();

    try (PostgreSQLContainer postgres = postgresContainer()) {
      postgres.start();
      configurePostgresRuntimeUser(postgres);
      createRuntimeOwnedWidgetsTable(postgres);

      TableSchema schema = widgetsSchema();
      RelationalSourceConfig config =
          sourceConfig(postgres, "runtime_pub_recreate", "runtime_slot_recreate");
      PostgresSourceAdapter adapter = new PostgresSourceAdapter();

      try (H2RuntimeStateStore stateStore =
          new H2RuntimeStateStore(tempDir.resolve("postgres-live-adapter-recreate-resources"))) {
        OpenedSourceRuntime<?> firstOpen = adapter.openRuntime(config, stateStore, List.of(schema));
        try {
          assertThat(firstOpen.runtime()).isInstanceOf(PostgresLiveStreamingRuntime.class);
        } finally {
          firstOpen.runtime().close();
        }

        try (Connection admin = openAdminConnection(postgres);
            Statement statement = admin.createStatement()) {
          awaitSlotInactive(admin, "runtime_slot_recreate");
          statement.execute("DROP PUBLICATION runtime_pub_recreate");
          statement.execute("SELECT pg_drop_replication_slot('runtime_slot_recreate')");
          assertThat(publicationTables(admin, "runtime_pub_recreate")).isEmpty();
          assertThat(slotCount(admin, "runtime_slot_recreate")).isZero();
        }

        OpenedSourceRuntime<?> reopened = adapter.openRuntime(config, stateStore, List.of(schema));
        try {
          assertThat(reopened.runtime()).isInstanceOf(PostgresLiveStreamingRuntime.class);
          try (Connection observer = openAdminConnection(postgres)) {
            assertThat(publicationTables(observer, "runtime_pub_recreate"))
                .containsExactly(
                    "dblog_meta.heartbeats", "dblog_meta.watermarks", "public.widgets");
            assertThat(slotCount(observer, "runtime_slot_recreate")).isEqualTo(1);
          }
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

    try (PostgreSQLContainer postgres = postgresContainer()) {
      postgres.start();
      configurePostgresRuntimeUser(postgres);
      createRuntimeOwnedWidgetsTable(postgres);

      TableSchema schema = widgetsSchema();
      RelationalSourceConfig config =
          sourceConfig(postgres, "runtime_pub_replay", "runtime_slot_replay");
      PostgresSourceAdapter adapter = new PostgresSourceAdapter();

      try (H2RuntimeStateStore stateStore =
          new H2RuntimeStateStore(tempDir.resolve("postgres-live-adapter-replay-state"))) {
        OpenedSourceRuntime<?> firstOpen = adapter.openRuntime(config, stateStore, List.of(schema));
        try {
          try (Connection writer = openRuntimeConnection(postgres)) {
            insertWidget(writer, 1L, "one");
          }

          PostgresLiveStreamingRuntime runtime =
              (PostgresLiveStreamingRuntime) firstOpen.runtime();
          PostgresPgoutputTransaction firstSeen =
              awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
          assertThat(firstSeen.events()).hasSize(1);
          assertThat(firstSeen.events().getFirst().afterRow().asMap())
              .containsEntry("id", 1L)
              .containsEntry("name", "one");
          assertThat(
                  new io.github.aandreakis.dblog.adapter.postgres.PostgresSourceCheckpointStore(
                      stateStore)
                      .load(SOURCE_ID))
              .isEmpty();
        } finally {
          firstOpen.runtime().close();
        }

        OpenedSourceRuntime<?> reopened = adapter.openRuntime(config, stateStore, List.of(schema));
        try {
          assertThat(reopened.loadedCheckpointDisplayValue()).isNull();

          PostgresLiveStreamingRuntime runtime =
              (PostgresLiveStreamingRuntime) reopened.runtime();
          PostgresPgoutputTransaction replayed =
              awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
          assertThat(replayed.events()).hasSize(1);
          assertThat(replayed.events().getFirst().afterRow().asMap())
              .containsEntry("id", 1L)
              .containsEntry("name", "one");
          runtime.acknowledge(replayed);

          assertThat(
                  new io.github.aandreakis.dblog.adapter.postgres.PostgresSourceCheckpointStore(
                      stateStore)
                      .load(SOURCE_ID))
              .contains(replayed.checkpointLsn());
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

    try (PostgreSQLContainer postgres = postgresContainer()) {
      postgres.start();
      configurePostgresRuntimeUser(postgres);
      createRuntimeOwnedWidgetsTable(postgres);

      TableSchema schema = widgetsSchema();
      RelationalSourceConfig config =
          sourceConfig(postgres, "runtime_pub_window", "runtime_slot_window");
      PostgresSourceAdapter adapter = new PostgresSourceAdapter();

      try (H2RuntimeStateStore stateStore =
          new H2RuntimeStateStore(tempDir.resolve("postgres-live-adapter-watermark-window"))) {
        io.github.aandreakis.dblog.adapter.postgres.PostgresSourceCheckpointStore checkpointStore =
            new io.github.aandreakis.dblog.adapter.postgres.PostgresSourceCheckpointStore(stateStore);
        OpenedSourceRuntime<?> opened = adapter.openRuntime(config, stateStore, List.of(schema));
        try {
          PostgresLiveStreamingRuntime runtime = (PostgresLiveStreamingRuntime) opened.runtime();
          var result =
              runtime.executeWithinWatermarkWindow(
                  (connection, window) -> {
                    assertThat(currentPostgresToken(connection)).isEqualTo(window.low().value());
                    return widgetCount(connection);
                  });

          assertThat(result.value()).isZero();
          assertThat(result.window().low()).isNotEqualTo(result.window().high());
          try (Connection observer = openRuntimeConnection(postgres)) {
            assertThat(currentPostgresToken(observer)).isEqualTo(result.window().high().value());
          }

          PostgresPgoutputTransaction lowTransaction =
              awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
          assertThat(lowTransaction.events()).hasSize(1);
          assertThat(lowTransaction.events().getFirst().tableId())
              .isEqualTo(WatermarkMetadata.tableIdFor(DATABASE_NAME));
          assertThat(lowTransaction.events().getFirst().operationType())
              .isEqualTo(OperationType.WATERMARK);
          assertThat(lowTransaction.events().getFirst().captureOrigin())
              .isEqualTo(CaptureOrigin.LOG);
          assertThat(lowTransaction.events().getFirst().afterRow().asMap())
              .containsEntry(WatermarkMetadata.TOKEN_COLUMN, result.window().low().value());
          runtime.acknowledge(lowTransaction);

          PostgresPgoutputTransaction highTransaction =
              awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
          assertThat(highTransaction.events()).hasSize(1);
          assertThat(highTransaction.events().getFirst().tableId())
              .isEqualTo(WatermarkMetadata.tableIdFor(DATABASE_NAME));
          assertThat(highTransaction.events().getFirst().operationType())
              .isEqualTo(OperationType.WATERMARK);
          assertThat(highTransaction.events().getFirst().captureOrigin())
              .isEqualTo(CaptureOrigin.LOG);
          assertThat(highTransaction.events().getFirst().afterRow().asMap())
              .containsEntry(WatermarkMetadata.TOKEN_COLUMN, result.window().high().value());
          runtime.acknowledge(highTransaction);

          assertThat(checkpointStore.load(SOURCE_ID)).contains(highTransaction.checkpointLsn());
        } finally {
          opened.runtime().close();
        }
      }
    }
  }

  @Test
  void ignoresForeignRunWatermarkEventsAndContinuesStreamingOnDefaultLivePath() throws Exception {
    assumeDockerIsAvailable();

    try (PostgreSQLContainer postgres = postgresContainer()) {
      postgres.start();
      configurePostgresRuntimeUser(postgres);
      createRuntimeOwnedWidgetsTable(postgres);

      TableSchema schema = widgetsSchema();
      RelationalSourceConfig config =
          sourceConfig(postgres, "runtime_pub_foreign", "runtime_slot_foreign");
      PostgresSourceAdapter adapter = new PostgresSourceAdapter();

      try (H2RuntimeStateStore stateStore =
          new H2RuntimeStateStore(tempDir.resolve("postgres-live-adapter-foreign-watermark"))) {
        OpenedSourceRuntime<?> opened = adapter.openRuntime(config, stateStore, List.of(schema));
        try {
          PostgresLiveStreamingRuntime runtime = (PostgresLiveStreamingRuntime) opened.runtime();
          runtime.executeWithinWatermarkWindow((connection, window) -> 0);
          runtime.acknowledge(awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT));
          runtime.acknowledge(awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT));

          try (Connection writer = openRuntimeConnection(postgres);
              Statement statement = writer.createStatement()) {
            statement.execute(
                "UPDATE dblog_meta.watermarks SET run_id = 'other-run', token = 'foreign-lw' WHERE id = 1");
          }

          assertThat(drainEvents(runtime, Duration.ofMillis(500))).isEmpty();

          try (Connection writer = openRuntimeConnection(postgres)) {
            insertWidget(writer, 1L, "one");
          }

          PostgresPgoutputTransaction transaction =
              awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
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

    try (PostgreSQLContainer postgres = postgresContainer()) {
      postgres.start();
      configurePostgresRuntimeUser(postgres);
      createRuntimeOwnedWidgetsTable(postgres);

      TableSchema schema = widgetsSchema();
      RelationalSourceConfig config =
          sourceConfig(postgres, "runtime_pub_heartbeat", "runtime_slot_heartbeat");
      PostgresSourceAdapter adapter = new PostgresSourceAdapter();

      try (H2RuntimeStateStore stateStore =
          new H2RuntimeStateStore(tempDir.resolve("postgres-live-adapter-foreign-heartbeat"))) {
        OpenedSourceRuntime<?> opened = adapter.openRuntime(config, stateStore, List.of(schema));
        try {
          PostgresLiveStreamingRuntime runtime = (PostgresLiveStreamingRuntime) opened.runtime();
          assertThat(
                  runtime.emitHeartbeatIfDue(
                      Instant.parse("2026-03-20T00:06:00Z"), Duration.ofMillis(1)))
              .isTrue();
          PostgresPgoutputTransaction heartbeatTransaction =
              awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
          assertThat(heartbeatTransaction.events()).hasSize(1);
          assertThat(heartbeatTransaction.events().getFirst().operationType())
              .isEqualTo(OperationType.HEARTBEAT);
          runtime.acknowledge(heartbeatTransaction);

          try (Connection writer = openRuntimeConnection(postgres);
              Statement statement = writer.createStatement()) {
            statement.execute(
                "UPDATE dblog_meta.heartbeats SET run_id = 'other-run', source_stream_id = 'runtime_slot_heartbeat', "
                    + "last_beat_at = '2026-03-20T00:07:00Z' WHERE id = 1");
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
  void failsClosedWhenLogicalSlotIsAlreadyActiveOnAnotherOpenRuntime() throws Exception {
    assumeDockerIsAvailable();

    try (PostgreSQLContainer postgres = postgresContainer()) {
      postgres.start();
      configurePostgresRuntimeUser(postgres);
      createRuntimeOwnedWidgetsTable(postgres);

      TableSchema schema = widgetsSchema();
      RelationalSourceConfig config =
          sourceConfig(postgres, "runtime_pub_active", "runtime_slot_active");
      PostgresSourceAdapter adapter = new PostgresSourceAdapter();

      try (H2RuntimeStateStore firstStateStore =
              new H2RuntimeStateStore(tempDir.resolve("postgres-live-adapter-active-first"));
          H2RuntimeStateStore secondStateStore =
              new H2RuntimeStateStore(tempDir.resolve("postgres-live-adapter-active-second"))) {
        OpenedSourceRuntime<?> firstOpen = adapter.openRuntime(config, firstStateStore, List.of(schema));
        try {
          assertThat(firstOpen.runtime()).isInstanceOf(PostgresLiveStreamingRuntime.class);
          try (Connection observer = openAdminConnection(postgres)) {
            awaitSlotActive(observer, "runtime_slot_active");
          }

          assertThatThrownBy(() -> adapter.openRuntime(config, secondStateStore, List.of(schema)))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining("already active");
        } finally {
          firstOpen.runtime().close();
        }
      }
    }
  }

  @Test
  void failsClosedWhenSavedHistoryBecomesUnavailableAndSlotIsInvalidated() throws Exception {
    assumeDockerIsAvailable();

    try (PostgreSQLContainer postgres =
        postgresContainer("4", "4", "-c", "max_slot_wal_keep_size=1MB")) {
      postgres.start();
      configurePostgresRuntimeUser(postgres);
      createRuntimeOwnedWidgetsTable(postgres);

      TableSchema schema = widgetsSchema();
      RelationalSourceConfig config =
          sourceConfig(postgres, "runtime_pub_lost", "runtime_slot_lost");
      PostgresSourceAdapter adapter = new PostgresSourceAdapter();

      try (Connection sqlConnection = openAdminConnection(postgres);
          Connection controlConnection = openAdminConnection(postgres)) {
        try (Statement statement = sqlConnection.createStatement()) {
          statement.execute("CREATE TABLE public.wal_burner (id BIGINT PRIMARY KEY, payload TEXT)");
        }

        try (H2RuntimeStateStore stateStore =
            new H2RuntimeStateStore(tempDir.resolve("postgres-live-adapter-slot-lost"))) {
          io.github.aandreakis.dblog.adapter.postgres.PostgresSourceCheckpointStore checkpointStore =
              new io.github.aandreakis.dblog.adapter.postgres.PostgresSourceCheckpointStore(stateStore);

          OpenedSourceRuntime<?> firstOpen = adapter.openRuntime(config, stateStore, List.of(schema));
          try {
            try (Connection writer = openRuntimeConnection(postgres)) {
              insertWidget(writer, 1L, "one");
            }

            PostgresLiveStreamingRuntime runtime =
                (PostgresLiveStreamingRuntime) firstOpen.runtime();
            PostgresPgoutputTransaction transaction = awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
            runtime.acknowledge(transaction);
            assertThat(checkpointStore.load(SOURCE_ID)).contains(transaction.checkpointLsn());
          } finally {
            firstOpen.runtime().close();
          }

          forceSlotInvalidation(controlConnection, "runtime_slot_lost");
          SlotState slotState = readSlotState(controlConnection, "runtime_slot_lost");
          assertThat(slotState.isUnavailable()).isTrue();

          assertThatThrownBy(() -> adapter.openRuntime(config, stateStore, List.of(schema)))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining("PostgreSQL logical slot is invalid or lost");
          assertThat(checkpointStore.load(SOURCE_ID)).isPresent();
        }
      }
    }
  }

  @Test
  void failsClosedWhenReplicaIdentityChangesAndANewRelationArrivesOnDefaultLivePath()
      throws Exception {
    assumeDockerIsAvailable();

    try (PostgreSQLContainer postgres = postgresContainer()) {
      postgres.start();
      configurePostgresRuntimeUser(postgres);
      createRuntimeOwnedWidgetsTable(postgres);

      TableSchema schema = widgetsSchema();
      RelationalSourceConfig config =
          sourceConfig(postgres, "runtime_pub_replica_identity", "runtime_slot_replica_identity");
      PostgresSourceAdapter adapter = new PostgresSourceAdapter();

      try (H2RuntimeStateStore stateStore =
          new H2RuntimeStateStore(tempDir.resolve("postgres-live-adapter-replica-identity"))) {
        OpenedSourceRuntime<?> opened = adapter.openRuntime(config, stateStore, List.of(schema));
        try {
          PostgresLiveStreamingRuntime runtime = (PostgresLiveStreamingRuntime) opened.runtime();

          try (Connection writer = openRuntimeConnection(postgres)) {
            insertWidget(writer, 1L, "one");
          }

          PostgresPgoutputTransaction firstTransaction =
              awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
          runtime.acknowledge(firstTransaction);

          try (Connection writer = openRuntimeConnection(postgres);
              Statement statement = writer.createStatement()) {
            statement.execute("ALTER TABLE public.widgets REPLICA IDENTITY DEFAULT");
            statement.execute("ALTER TABLE public.widgets ADD COLUMN note TEXT");
            statement.execute(
                "INSERT INTO public.widgets (id, name, note) VALUES (2, 'two', 'note-2')");
          }

          assertThatThrownBy(() -> awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining("REPLICA IDENTITY FULL");
        } finally {
          opened.runtime().close();
        }
      }
    }
  }

  private static PostgreSQLContainer postgresContainer() {
    return sharedPostgres;
  }

  private static PostgreSQLContainer postgresContainer(
      String maxReplicationSlots, String maxWalSenders, String... extraPostgresOptions) {
    return LivePostgresTestContainers.newContainer(
        DockerImageName.parse(LivePostgresTestContainers.DEFAULT_IMAGE),
        maxReplicationSlots,
        maxWalSenders,
        extraPostgresOptions);
  }

  private static void dropReplicationSlots(Connection connection) throws Exception {
    for (String slotName :
        scalarStrings(
            connection,
            "SELECT slot_name FROM pg_catalog.pg_replication_slots "
                + "WHERE database = current_database()")) {
      try (PreparedStatement statement =
          connection.prepareStatement("SELECT pg_drop_replication_slot(?)")) {
        statement.setString(1, slotName);
        statement.execute();
      }
    }
  }

  private static List<String> scalarStrings(Connection connection, String sql) throws Exception {
    List<String> values = new ArrayList<>();
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      while (resultSet.next()) {
        values.add(resultSet.getString(1));
      }
    }
    return List.copyOf(values);
  }

  private static String quoteIdentifier(String identifier) {
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }

  private static void configureSqlConnection(Connection connection) throws Exception {
    connection.setAutoCommit(true);
    connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
  }

  private static final class SharedPostgreSQLContainer extends PostgreSQLContainer {
    private SharedPostgreSQLContainer(DockerImageName imageName) {
      super(imageName);
      LivePostgresTestContainers.configureLiveCdcContainer(this);
    }

    @Override
    public void close() {}

    private void stopShared() {
      super.close();
    }
  }

  private static void configurePostgresRuntimeUser(PostgreSQLContainer postgres)
      throws Exception {
    try (Connection connection = openAdminConnection(postgres);
        Statement statement = connection.createStatement()) {
      statement.execute(
          "DO $$ BEGIN "
              + "IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = '"
              + RUNTIME_USERNAME
              + "') THEN "
              + "CREATE ROLE "
              + RUNTIME_USERNAME
              + " WITH LOGIN PASSWORD '"
              + RUNTIME_PASSWORD
              + "' REPLICATION; "
              + "END IF; END $$");
      statement.execute(
          "ALTER ROLE "
              + RUNTIME_USERNAME
              + " WITH LOGIN PASSWORD '"
              + RUNTIME_PASSWORD
              + "' REPLICATION");
      statement.execute(
          "GRANT ALL PRIVILEGES ON DATABASE " + DATABASE_NAME + " TO " + RUNTIME_USERNAME);
      statement.execute("GRANT USAGE, CREATE ON SCHEMA public TO " + RUNTIME_USERNAME);
    }
  }

  private static void createRuntimeOwnedWidgetsTable(PostgreSQLContainer postgres)
      throws Exception {
    try (Connection connection = openAdminConnection(postgres);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TABLE public.widgets (id BIGINT PRIMARY KEY, name TEXT)");
      statement.execute("ALTER TABLE public.widgets OWNER TO " + RUNTIME_USERNAME);
      statement.execute("ALTER TABLE public.widgets REPLICA IDENTITY FULL");
    }
  }

  private static TableSchema widgetsSchema() {
    return TableSchema.create(
        new io.github.aandreakis.dblog.core.model.TableId(
            DATABASE_NAME, "public", "widgets"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true)),
        Instant.parse("2026-04-11T00:00:00Z"));
  }

  private static RelationalSourceConfig sourceConfig(
      PostgreSQLContainer postgres, String publicationName, String slotName) {
    return new RelationalSourceConfig(
        SOURCE_ID,
        postgres.getJdbcUrl(),
        RUNTIME_USERNAME,
        RUNTIME_PASSWORD,
        DATABASE_NAME,
        List.of("public.widgets"),
        Map.of("postgres.publicationName", publicationName, "postgres.slotName", slotName),
        false);
  }

  private static Connection openAdminConnection(PostgreSQLContainer postgres) throws Exception {
    return DriverManager.getConnection(
        postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
  }

  private static Connection openRuntimeConnection(PostgreSQLContainer postgres) throws Exception {
    return DriverManager.getConnection(
        postgres.getJdbcUrl(), RUNTIME_USERNAME, RUNTIME_PASSWORD);
  }

  private static void insertWidget(Connection connection, long id, String name) throws Exception {
    try (Statement statement = connection.createStatement()) {
      statement.execute(
          "INSERT INTO public.widgets (id, name) VALUES (" + id + ", '" + name + "')");
    }
  }

  private static String tableOwner(Connection connection, String schemaName, String tableName)
      throws java.sql.SQLException {
    try (var statement =
        connection.prepareStatement(
            "SELECT tableowner FROM pg_catalog.pg_tables WHERE schemaname = ? AND tablename = ?")) {
      statement.setString(1, schemaName);
      statement.setString(2, tableName);
      try (ResultSet resultSet = statement.executeQuery()) {
        assertThat(resultSet.next()).isTrue();
        return resultSet.getString(1);
      }
    }
  }

  private static List<String> publicationTables(Connection connection, String publicationName)
      throws java.sql.SQLException {
    List<String> tables = new ArrayList<>();
    try (var statement =
        connection.prepareStatement(
            "SELECT schemaname || '.' || tablename "
                + "FROM pg_catalog.pg_publication_tables "
                + "WHERE pubname = ? "
                + "ORDER BY schemaname, tablename")) {
      statement.setString(1, publicationName);
      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          tables.add(resultSet.getString(1));
        }
      }
    }
    return tables;
  }

  private static int slotCount(Connection connection, String slotName) throws java.sql.SQLException {
    try (var statement =
        connection.prepareStatement(
            "SELECT COUNT(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = ?")) {
      statement.setString(1, slotName);
      try (ResultSet resultSet = statement.executeQuery()) {
        assertThat(resultSet.next()).isTrue();
        return resultSet.getInt(1);
      }
    }
  }

  private static long widgetCount(Connection connection) throws java.sql.SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM public.widgets")) {
      assertThat(resultSet.next()).isTrue();
      return resultSet.getLong(1);
    }
  }

  private static String currentPostgresToken(Connection connection) throws java.sql.SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("SELECT token FROM dblog_meta.watermarks WHERE id = 1")) {
      assertThat(resultSet.next()).isTrue();
      return resultSet.getString(1);
    }
  }

  private static void awaitSlotActive(Connection connection, String slotName) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    while (System.nanoTime() < deadline) {
      try (var statement =
              connection.prepareStatement(
                  "SELECT active_pid FROM pg_replication_slots WHERE slot_name = ?")) {
        statement.setString(1, slotName);
        try (var resultSet = statement.executeQuery()) {
          if (resultSet.next() && resultSet.getObject(1) != null) {
            return;
          }
        }
      }
      Thread.sleep(50L);
    }
    throw new IllegalStateException(
        "Timed out waiting for PostgreSQL slot to become active: " + slotName);
  }

  private static void awaitSlotInactive(Connection connection, String slotName) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    while (System.nanoTime() < deadline) {
      try (var statement =
              connection.prepareStatement(
                  "SELECT active_pid FROM pg_replication_slots WHERE slot_name = ?")) {
        statement.setString(1, slotName);
        try (var resultSet = statement.executeQuery()) {
          if (resultSet.next() && resultSet.getObject(1) == null) {
            return;
          }
        }
      }
      Thread.sleep(50L);
    }
    throw new IllegalStateException(
        "Timed out waiting for PostgreSQL slot to become inactive: " + slotName);
  }

  private static void forceSlotInvalidation(Connection connection, String slotName)
      throws Exception {
    for (int batch = 0; batch < 24; batch++) {
      long startId = batch * 128L + 1L;
      long endId = startId + 127L;
      try (Statement statement = connection.createStatement()) {
        statement.execute(
            "INSERT INTO public.wal_burner (id, payload) "
                + "SELECT gs, repeat(md5(gs::text), 4096) "
                + "FROM generate_series("
                + startId
                + ", "
                + endId
                + ") gs "
                + "ON CONFLICT (id) DO UPDATE SET payload = excluded.payload");
        statement.execute("SELECT pg_switch_wal()");
        statement.execute("CHECKPOINT");
      }
      if (readSlotState(connection, slotName).isUnavailable()) {
        return;
      }
    }
    throw new IllegalStateException(
        "Failed to invalidate PostgreSQL logical slot after generating repeated WAL pressure");
  }

  private static SlotState readSlotState(Connection connection, String slotName)
      throws Exception {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "SELECT wal_status, COALESCE(invalidation_reason, '') "
                    + "FROM pg_replication_slots WHERE slot_name = '"
                    + slotName
                    + "'")) {
      assertThat(resultSet.next()).isTrue();
      return new SlotState(resultSet.getString(1), resultSet.getString(2));
    }
  }

  private record SlotState(String walStatus, String invalidationReason) {
    private boolean isUnavailable() {
      return "lost".equals(walStatus)
          || (invalidationReason != null && !invalidationReason.isBlank());
    }
  }

}
