package io.github.aandreakis.dblog.runtime.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.adapter.api.OpenedSourceRuntime;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.SourceAdapter;
import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.adapter.mysql.MySqlBinlogTransaction;
import io.github.aandreakis.dblog.adapter.mysql.MySqlBufferedStreamingRuntime;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourceCheckpointStore;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourcePosition;
import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import io.github.aandreakis.dblog.testsupport.ChangeEventTestFixtures;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DbLogApplicationTests {
  @TempDir Path tempDir;

  @Test
  void exposesRuntimeStatusAcceptsSubmissionsAndRejectsWorkAfterClose() throws Exception {
    TableSchema schema = schema();
    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(tempDir.resolve("inspection-app-state"))) {
      DbLogApplication<?> app =
          DbLogApplication.open(
              new FakeAdapter(schema),
              new RelationalSourceConfig(
                  "sourceA",
                  "jdbc:fake://localhost/db",
                  "user",
                  "",
                  "db",
                  List.of("appdb.customers"),
                  java.util.Map.of(),
                  false),
              stateStore,
              null,
              events -> {},
              100);
      try {
        assertThat(app.snapshot().mode()).isEqualTo("inspection");
        assertThat(app.snapshot().sourceId()).isEqualTo("sourceA");
        assertThat(app.snapshot().capturedTableCount()).isEqualTo(1);
        assertThat(app.snapshot().healthStatus()).isEqualTo("UP");
        assertThat(app.submit(DumpScope.ALL_TABLES, null, List.of()).requestId()).isNotBlank();
      } finally {
        app.close();
      }

      assertThatThrownBy(() -> app.submit(DumpScope.ALL_TABLES, null, List.of()))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("closed");
    }
  }

  @Test
  void snapshotReportsTheBootModeSuppliedAtOpen() throws Exception {
    TableSchema schema = schema();
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("bootmode-state"))) {
      try (DbLogApplication<?> app =
          DbLogApplication.open(
              "runtime",
              new FakeAdapter(schema),
              new RelationalSourceConfig(
                  "sourceA",
                  "jdbc:fake://localhost/db",
                  "user",
                  "",
                  "db",
                  List.of("appdb.customers"),
                  java.util.Map.of(),
                  false),
              stateStore,
              null,
              events -> {},
              100,
              null,
              DbLogApplication.ControlPlaneEventCaptureOptions.defaults())) {
        assertThat(app.snapshot().mode()).isEqualTo("runtime");
      }
    }
  }

  @Test
  void createsAControlPlaneServerFromTheApplication() throws Exception {
    TableSchema schema = schema();
    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(tempDir.resolve("inspection-app-http-state"))) {
      DbLogApplication<?> app =
          DbLogApplication.open(
              new FakeAdapter(schema),
              new RelationalSourceConfig(
                  "sourceA",
                  "jdbc:fake://localhost/db",
                  "user",
                  "",
                  "db",
                  List.of("appdb.customers"),
                  java.util.Map.of(),
                  false),
              stateStore,
              null,
              events -> {},
              100);
      var server = app.controlPlaneServer("127.0.0.1", 0);
      server.start();
      try {
        String body =
            HttpClient.newHttpClient()
                .send(
                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.boundPort() + "/api/v1/runtime"))
                        .GET()
                        .build(),
                    HttpResponse.BodyHandlers.ofString())
                .body();
        assertThat(body).contains("\"mode\":\"inspection\"").contains("\"sourceId\":\"sourceA\"");

        String eventsBody =
            HttpClient.newHttpClient()
                .send(
                    HttpRequest.newBuilder(
                            URI.create(
                                "http://127.0.0.1:" + server.boundPort() + "/api/v1/events/recent?limit=10"))
                        .GET()
                        .build(),
                    HttpResponse.BodyHandlers.ofString())
                .body();
        assertThat(eventsBody).contains("\"events\":[]");
      } finally {
        server.stop();
        app.close();
      }
    }
  }

  @Test
  void canIngressACommittedTransactionAndDrainItThroughTheApplication() throws Exception {
    TableSchema schema = schema();
    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(tempDir.resolve("inspection-app-streaming-state"))) {
      DbLogApplication<MySqlBinlogTransaction> app =
          cast(
              DbLogApplication.open(
                  new MySqlLikeAdapter(schema),
                  new RelationalSourceConfig(
                      "sourceA",
                      "jdbc:fake://localhost/db",
                      "user",
                      "",
                      "db",
                      List.of("appdb.customers"),
                      java.util.Map.of(),
                      false),
                  stateStore,
                  null,
                  events -> {},
                  100));
      try {
        app.enqueueCommittedTransaction(
            new MySqlBinlogTransaction(
                "tx-1",
                null,
                new MySqlSourcePosition("mysql-bin.000001", 42L, null),
                Instant.parse("2026-04-10T00:00:01Z"),
                List.of(
                    ChangeEventTestFixtures.fromRowMaps(
                        schema.tableId(),
                        OperationType.UPDATE,
                        CaptureOrigin.LOG,
                        java.util.Map.of("id", 1),
                        null,
                        java.util.Map.of("id", 1),
                        new MySqlSourcePosition("mysql-bin.000001", 42L, null),
                        "tx-1",
                        null))));

        assertThat(app.drainStreaming(java.time.Duration.ofMillis(20))).isEqualTo(1);
        assertThat(app.snapshot().lastAcknowledgedCheckpoint()).isEqualTo("mysql-bin.000001:42");
      } finally {
        app.close();
      }
    }
  }

  @Test
  void canDisableControlPlaneEventRecordingForHotPaths() throws Exception {
    TableSchema schema = schema();
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("inspection-app-event-capture-disabled-state"))) {
      DbLogApplication<MySqlBinlogTransaction> app =
          cast(
              DbLogApplication.open(
                  new MySqlLikeAdapter(schema),
                  new RelationalSourceConfig(
                      "sourceA",
                      "jdbc:fake://localhost/db",
                      "user",
                      "",
                      "db",
                      List.of("appdb.customers"),
                      java.util.Map.of(),
                      false),
                  stateStore,
                  null,
                  events -> {},
                  100,
                  null,
                  DbLogApplication.ControlPlaneEventCaptureOptions.disabled()));
      try {
        app.enqueueCommittedTransaction(
            new MySqlBinlogTransaction(
                "tx-1",
                null,
                new MySqlSourcePosition("mysql-bin.000001", 42L, null),
                Instant.parse("2026-04-10T00:00:01Z"),
                List.of(
                    ChangeEventTestFixtures.fromRowMaps(
                        schema.tableId(),
                        OperationType.UPDATE,
                        CaptureOrigin.LOG,
                        java.util.Map.of("id", 1),
                        null,
                        java.util.Map.of("id", 1),
                        new MySqlSourcePosition("mysql-bin.000001", 42L, null),
                        "tx-1",
                        null))));

        assertThat(app.drainStreaming(java.time.Duration.ofMillis(20))).isEqualTo(1);
        assertThat(
                app.controlPlaneQueryService()
                    .recentEventsPayload(10)
                    .get("cumulativeEventsObserved"))
            .isEqualTo(0L);
        assertThat(
                app.controlPlaneQueryService()
                    .recentEventsPayload(10)
                    .get("recentEventWindowSize"))
            .isEqualTo(0);
      } finally {
        app.close();
      }
    }
  }

  @Test
  void loadsPersistedCheckpointIntoRuntimeStatusWhenOpenedThroughMysqlLikeAdapter() throws Exception {
    TableSchema schema = schema();
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("inspection-app-loaded-checkpoint-state"))) {
      stateStore
          .streamPositions()
          .saveCheckpoint("sourceA", new MySqlSourcePosition("mysql-bin.000007", 777L, null));

      DbLogApplication<MySqlBinlogTransaction> app =
          cast(
              DbLogApplication.open(
                  new MySqlLikeAdapter(schema),
                  new RelationalSourceConfig(
                      "sourceA",
                      "jdbc:fake://localhost/db",
                      "user",
                      "",
                      "db",
                      List.of("appdb.customers"),
                      java.util.Map.of(),
                      false),
                  stateStore,
                  null,
                  events -> {},
                  100));
      try {
        assertThat(app.snapshot().lastAcknowledgedCheckpoint()).isEqualTo("mysql-bin.000007:777");
      } finally {
        app.close();
      }
    }
  }

  private static TableSchema schema() {
    return TableSchema.create(
        new TableId("sourceA", "appdb", "customers"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
        Instant.parse("2026-04-10T00:00:00Z"));
  }

  private static final class FakeAdapter implements SourceAdapter {
    private final TableSchema schema;

    private FakeAdapter(TableSchema schema) {
      this.schema = schema;
    }

    @Override
    public String key() {
      return "fake";
    }

    @Override
    public String displayName() {
      return "Fake";
    }

    @Override
    public io.github.aandreakis.dblog.adapter.api.SourceDialect dialect() {
      return io.github.aandreakis.dblog.support.FakeSourceDialect.INSTANCE;
    }

    @Override
    public void validateSourceConfig(RelationalSourceConfig config) {}

    @Override
    public io.github.aandreakis.dblog.adapter.api.SourcePreflight preflight() {
      return (c, s) -> {};
    }

    @Override
    public List<TableSchema> inspectSchemas(RelationalSourceConfig config) {
      return List.of(schema);
    }

    @Override
    public OpenedSourceRuntime<? extends SourceTransaction<?>> openRuntime(
        RelationalSourceConfig config,
        RuntimeStateStore stateStore,
        List<TableSchema> contractSchemas) {
      return new OpenedSourceRuntime<>(
          new InspectionOnlySourceRuntime("Fake", connection(), contractSchemas),
          new NoopChunkReader(),
          null);
    }

    private java.sql.Connection connection() {
      try {
        return java.sql.DriverManager.getConnection("jdbc:h2:mem:inspection_app;DB_CLOSE_DELAY=-1");
      } catch (java.sql.SQLException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private static final class NoopChunkReader implements SourceChunkReader {
    @Override
    public Optional<io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple>
        tableScanUpperBoundPrimaryKeyTuple(java.sql.Connection connection, TableSchema schema) {
      return Optional.empty();
    }

    @Override
    public Optional<io.github.aandreakis.dblog.core.reconcile.Chunk> nextTableChunk(
        java.sql.Connection connection,
        String jobId,
        TableSchema schema,
        io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple startAfterPrimaryKey,
        io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple stopAtPrimaryKey,
        int chunkSize) {
      return Optional.empty();
    }

    @Override
    public Optional<io.github.aandreakis.dblog.core.reconcile.Chunk>
        targetedPrimaryKeyTuples(
            java.sql.Connection connection,
            String jobId,
            TableSchema schema,
            List<io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple>
                requestedPrimaryKeys) {
      return Optional.empty();
    }
  }

  private static final class MySqlLikeAdapter implements SourceAdapter {
    private final TableSchema schema;

    private MySqlLikeAdapter(TableSchema schema) {
      this.schema = schema;
    }

    @Override
    public String key() {
      return "mysql";
    }

    @Override
    public String displayName() {
      return "MySQL";
    }

    @Override
    public io.github.aandreakis.dblog.adapter.api.SourceDialect dialect() {
      return io.github.aandreakis.dblog.adapter.mysql.MySqlDialect.INSTANCE;
    }

    @Override
    public void validateSourceConfig(RelationalSourceConfig config) {}

    @Override
    public io.github.aandreakis.dblog.adapter.api.SourcePreflight preflight() {
      return (c, s) -> {};
    }

    @Override
    public List<TableSchema> inspectSchemas(RelationalSourceConfig config) {
      return List.of(schema);
    }

    @Override
    public OpenedSourceRuntime<? extends SourceTransaction<?>> openRuntime(
        RelationalSourceConfig config,
        RuntimeStateStore stateStore,
        List<TableSchema> contractSchemas) {
      try {
        MySqlSourcePosition loadedCheckpoint =
            new MySqlSourceCheckpointStore(stateStore).load(config.sourceId()).orElse(null);
        return new OpenedSourceRuntime<>(
            new MySqlBufferedStreamingRuntime(
                java.sql.DriverManager.getConnection("jdbc:h2:mem:inspection_app_mysql;DB_CLOSE_DELAY=-1"),
                config.sourceId(),
                "stream-1",
                contractSchemas,
                new io.github.aandreakis.dblog.adapter.api.WatermarkMetadataWriter() {
                  @Override
                  public void ensureMetadataTable(java.sql.Connection connection) {}

                  @Override
                  public void writeWatermark(
                      java.sql.Connection connection,
                      String runId,
                      io.github.aandreakis.dblog.core.model.WatermarkToken token) {}
                },
                new io.github.aandreakis.dblog.adapter.api.HeartbeatMetadataWriter() {
                  @Override
                  public void ensureHeartbeatTable(java.sql.Connection connection) {}

                  @Override
                  public boolean writeHeartbeatIfDue(
                      java.sql.Connection connection,
                      String runId,
                      String sourceStreamId,
                      Instant heartbeatTime,
                      java.time.Duration minimumInterval) {
                    return false;
                  }
                },
                checkpoint -> new MySqlSourceCheckpointStore(stateStore).save(config.sourceId(), checkpoint),
                loadedCheckpoint),
            new NoopChunkReader(),
            loadedCheckpoint == null ? null : loadedCheckpoint.displayValue());
      } catch (java.sql.SQLException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T cast(Object value) {
    return (T) value;
  }
}
