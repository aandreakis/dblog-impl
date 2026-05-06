package io.github.aandreakis.dblog.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.api.HeartbeatMetadataWriter;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.api.WatermarkMetadataWriter;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourceAdapter;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.core.request.DumpRequestState;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.runtime.bootstrap.RelationalRuntimeAssembly;
import io.github.aandreakis.dblog.runtime.bootstrap.RelationalRuntimeBootstrap;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class InspectionModeMySqlDumpFlowTests {
  @TempDir Path tempDir;

  @Test
  void processesATableDumpRequestEndToEndThroughTheIntegratedInspectionStack() throws Exception {
    String h2Jdbc = "jdbc:h2:mem:phase15_mysql;MODE=MySQL;DB_CLOSE_DELAY=-1";
    TableSchema schema =
        TableSchema.create(
            new io.github.aandreakis.dblog.core.model.TableId("sourceA", "appdb", "customers"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-10T00:00:00Z"));
    try (Connection connection = DriverManager.getConnection(h2Jdbc)) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("CREATE SCHEMA IF NOT EXISTS appdb");
        statement.execute("CREATE TABLE appdb.customers (id BIGINT PRIMARY KEY, name VARCHAR(255))");
        statement.execute("INSERT INTO appdb.customers (id, name) VALUES (1, 'one'), (2, 'two')");
      }
    }

    RecordingSink sink = new RecordingSink();
    MySqlSourceAdapter adapter =
        new MySqlSourceAdapter(
            new MySqlSourceAdapter.Dependencies() {
              private final io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlChunkReader chunkReader =
                  new io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlChunkReader();

              @Override
              public Connection openSqlConnection(RelationalSourceConfig config) {
                try {
                  return DriverManager.getConnection(h2Jdbc);
                } catch (java.sql.SQLException failure) {
                  throw new io.github.aandreakis.dblog.DbLogRuntimeException(failure);
                }
              }

              @Override
              public List<TableSchema> inspectSchemas(
                  Connection connection, RelationalSourceConfig config) {
                return List.of(schema);
              }

              @Override
              public SourceChunkReader chunkReader() {
                return chunkReader;
              }

              @Override
              public WatermarkMetadataWriter watermarkWriter() {
                return new WatermarkMetadataWriter() {
                  @Override
                  public void ensureMetadataTable(Connection connection) {}

                  @Override
                  public void writeWatermark(
                      Connection sqlConnection,
                      String runId,
                      io.github.aandreakis.dblog.core.model.WatermarkToken token) {}
                };
              }

              @Override
              public HeartbeatMetadataWriter heartbeatWriter() {
                return new HeartbeatMetadataWriter() {
                  @Override
                  public void ensureHeartbeatTable(Connection connection) {}

                  @Override
                  public boolean writeHeartbeatIfDue(
                      Connection connection,
                      String runId,
                      String sourceStreamId,
                      Instant heartbeatTime,
                      Duration minimumInterval) {
                    return false;
                  }
                };
              }
            });

    RelationalSourceConfig config =
        new RelationalSourceConfig(
            "sourceA",
            "jdbc:mysql://127.0.0.1:3306/appdb",
            "dblog",
            "secret",
            "appdb",
            List.of("appdb.customers"),
            java.util.Map.of(),
            false);

    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(tempDir.resolve("phase15-state"))) {
      RelationalRuntimeBootstrap bootstrap = new RelationalRuntimeBootstrap();
      RelationalRuntimeBootstrap.BootstrappedSession bootstrapped =
          bootstrap.open(new RelationalRuntimeBootstrap.BootstrapRequest(adapter, config, stateStore, sink));

      DumpRequest request =
          new DumpRequest(
              "42", DumpScope.TABLE, bootstrapped.contractSchemas().getFirst().tableId(), List.of());
      stateStore.dumpRequests().upsert(request);

      RelationalRuntimeAssembly.forBootstrappedSession(
              bootstrapped, stateStore, "MySQL", "sourceA", 100)
          .processPendingRequests(Duration.ofMillis(5));

      assertThat(stateStore.dumpRequests().loadStatus("42")).isPresent();
      assertThat(stateStore.dumpRequests().loadStatus("42").orElseThrow().state())
          .isEqualTo(DumpRequestState.COMPLETED);
      assertThat(sink.emittedEvents).hasSize(2);
      assertThat(sink.emittedEvents).extracting(ChangeEvent::captureOrigin).containsOnly(io.github.aandreakis.dblog.core.model.CaptureOrigin.SELECT);
    }
  }

  private static final class RecordingSink implements io.github.aandreakis.dblog.sink.api.ChangeEventSink {
    private final List<ChangeEvent> emittedEvents = new ArrayList<>();

    @Override
    public void appendEvents(List<ChangeEvent> events) {
      emittedEvents.addAll(events);
    }
  }
}
