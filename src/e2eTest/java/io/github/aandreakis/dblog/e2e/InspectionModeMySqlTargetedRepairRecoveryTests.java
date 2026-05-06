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
import io.github.aandreakis.dblog.core.request.DumpRequestStatus;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.runtime.bootstrap.DbLogApplication;
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

class InspectionModeMySqlTargetedRepairRecoveryTests {
  @TempDir Path tempDir;

  @Test
  void rerunsAnActiveTargetedRepairAfterRestartWithoutPersistedWindowState() throws Exception {
    String h2Jdbc = "jdbc:h2:mem:phase30_mysql_repair_recovery;MODE=MySQL;DB_CLOSE_DELAY=-1";
    try (Connection connection = DriverManager.getConnection(h2Jdbc)) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("CREATE SCHEMA IF NOT EXISTS appdb");
        statement.execute("CREATE TABLE appdb.customers (id BIGINT PRIMARY KEY, name VARCHAR(255))");
        statement.execute("INSERT INTO appdb.customers (id, name) VALUES (1, 'one'), (2, 'two')");
      }
    }

    TableSchema schema =
        TableSchema.create(
            new io.github.aandreakis.dblog.core.model.TableId("sourceA", "appdb", "customers"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-10T00:00:00Z"));

    MySqlSourceAdapter adapter = adapter(h2Jdbc, schema);
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
    Path statePath = tempDir.resolve("phase30-mysql-repair-recovery-state");

    RecordingSink firstSink = new RecordingSink();
    DumpRequest request;
    try (H2RuntimeStateStore firstStateStore = new H2RuntimeStateStore(statePath);
        DbLogApplication<?> firstApp =
            DbLogApplication.open(adapter, config, firstStateStore, null, firstSink, 100)) {
      request =
          firstApp.submitFromLiterals(
              DumpScope.PRIMARY_KEYS, schema.tableId(), List.of("1", "9"));
      var batch = firstApp.stack().coordinator().coordinateNextBatch().orElseThrow();
      assertThat(batch.request().requestId()).isEqualTo(request.requestId());

      DumpRequestStatus activeStatus =
          firstStateStore.dumpRequests().loadStatus(request.requestId()).orElseThrow();
      assertThat(activeStatus.state()).isEqualTo(DumpRequestState.ACTIVE);
      assertThat(firstSink.emittedEvents).isEmpty();
    }

    RecordingSink secondSink = new RecordingSink();
    try (H2RuntimeStateStore secondStateStore = new H2RuntimeStateStore(statePath);
        DbLogApplication<?> secondApp =
            DbLogApplication.open(adapter, config, secondStateStore, null, secondSink, 100)) {
      DumpRequestStatus recoveredStatus =
          secondStateStore.dumpRequests().loadStatus(request.requestId()).orElseThrow();
      assertThat(recoveredStatus.state()).isEqualTo(DumpRequestState.ACTIVE);

      secondApp.processPendingRequests(Duration.ofMillis(5));

      DumpRequestStatus completed =
          secondStateStore.dumpRequests().loadStatus(request.requestId()).orElseThrow();
      assertThat(completed.state()).isEqualTo(DumpRequestState.COMPLETED);
      assertThat(completed.missingPrimaryKeyLiterals()).containsExactly("9");
      assertThat(secondSink.emittedEvents).hasSize(1);
    }
  }

  private static MySqlSourceAdapter adapter(String h2Jdbc, TableSchema schema) {
    return new MySqlSourceAdapter(
        new MySqlSourceAdapter.Dependencies() {
          private final io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlChunkReader
              chunkReader = new io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlChunkReader();

          @Override
          public Connection openSqlConnection(RelationalSourceConfig config) {
            try {
              return DriverManager.getConnection(h2Jdbc);
            } catch (java.sql.SQLException failure) {
              throw new io.github.aandreakis.dblog.DbLogRuntimeException(failure);
            }
          }

          @Override
          public List<TableSchema> inspectSchemas(Connection connection, RelationalSourceConfig config) {
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
  }

  private static final class RecordingSink implements io.github.aandreakis.dblog.sink.api.ChangeEventSink {
    private final List<ChangeEvent> emittedEvents = new ArrayList<>();

    @Override
    public void appendEvents(List<ChangeEvent> events) {
      emittedEvents.addAll(events);
    }
  }
}
