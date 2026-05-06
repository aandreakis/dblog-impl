package io.github.aandreakis.dblog.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.adapter.api.HeartbeatMetadataWriter;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.api.WatermarkMetadataWriter;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourceAdapter;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.core.request.DumpRequestState;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.request.DumpTableProgress;
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
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class InspectionModeMySqlSinkFailureRecoveryTests {
  @TempDir Path tempDir;

  @Test
  void recoversAfterSinkFailureBeforeCheckpointAcknowledge() throws Exception {
    String h2Jdbc = "jdbc:h2:mem:phase30_mysql_sink_failure;MODE=MySQL;DB_CLOSE_DELAY=-1";
    TableSchema schema =
        TableSchema.create(
            new TableId("sourceA", "appdb", "customers"),
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
    Path statePath = tempDir.resolve("phase30-mysql-sink-failure-state");

    FailingSink firstSink = new FailingSink();
    DumpRequest request;
    try (H2RuntimeStateStore firstStateStore = new H2RuntimeStateStore(statePath);
        DbLogApplication<?> firstApp =
            DbLogApplication.open(adapter, config, firstStateStore, null, firstSink, 100)) {
      request = firstApp.submit(DumpScope.TABLE, schema.tableId(), List.of());

      assertThatThrownBy(() -> firstApp.processPendingRequests(Duration.ofMillis(5)))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("simulated sink failure before acknowledge");

      assertThat(firstSink.attemptedAppends).isEqualTo(1);
      assertThat(firstStateStore.dumpRequests().loadStatus(request.requestId()).orElseThrow().state())
          .isEqualTo(DumpRequestState.ACTIVE);
      DumpTableProgress progress =
          firstStateStore
              .dumpProgress()
              .load(request.requestId(), schema.tableId().displayName())
              .orElseThrow();
      assertThat(progress.hasActiveChunk()).isTrue();
    }

    RecordingSink secondSink = new RecordingSink();
    try (H2RuntimeStateStore secondStateStore = new H2RuntimeStateStore(statePath);
        DbLogApplication<?> secondApp =
            DbLogApplication.open(adapter, config, secondStateStore, null, secondSink, 100)) {
      secondApp.processPendingRequests(Duration.ofMillis(5));

      assertThat(secondStateStore.dumpRequests().loadStatus(request.requestId()).orElseThrow().state())
          .isEqualTo(DumpRequestState.COMPLETED);
      assertThat(secondSink.emittedEvents).hasSize(2);
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

  private static final class FailingSink implements io.github.aandreakis.dblog.sink.api.ChangeEventSink {
    private int attemptedAppends;

    @Override
    public void appendEvents(List<ChangeEvent> events) {
      attemptedAppends++;
      throw new IllegalStateException("simulated sink failure before acknowledge");
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
