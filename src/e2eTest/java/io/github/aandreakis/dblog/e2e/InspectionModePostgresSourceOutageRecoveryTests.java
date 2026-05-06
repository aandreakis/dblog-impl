package io.github.aandreakis.dblog.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.postgres.PostgresSourceAdapter;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.request.CoreRequestExecutionException;
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.core.request.DumpRequestState;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.e2e.support.PostgresInspectionOnlyDependencies;
import io.github.aandreakis.dblog.runtime.bootstrap.DbLogApplication;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class InspectionModePostgresSourceOutageRecoveryTests {
  @TempDir Path tempDir;

  @Test
  void recoversAfterAnInitialSourceReadFailureWithoutCorruptingDurableState() throws Exception {
    String h2Jdbc = "jdbc:h2:mem:phase30_postgres_outage;MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
    TableSchema schema =
        TableSchema.create(
            new TableId("appdb", "public", "customers"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-10T00:00:00Z"));
    try (Connection connection = DriverManager.getConnection(h2Jdbc)) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("CREATE SCHEMA IF NOT EXISTS \"public\"");
        statement.execute(
            "CREATE TABLE \"public\".\"customers\" (\"id\" BIGINT PRIMARY KEY, \"name\" VARCHAR(255))");
        statement.execute(
            "INSERT INTO \"public\".\"customers\" (\"id\", \"name\") VALUES (1, 'one'), (2, 'two')");
      }
    }

    RelationalSourceConfig config =
        new RelationalSourceConfig(
            "sourceA",
            "jdbc:postgresql://127.0.0.1:5432/appdb",
            "postgres",
            "secret",
            "appdb",
            List.of("public.customers"),
            java.util.Map.of(),
            false);
    Path statePath = tempDir.resolve("phase30-postgres-outage-state");

    RecordingSink firstSink = new RecordingSink();
    DumpRequest request;
    AtomicBoolean failFirstRead = new AtomicBoolean(true);
    try (H2RuntimeStateStore firstStateStore = new H2RuntimeStateStore(statePath);
        DbLogApplication<?> firstApp =
            DbLogApplication.open(
                adapter(h2Jdbc, schema, failFirstRead),
                config,
                firstStateStore,
                null,
                firstSink,
                100)) {
      request = firstApp.submit(DumpScope.TABLE, schema.tableId(), List.of());

      assertThatThrownBy(() -> firstApp.processPendingRequests(Duration.ofMillis(5)))
          .isInstanceOf(CoreRequestExecutionException.class)
          .hasRootCauseMessage("simulated PostgreSQL source outage during chunk read");

      assertThat(firstSink.emittedEvents).isEmpty();
      assertThat(firstStateStore.dumpRequests().loadStatus(request.requestId()).orElseThrow().state())
          .isEqualTo(DumpRequestState.ACTIVE);
      assertThat(firstStateStore.dumpProgress().load(request.requestId(), schema.tableId().displayName()))
          .isEmpty();
    }

    RecordingSink secondSink = new RecordingSink();
    try (H2RuntimeStateStore secondStateStore = new H2RuntimeStateStore(statePath);
        DbLogApplication<?> secondApp =
            DbLogApplication.open(
                adapter(h2Jdbc, schema, new AtomicBoolean(false)),
                config,
                secondStateStore,
                null,
                secondSink,
                100)) {
      assertThat(secondStateStore.dumpRequests().loadStatus(request.requestId()).orElseThrow().state())
          .isEqualTo(DumpRequestState.ACTIVE);

      secondApp.processPendingRequests(Duration.ofMillis(5));

      assertThat(secondStateStore.dumpRequests().loadStatus(request.requestId()).orElseThrow().state())
          .isEqualTo(DumpRequestState.COMPLETED);
      assertThat(secondSink.emittedEvents).hasSize(2);
    }
  }

  private static PostgresSourceAdapter adapter(
      String h2Jdbc, TableSchema schema, AtomicBoolean failFirstRead) {
    SourceChunkReader delegate =
        new io.github.aandreakis.dblog.adapter.postgres.internal.JdbcPostgresChunkReader();
    SourceChunkReader outageChunkReader =
        new SourceChunkReader() {
          @Override
          public java.util.Optional<
                  io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple>
              tableScanUpperBoundPrimaryKeyTuple(Connection connection, TableSchema schema)
                  throws SQLException {
            return delegate.tableScanUpperBoundPrimaryKeyTuple(connection, schema);
          }

          @Override
          public java.util.Optional<io.github.aandreakis.dblog.core.reconcile.Chunk>
              nextTableChunk(
                  Connection connection,
                  String jobId,
                  TableSchema schema,
                  io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple
                      startAfterPrimaryKey,
                  io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple stopAtPrimaryKey,
                  int chunkSize)
                  throws SQLException {
            if (failFirstRead.compareAndSet(true, false)) {
              throw new SQLException("simulated PostgreSQL source outage during chunk read");
            }
            return delegate.nextTableChunk(
                connection, jobId, schema, startAfterPrimaryKey, stopAtPrimaryKey, chunkSize);
          }

          @Override
          public java.util.Optional<io.github.aandreakis.dblog.core.reconcile.Chunk>
              targetedPrimaryKeyTuples(
                  Connection connection,
                  String jobId,
                  TableSchema schema,
                  List<io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple>
                      requestedPrimaryKeys)
                  throws SQLException {
            return delegate.targetedPrimaryKeyTuples(
                connection, jobId, schema, requestedPrimaryKeys);
          }
        };
    return new PostgresSourceAdapter(
        PostgresInspectionOnlyDependencies.withFixedSchema(h2Jdbc, schema, outageChunkReader));
  }

  private static final class RecordingSink implements io.github.aandreakis.dblog.sink.api.ChangeEventSink {
    private final List<ChangeEvent> emittedEvents = new ArrayList<>();

    @Override
    public void appendEvents(List<ChangeEvent> events) {
      emittedEvents.addAll(events);
    }
  }
}
