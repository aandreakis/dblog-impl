package io.github.aandreakis.dblog.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.postgres.PostgresSourceAdapter;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.core.request.DumpRequestState;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.SchemaDriftException;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.e2e.support.PostgresInspectionOnlyDependencies;
import io.github.aandreakis.dblog.runtime.bootstrap.DbLogApplication;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class InspectionModePostgresSchemaDriftInvalidationTests {
  @TempDir Path tempDir;

  @Test
  void marksRequestFailedAndSignalsFullDumpRequiredWhenChunkReadDetectsDrift() throws Exception {
    String h2Jdbc =
        "jdbc:h2:mem:phase30_postgres_schema_drift;MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
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

    try (H2RuntimeStateStore stateStore =
            new H2RuntimeStateStore(tempDir.resolve("phase30-postgres-schema-drift-state"));
        DbLogApplication<?> app =
            DbLogApplication.open(
                driftingAdapter(h2Jdbc, schema), config, stateStore, null, events -> {}, 100)) {
      DumpRequest request = app.submit(DumpScope.TABLE, schema.tableId(), List.of());

      app.processPendingRequests(Duration.ofMillis(5));

      assertThat(stateStore.dumpRequests().loadStatus(request.requestId()).orElseThrow().state())
          .isEqualTo(DumpRequestState.FAILED);
      assertThat(stateStore.schemas().loadFullDumpRequiredSignals())
          .anySatisfy(
              signal -> {
                assertThat(signal.tableId()).isEqualTo(schema.tableId());
                assertThat(signal.reason()).contains("simulated primary-key drift");
              });
    }
  }

  private static PostgresSourceAdapter driftingAdapter(String h2Jdbc, TableSchema schema) {
    SourceChunkReader driftingChunkReader =
        new SourceChunkReader() {
          @Override
          public java.util.Optional<
                  io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple>
              tableScanUpperBoundPrimaryKeyTuple(Connection connection, TableSchema schema) {
            return java.util.Optional.of(schema.primaryKeyTupleFromLiteral("10"));
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
                  int chunkSize) {
            throw new SchemaDriftException("simulated primary-key drift");
          }

          @Override
          public java.util.Optional<io.github.aandreakis.dblog.core.reconcile.Chunk>
              targetedPrimaryKeyTuples(
                  Connection connection,
                  String jobId,
                  TableSchema schema,
                  List<io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple>
                      requestedPrimaryKeys) {
            throw new UnsupportedOperationException();
          }
        };
    return new PostgresSourceAdapter(
        PostgresInspectionOnlyDependencies.withFixedSchema(h2Jdbc, schema, driftingChunkReader));
  }
}
