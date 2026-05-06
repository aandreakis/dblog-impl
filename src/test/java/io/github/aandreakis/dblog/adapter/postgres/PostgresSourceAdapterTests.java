package io.github.aandreakis.dblog.adapter.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import io.github.aandreakis.dblog.adapter.api.HeartbeatMetadataWriter;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.api.SourceConnections;
import io.github.aandreakis.dblog.adapter.api.WatermarkMetadataWriter;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PostgresSourceAdapterTests {
  @TempDir Path tempDir;

  @Test
  void validatesJdbcPrefixTwoPartTablesAndOptionalDatabaseConsistency() {
    PostgresSourceAdapter adapter = new PostgresSourceAdapter();

    adapter.validateSourceConfig(
        new RelationalSourceConfig(
            "sourceA",
            "jdbc:postgresql://127.0.0.1:5432/appdb",
            "postgres",
            "secret",
            "appdb",
            List.of("public.customers"),
            java.util.Map.of(),
            false));

    assertThatThrownBy(
            () ->
                adapter.validateSourceConfig(
                    new RelationalSourceConfig(
                        "sourceA",
                        "jdbc:mysql://127.0.0.1:5432/appdb",
                        "postgres",
                        "secret",
                        "appdb",
                        List.of("public.customers"),
                        java.util.Map.of(),
                        false)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("jdbc:postgresql://");

    assertThatThrownBy(
            () ->
                adapter.validateSourceConfig(
                    new RelationalSourceConfig(
                        "sourceA",
                        "jdbc:postgresql://127.0.0.1:5432/appdb",
                        "postgres",
                        "secret",
                        "otherdb",
                        List.of("public.customers"),
                        java.util.Map.of(),
                        false)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Configured PostgreSQL database does not match JDBC URL database");
  }

  @Test
  void prefersLiveStreamRuntimeWhenDependenciesProvideOne() throws Exception {
    Connection connection = mock(Connection.class);
    Connection replicationConnection = mock(Connection.class);
    SourceChunkReader chunkReader = new NoopChunkReader();
    PostgresSourceAdapter adapter =
        new PostgresSourceAdapter(
            new PostgresSourceAdapter.Dependencies() {
              @Override
              public Connection openSqlConnection(RelationalSourceConfig config) {
                return connection;
              }

              @Override
              public Connection openReplicationConnection(RelationalSourceConfig config) {
                return replicationConnection;
              }

              @Override
              public List<TableSchema> inspectSchemas(Connection sqlConnection, RelationalSourceConfig config) {
                return List.of();
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
                      java.time.Instant heartbeatTime,
                      java.time.Duration minimumInterval) {
                    return false;
                  }
                };
              }

              @Override
              public java.util.Optional<io.github.aandreakis.dblog.adapter.api.OpenedSourceRuntime<? extends io.github.aandreakis.dblog.adapter.api.SourceTransaction<?>>> maybeOpenLiveRuntime(
                  RelationalSourceConfig config,
                  io.github.aandreakis.dblog.state.api.RuntimeStateStore stateStore,
                  List<TableSchema> contractSchemas,
                  PostgresSourceCheckpointStore checkpointStore,
                  SourceConnections connections,
                  SourceChunkReader runtimeChunkReader,
                  io.github.aandreakis.dblog.tap.Tap tap) {
                assertThat(connections.sql()).isSameAs(connection);
                assertThat(connections.replication()).contains(replicationConnection);
                return java.util.Optional.of(
                    new io.github.aandreakis.dblog.adapter.api.OpenedSourceRuntime<>(
                        new io.github.aandreakis.dblog.adapter.api.SourceRuntime<PostgresPgoutputTransaction>() {
                          @Override
                          public java.util.Optional<PostgresPgoutputTransaction> readPendingTransaction() {
                            return java.util.Optional.empty();
                          }

                          @Override
                          public void acknowledge(PostgresPgoutputTransaction transaction) {}
                        },
                        runtimeChunkReader,
                        "live"));
              }
            });
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
    TableSchema schema =
        TableSchema.create(
            new io.github.aandreakis.dblog.core.model.TableId("appdb", "public", "customers"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-10T00:00:00Z"));

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("postgres-source-adapter-live"))) {
      var opened = adapter.openRuntime(config, stateStore, List.of(schema));

      assertThat(opened.chunkReader()).isSameAs(chunkReader);
      assertThat(opened.loadedCheckpointDisplayValue()).isEqualTo("live");
    }
  }

  private static final class NoopChunkReader implements SourceChunkReader {
    @Override
    public java.util.Optional<io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple>
        tableScanUpperBoundPrimaryKeyTuple(
            java.sql.Connection connection,
            io.github.aandreakis.dblog.core.schema.TableSchema schema) {
      return java.util.Optional.empty();
    }

    @Override
    public java.util.Optional<io.github.aandreakis.dblog.core.reconcile.Chunk>
        nextTableChunk(
            java.sql.Connection connection,
            String jobId,
            io.github.aandreakis.dblog.core.schema.TableSchema schema,
            io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple startAfterPrimaryKey,
            io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple stopAtPrimaryKey,
            int chunkSize) {
      return java.util.Optional.empty();
    }

    @Override
    public java.util.Optional<io.github.aandreakis.dblog.core.reconcile.Chunk>
        targetedPrimaryKeyTuples(
            java.sql.Connection connection,
            String jobId,
            io.github.aandreakis.dblog.core.schema.TableSchema schema,
            java.util.List<io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple>
                requestedPrimaryKeys) {
      return java.util.Optional.empty();
    }
  }
}
