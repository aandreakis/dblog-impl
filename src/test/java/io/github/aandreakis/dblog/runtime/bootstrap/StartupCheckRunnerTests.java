package io.github.aandreakis.dblog.runtime.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.api.OpenedSourceRuntime;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.SourceAdapter;
import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.api.SourceRuntime;
import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.boot.SourceAdapterRegistry;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.sink.api.ChangeEventSink;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StartupCheckRunnerTests {
  @TempDir Path tempDir;

  @Test
  void runsBootstrapValidationAndClosesOpenedResources() throws Exception {
    AtomicBoolean runtimeClosed = new AtomicBoolean(false);
    AtomicBoolean sinkClosed = new AtomicBoolean(false);
    TableSchema schema = schema();
    SourceAdapter adapter =
        new SourceAdapter() {
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
                new SourceRuntime<NoopTransaction>() {
                  @Override
                  public Optional<NoopTransaction> readPendingTransaction() {
                    return Optional.empty();
                  }

                  @Override
                  public void acknowledge(NoopTransaction transaction) {}

                  @Override
                  public void close() {
                    runtimeClosed.set(true);
                  }
                },
                new NoopChunkReader(),
                "loaded-checkpoint");
          }
        };
    StartupCheckRunner runner =
        new StartupCheckRunner(new SourceAdapterRegistry(List.of(adapter)));

    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(tempDir.resolve("startup-check-state"))) {
      StartupCheckRunner.StartupCheckResult result =
          runner.run(
              new StartupCheckRunner.StartupCheckRequest(
                  "fake",
                  new RelationalSourceConfig(
                      "sourceA",
                      "jdbc:fake://localhost/db",
                      "user",
                      "",
                      "db",
                      List.of("sourceA.appdb.widgets"),
                      java.util.Map.of(),
                      false),
                  stateStore,
                  new ChangeEventSink() {
                    @Override
                    public void appendEvents(List<ChangeEvent> events) {}

                    @Override
                    public void close() {
                      sinkClosed.set(true);
                    }
                  }));

      assertThat(result.adapterKey()).isEqualTo("fake");
      assertThat(result.adapterDisplayName()).isEqualTo("Fake");
      assertThat(result.sourceId()).isEqualTo("sourceA");
      assertThat(result.loadedCheckpointDisplayValue()).isEqualTo("loaded-checkpoint");
      assertThat(result.liveSchemaCount()).isEqualTo(1);
      assertThat(result.contractSchemaCount()).isEqualTo(1);
      assertThat(runtimeClosed).isTrue();
      assertThat(sinkClosed).isTrue();
    }
  }

  private static TableSchema schema() {
    return TableSchema.create(
        new TableId("sourceA", "appdb", "widgets"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
        Instant.parse("2026-04-10T00:00:00Z"));
  }

  private record NoopTransaction(
      String transactionId,
      io.github.aandreakis.dblog.core.model.SourcePosition checkpointPosition,
      List<ChangeEvent> events)
      implements SourceTransaction<io.github.aandreakis.dblog.core.model.SourcePosition> {
    @Override
    public Instant commitTimestamp() {
      return Instant.EPOCH;
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
}
