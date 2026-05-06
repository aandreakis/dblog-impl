package io.github.aandreakis.dblog.runtime.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.api.OpenedSourceRuntime;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.SourceAdapter;
import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.api.SourceRuntime;
import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RelationalRuntimeAssemblyTests {
  @TempDir Path tempDir;

  @Test
  void buildsCoordinatorsAndRequestPumpForWatermarkCapableBootstrappedSession()
      throws Exception {
    TableSchema schema =
        TableSchema.create(
            new TableId("sourceA", "appdb", "customers"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-10T00:00:00Z"));

    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(tempDir.resolve("inspection-stack-state"))) {
      RelationalRuntimeBootstrap bootstrap = new RelationalRuntimeBootstrap();
      RelationalRuntimeBootstrap.BootstrappedSession bootstrapped =
          bootstrap.open(
              new RelationalRuntimeBootstrap.BootstrapRequest(
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
                  events -> {}));

      RelationalRuntimeStack<?> stack =
          RelationalRuntimeAssembly.forBootstrappedSession(
              bootstrapped, stateStore, "MySQL", "sourceA", 100);

      assertThat(stack.dumpWindowCoordinator()).isNotNull();
      assertThat(stack.targetedRepairCoordinator()).isNotNull();
      assertThat(stack.coordinator()).isNotNull();
      assertThat(stack.requestPump()).isNotNull();
    }
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
          new InspectionOnlySourceRuntime("Fake", mockConnection(), contractSchemas),
          new NoopChunkReader(),
          null);
    }

    private java.sql.Connection mockConnection() {
      try {
        return java.sql.DriverManager.getConnection("jdbc:h2:mem:inspection_stack;DB_CLOSE_DELAY=-1");
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
}
