package io.github.aandreakis.dblog.integration.boot;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.api.OpenedSourceRuntime;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.SourceAdapter;
import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.api.SourceRuntime;
import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.boot.DbLogBootstrap;
import io.github.aandreakis.dblog.boot.SourceAdapterRegistry;
import io.github.aandreakis.dblog.config.DbLogBootstrapMode;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.runtime.bootstrap.StartupCheckRunner;
import io.github.aandreakis.dblog.runtime.host.DbLogRuntimeHostLifecycle;
import io.github.aandreakis.dblog.runtime.host.RuntimeSession;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import io.github.aandreakis.dblog.verification.scenario.ScenarioHarness;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag("integration-core")
class DbLogBootstrapModeIT {
  @TempDir Path tempDir;

  @Test
  void executesRuntimeModeThroughBootstrapAgainstRealHostLifecycle() throws Exception {
    DbLogBootstrap bootstrap = new DbLogBootstrap(new SourceAdapterRegistry(List.of(fakeAdapter())));
    DbLogRuntimeHostLifecycle<NoopTransaction> runtimeHostLifecycle =
        new DbLogRuntimeHostLifecycle<>(
            new RuntimeSession<>(new NoopRuntime(), new NoopChunkReader(), events -> {}));

    DbLogBootstrap.BootExecutionResult result =
        bootstrap.execute(
            new DbLogBootstrap.BootExecutionRequest(
                new DbLogBootstrap.BootstrapRequest(DbLogBootstrapMode.RUNTIME, "fake"),
                "runtime-streaming",
                Duration.ofMillis(1),
                null,
                null),
            runtimeHostLifecycle,
            new ScenarioHarness(),
            new StartupCheckRunner(new SourceAdapterRegistry(List.of(fakeAdapter()))));

    assertThat(result.target()).isEqualTo(DbLogBootstrap.BootTarget.RUNTIME_HOST);
    assertThat(result.outcome()).isEqualTo(0);
  }

  @Test
  void executesScenarioModeThroughBootstrapAgainstRealScenarioHarness() throws Exception {
    DbLogBootstrap bootstrap = new DbLogBootstrap(new SourceAdapterRegistry(List.of(fakeAdapter())));

    DbLogBootstrap.BootExecutionResult result =
        bootstrap.execute(
            new DbLogBootstrap.BootExecutionRequest(
                new DbLogBootstrap.BootstrapRequest(DbLogBootstrapMode.SCENARIO, "fake"),
                null,
                null,
                new ScenarioHarness.ScenarioRequest<>(
                    "scenario-it",
                    "integration scenario",
                    () -> "scenario-ok",
                    observation -> assertThat(observation).isEqualTo("scenario-ok"),
                    null),
                null),
            new DbLogRuntimeHostLifecycle<>(
                new RuntimeSession<>(new NoopRuntime(), new NoopChunkReader(), events -> {})),
            new ScenarioHarness(),
            new StartupCheckRunner(new SourceAdapterRegistry(List.of(fakeAdapter()))));

    assertThat(result.target()).isEqualTo(DbLogBootstrap.BootTarget.SCENARIO_HARNESS);
    assertThat(result.outcome()).isInstanceOf(ScenarioHarness.ScenarioResult.class);
    assertThat(((ScenarioHarness.ScenarioResult<?>) result.outcome()).succeeded()).isTrue();
  }

  @Test
  void executesStartupCheckModeThroughBootstrapAgainstRealStartupCheckRunner() throws Exception {
    SourceAdapter fakeAdapter = fakeAdapter();
    DbLogBootstrap bootstrap = new DbLogBootstrap(new SourceAdapterRegistry(List.of(fakeAdapter)));
    StartupCheckRunner runner =
        new StartupCheckRunner(new SourceAdapterRegistry(List.of(fakeAdapter)));

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("bootstrap-startup-check-it"))) {
      DbLogBootstrap.BootExecutionResult result =
          bootstrap.execute(
              new DbLogBootstrap.BootExecutionRequest(
                  new DbLogBootstrap.BootstrapRequest(DbLogBootstrapMode.STARTUP_CHECK, "fake"),
                  null,
                  null,
                  null,
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
                      events -> {})),
              new DbLogRuntimeHostLifecycle<>(
                  new RuntimeSession<>(new NoopRuntime(), new NoopChunkReader(), events -> {})),
              new ScenarioHarness(),
              runner);

      assertThat(result.target()).isEqualTo(DbLogBootstrap.BootTarget.STARTUP_CHECK);
      assertThat(result.outcome()).isInstanceOf(StartupCheckRunner.StartupCheckResult.class);
      StartupCheckRunner.StartupCheckResult startupCheckResult =
          (StartupCheckRunner.StartupCheckResult) result.outcome();
      assertThat(startupCheckResult.adapterKey()).isEqualTo("fake");
      assertThat(startupCheckResult.liveSchemaCount()).isEqualTo(1);
    }
  }

  private static SourceAdapter fakeAdapter() {
    TableSchema schema =
        TableSchema.create(
            new TableId("sourceA", "appdb", "widgets"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-10T00:00:00Z"));
    return new SourceAdapter() {
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
        return new io.github.aandreakis.dblog.adapter.api.SourceDialect() {
          @Override
          public String key() {
            return "fake";
          }

          @Override
          public String displayName() {
            return "Fake";
          }

          @Override
          public String jdbcUrlPrefix() {
            return "jdbc:fake://";
          }

          @Override
          public io.github.aandreakis.dblog.adapter.api.TableNamingPolicy tableNamingPolicy() {
            return io.github.aandreakis.dblog.adapter.api.TableNamingPolicy.TWO_PART_SCHEMA_TABLE;
          }

          @Override
          public void validateNativeConfig(RelationalSourceConfig config) {}

          @Override
          public io.github.aandreakis.dblog.core.schema.NeutralColumnType neutralType(
              io.github.aandreakis.dblog.adapter.api.RawColumnMetadata column) {
            return io.github.aandreakis.dblog.core.schema.NeutralColumnType.UNSUPPORTED;
          }
        };
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
        return new OpenedSourceRuntime<>(new NoopRuntime(), new NoopChunkReader(), "loaded-checkpoint");
      }
    };
  }

  private static final class NoopRuntime implements SourceRuntime<NoopTransaction> {
    @Override
    public Optional<NoopTransaction> readPendingTransaction() {
      return Optional.empty();
    }

    @Override
    public void acknowledge(NoopTransaction transaction) {}
  }

  private record NoopSourcePosition(String displayValue) implements SourcePosition {}

  private record NoopTransaction(
      String transactionId,
      NoopSourcePosition checkpointPosition,
      List<ChangeEvent> events)
      implements SourceTransaction<NoopSourcePosition> {
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
    public Optional<Chunk> nextTableChunk(
        java.sql.Connection connection,
        String jobId,
        TableSchema schema,
        io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple startAfterPrimaryKey,
        io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple stopAtPrimaryKey,
        int chunkSize) {
      return Optional.empty();
    }

    @Override
    public Optional<Chunk> targetedPrimaryKeyTuples(
        java.sql.Connection connection,
        String jobId,
        TableSchema schema,
        List<io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple> requestedPrimaryKeys) {
      return Optional.empty();
    }
  }
}
