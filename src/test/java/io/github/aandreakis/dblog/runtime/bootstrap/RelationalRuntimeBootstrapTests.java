package io.github.aandreakis.dblog.runtime.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import io.github.aandreakis.dblog.core.schema.SchemaDriftException;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.sink.api.SinkSchemaValidator;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RelationalRuntimeBootstrapTests {
  @TempDir Path tempDir;

  @Test
  void preparesContractSchemasPersistsObservedSchemasAndBuildsRuntimeSession() throws Exception {
    TableSchema legacyContract =
        TableSchema.create(
            new TableId("sourceA", "public", "customers"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-10T00:00:00Z"));
    TableSchema liveSchema =
        TableSchema.create(
            new TableId("sourceA", "public", "customers"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true),
                new ColumnDefinition("legacy_note", "json", NeutralColumnType.UNSUPPORTED, false, true)),
            Instant.parse("2026-04-10T00:05:00Z"));

    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(tempDir.resolve("bootstrap-state"))) {
      stateStore.schemas().saveContractSchema(legacyContract);
      RelationalRuntimeBootstrap bootstrap = new RelationalRuntimeBootstrap();
      FakeAdapter adapter = new FakeAdapter(liveSchema);

      RelationalRuntimeBootstrap.BootstrappedSession bootstrapped =
          bootstrap.open(
              new RelationalRuntimeBootstrap.BootstrapRequest(
                  adapter,
                  new RelationalSourceConfig(
                      "sourceA",
                      "jdbc:fake://localhost/db",
                      "user",
                      "",
                      "db",
                      List.of("public.customers"),
                      java.util.Map.of(),
                      false),
                  stateStore,
                  events -> {}));

      assertThat(bootstrapped.loadedCheckpointDisplayValue()).isEqualTo("checkpoint:loaded");
      assertThat(bootstrapped.liveSchemas()).containsExactly(liveSchema);
      assertThat(bootstrapped.contractSchemas()).hasSize(1);
      assertThat(bootstrapped.contractSchemas().getFirst().selectedColumnNames())
          .containsExactly("id", "name");
      assertThat(bootstrapped.contractSchemas().getFirst().ignoredColumns()).containsExactly("legacy_note");
      assertThat(stateStore.schemas().loadObservedSchema(liveSchema.tableId().displayName()))
          .contains(liveSchema);
      assertThat(stateStore.schemas().loadContractSchema(liveSchema.tableId().displayName()))
          .contains(bootstrapped.contractSchemas().getFirst());
      assertThat(adapter.validated).isTrue();
      assertThat(adapter.openedWithContractSchemas).containsExactly(bootstrapped.contractSchemas().getFirst());
    }
  }

  @Test
  void rejectsSelectedColumnNeutralTypeDriftInsteadOfOverwritingContract() throws Exception {
    TableId tableId = new TableId("sourceA", "public", "customers");
    TableSchema contract =
        TableSchema.create(
            tableId,
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("active", "varchar(5)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-10T00:00:00Z"));
    TableSchema liveSchema =
        TableSchema.create(
            tableId,
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("active", "boolean", NeutralColumnType.BOOLEAN, false, true)),
            Instant.parse("2026-04-10T00:05:00Z"));

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("bootstrap-schema-drift"))) {
      stateStore.schemas().saveContractSchema(contract);
      RelationalRuntimeBootstrap bootstrap = new RelationalRuntimeBootstrap();

      assertThatThrownBy(
              () ->
                  bootstrap.open(
                      new RelationalRuntimeBootstrap.BootstrapRequest(
                          new FakeAdapter(liveSchema),
                          new RelationalSourceConfig(
                              "sourceA",
                              "jdbc:fake://localhost/db",
                              "user",
                              "",
                              "db",
                              List.of("public.customers"),
                              java.util.Map.of(),
                              false),
                          stateStore,
                          events -> {})))
          .isInstanceOf(SchemaDriftException.class);
      assertThat(stateStore.schemas().loadContractSchema(tableId.displayName())).contains(contract);
      assertThat(stateStore.schemas().loadObservedSchema(tableId.displayName())).contains(liveSchema);
      assertThat(stateStore.schemas().loadFullDumpRequiredSignals())
          .anySatisfy(
              signal -> {
                assertThat(signal.sourceId()).isEqualTo("sourceA");
                assertThat(signal.tableId()).isEqualTo(tableId);
                assertThat(signal.reason()).contains("selected-column contract changed");
                assertThat(signal.reason()).contains("full dump required");
              });
    }
  }

  @Test
  void validatesSinkAgainstContractSchemasBeforeOpeningRuntime() throws Exception {
    TableSchema liveSchema =
        TableSchema.create(
            new TableId("sourceA", "public", "customers"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-10T00:05:00Z"));

    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(tempDir.resolve("bootstrap-sink-validation"))) {
      RelationalRuntimeBootstrap bootstrap = new RelationalRuntimeBootstrap();
      FakeAdapter adapter = new FakeAdapter(liveSchema);
      RecordingSink sink = new RecordingSink();

      bootstrap.open(
          new RelationalRuntimeBootstrap.BootstrapRequest(
              adapter,
              new RelationalSourceConfig(
                  "sourceA",
                  "jdbc:fake://localhost/db",
                  "user",
                  "",
                  "db",
                  List.of("public.customers"),
                  java.util.Map.of(),
                  false),
              stateStore,
              sink));

      assertThat(sink.validatedSchemas).containsExactly(liveSchema);
    }
  }

  @Test
  void validatesRetryBackoffBeforeClaimingSourceOwnershipOrOpeningRuntime() throws Exception {
    TableSchema liveSchema =
        TableSchema.create(
            new TableId("sourceA", "public", "customers"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-10T00:05:00Z"));

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("bootstrap-invalid-retry-backoff"))) {
      RelationalRuntimeBootstrap bootstrap = new RelationalRuntimeBootstrap();
      FakeAdapter adapter = new FakeAdapter(liveSchema);

      assertThatThrownBy(
              () ->
                  bootstrap.open(
                      new RelationalRuntimeBootstrap.BootstrapRequest(
                          adapter,
                          new RelationalSourceConfig(
                              "sourceA",
                              "jdbc:fake://localhost/db",
                              "user",
                              "",
                              "db",
                              List.of("public.customers"),
                              java.util.Map.of(
                                  "fake.retryLogConnectionLoss",
                                  "true",
                                  "fake.reconnectBackoff",
                                  "2s"),
                              false),
                          stateStore,
                          events -> {})))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("fake.reconnectBackoff")
          .hasMessageContaining("ISO-8601 duration")
          .hasMessageContaining("PT2S")
          .hasMessageContaining("2s");
      assertThat(adapter.validated).isTrue();
      assertThat(adapter.inspectCount).isZero();
      assertThat(adapter.openCount).isZero();
      assertThatCode(() -> stateStore.ownership().claimSourceOwnership("otherSource"))
          .doesNotThrowAnyException();
    }
  }

  private static final class FakeAdapter implements SourceAdapter {
    private final TableSchema liveSchema;
    private boolean validated;
    private int inspectCount;
    private int openCount;
    private List<TableSchema> openedWithContractSchemas = List.of();

    private FakeAdapter(TableSchema liveSchema) {
      this.liveSchema = liveSchema;
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
    public void validateSourceConfig(RelationalSourceConfig config) {
      validated = true;
    }

    @Override
    public io.github.aandreakis.dblog.adapter.api.SourcePreflight preflight() {
      return (c, s) -> {};
    }

    @Override
    public List<TableSchema> inspectSchemas(RelationalSourceConfig config) {
      inspectCount++;
      return List.of(liveSchema);
    }

    @Override
    public OpenedSourceRuntime<? extends SourceTransaction<?>> openRuntime(
        RelationalSourceConfig config,
        RuntimeStateStore stateStore,
        List<TableSchema> contractSchemas) {
      openCount++;
      openedWithContractSchemas = List.copyOf(contractSchemas);
      return new OpenedSourceRuntime<>(new NoopRuntime(), new NoopChunkReader(), "checkpoint:loaded");
    }
  }

  private static final class NoopRuntime implements SourceRuntime<NoopTransaction> {
    @Override
    public Optional<NoopTransaction> readPendingTransaction() {
      return Optional.empty();
    }

    @Override
    public void acknowledge(NoopTransaction transaction) {}
  }

  private record NoopTransaction(
      String transactionId, SourcePosition checkpointPosition, List<ChangeEvent> events)
      implements SourceTransaction<SourcePosition> {
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

  private static final class RecordingSink
      implements io.github.aandreakis.dblog.sink.api.ChangeEventSink, SinkSchemaValidator {
    private List<TableSchema> validatedSchemas = List.of();

    @Override
    public void appendEvents(List<ChangeEvent> events) {}

    @Override
    public void validateCapturedSchemas(List<TableSchema> capturedSchemas) {
      validatedSchemas = List.copyOf(capturedSchemas);
    }
  }
}
