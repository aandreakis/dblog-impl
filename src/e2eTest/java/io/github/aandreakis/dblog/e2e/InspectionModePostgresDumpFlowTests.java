package io.github.aandreakis.dblog.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.postgres.PostgresSourceAdapter;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.core.request.DumpRequestState;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.e2e.support.PostgresInspectionOnlyDependencies;
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

class InspectionModePostgresDumpFlowTests {
  @TempDir Path tempDir;

  @Test
  void processesATableDumpRequestEndToEndThroughTheIntegratedInspectionStack() throws Exception {
    String h2Jdbc = "jdbc:h2:mem:phase16_postgres;MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
    TableSchema schema =
        TableSchema.create(
            new io.github.aandreakis.dblog.core.model.TableId("appdb", "public", "customers"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-10T00:00:00Z"));
    try (Connection connection = DriverManager.getConnection(h2Jdbc)) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("CREATE SCHEMA IF NOT EXISTS \"public\"");
        statement.execute("CREATE TABLE \"public\".\"customers\" (\"id\" BIGINT PRIMARY KEY, \"name\" VARCHAR(255))");
        statement.execute("INSERT INTO \"public\".\"customers\" (\"id\", \"name\") VALUES (1, 'one'), (2, 'two')");
      }
    }

    RecordingSink sink = new RecordingSink();
    PostgresSourceAdapter adapter =
        new PostgresSourceAdapter(
            PostgresInspectionOnlyDependencies.withFixedSchema(h2Jdbc, schema));

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

    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(tempDir.resolve("phase16-state"))) {
      RelationalRuntimeBootstrap bootstrap = new RelationalRuntimeBootstrap();
      RelationalRuntimeBootstrap.BootstrappedSession bootstrapped =
          bootstrap.open(new RelationalRuntimeBootstrap.BootstrapRequest(adapter, config, stateStore, sink));

      DumpRequest request =
          new DumpRequest("42", DumpScope.TABLE, bootstrapped.contractSchemas().getFirst().tableId(), List.of());
      stateStore.dumpRequests().upsert(request);

      RelationalRuntimeAssembly.forBootstrappedSession(bootstrapped, stateStore, "PostgreSQL", "sourceA", 100)
          .processPendingRequests(Duration.ofMillis(5));

      assertThat(stateStore.dumpRequests().loadStatus("42")).isPresent();
      assertThat(stateStore.dumpRequests().loadStatus("42").orElseThrow().state())
          .isEqualTo(DumpRequestState.COMPLETED);
      assertThat(sink.emittedEvents).hasSize(2);
      assertThat(sink.emittedEvents)
          .extracting(ChangeEvent::captureOrigin)
          .containsOnly(io.github.aandreakis.dblog.core.model.CaptureOrigin.SELECT);
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
