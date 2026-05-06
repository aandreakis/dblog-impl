package io.github.aandreakis.dblog.controlplane.service;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.core.request.DumpRequestStatus;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.FullDumpRequiredSignal;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.SchemaUncertaintySignal;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import io.github.aandreakis.dblog.testsupport.ChangeEventTestFixtures;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ControlPlaneQueryServiceTests {
  @TempDir Path tempDir;

  @Test
  void loadsRuntimeRequestsSchemasAndSchemaIssuesFromStateStore() {
    Path statePath = tempDir.resolve("next-controlplane-query");
    TableId tableId = new TableId("sourceA", "appdb", "customers");
    TableSchema contractSchema = contractSchema(tableId);
    TableSchema observedSchema = observedSchema(tableId);

    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(statePath)) {
      DumpRequest request = new DumpRequest("42", DumpScope.TABLE, tableId, List.of());
      DumpRequest queued = new DumpRequest("45", DumpScope.ALL_TABLES, null, List.of());
      stateStore.dumpRequests().upsert(request);
      stateStore.dumpRequests().upsert(queued);
      stateStore.dumpRequests().saveStatus(DumpRequestStatus.active(request));
      stateStore.schemas().saveContractSchema(contractSchema);
      stateStore.schemas().saveObservedSchema(observedSchema);
      stateStore.schemas()
          .saveFullDumpRequiredSignal(
              new FullDumpRequiredSignal(
                  "sourceA",
                  tableId,
                  "contract and observed schema drift requires a full dump",
                  Instant.parse("2026-04-01T00:06:00Z")));
      stateStore.schemas()
          .saveSchemaUncertaintySignal(
              new SchemaUncertaintySignal(
                  "sourceA",
                  tableId,
                  "schema continuity is still being reconciled",
                  Instant.parse("2026-04-01T00:07:00Z"),
                  Instant.parse("2026-04-01T00:08:00Z"),
                  2));

      ControlPlaneQueryService queryService =
          new ControlPlaneQueryService(
              stateStore,
              () ->
                  new RuntimeStatusProvider.RuntimeStatusSnapshot(
                      "runtime", "mysql", "UP", true, null),
              eventStore(tableId));

      Map<String, Object> runtime = queryService.runtimePayload();
      assertThat(runtime).containsEntry("mode", "runtime").containsEntry("adapter", "mysql");

      Map<String, Object> requests = queryService.requestsPayload(null, null);
      assertThat((List<?>) requests.get("requests")).hasSize(2);
      @SuppressWarnings("unchecked")
      Map<String, Integer> countsByState = (Map<String, Integer>) requests.get("countsByState");
      assertThat(countsByState)
          .containsEntry("ACTIVE", 1)
          .containsEntry("QUEUED", 1);
      assertThat(String.valueOf(requests))
          .contains("createdAt")
          .contains("updatedAt");

      Map<String, Object> schemas = queryService.runtimeSchemasPayload();
      assertThat(schemas).containsEntry("count", 1);
      assertThat(String.valueOf(schemas)).contains("FULL_DUMP_REQUIRED");
      assertThat(String.valueOf(schemas)).contains("legacy_note");

      Map<String, Object> issues = queryService.runtimeSchemaIssuesPayload();
      assertThat(String.valueOf(issues))
          .contains("contract and observed schema drift requires a full dump")
          .contains("schema continuity is still being reconciled");

      Map<String, Object> recentEvents = queryService.recentEventsPayload(10);
      assertThat(String.valueOf(recentEvents)).contains("request-batch-42");

      Map<String, Object> tableEvents = queryService.tableEventsPayload(tableId.displayName(), 10);
      assertThat(String.valueOf(tableEvents)).contains(tableId.displayName()).contains("source:42");
    }
  }

  @Test
  void exposesRuntimeMeasurementPayloadWhenMeterRegistryIsAvailable() {
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("next-controlplane-query-metrics"))) {
      SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
      try {
      meterRegistry.counter("dblog.runtime.source_capture.polls.total", "adapter", "mysql").increment(3.0d);
      meterRegistry.counter("dblog.runtime.chunking.next_table_chunk.calls.total", "adapter", "mysql").increment(2.0d);
      meterRegistry.counter("dblog.runtime.request_coordinator.coordinate.calls.total", "adapter", "mysql").increment(4.0d);

      ControlPlaneQueryService queryService =
          new ControlPlaneQueryService(
              stateStore,
              () ->
                  new RuntimeStatusProvider.RuntimeStatusSnapshot(
                      "runtime", "mysql", "UP", true, null),
              ControlPlaneEventStore.noop(),
              meterRegistry,
              "mysql");

      Map<String, Object> payload = queryService.metricsPayload();
      assertThat(String.valueOf(payload))
          .contains("sourceCapture")
          .contains("chunking")
          .contains("requestCoordinator")
          .contains("polls=3.0")
          .contains("nextTableChunkCalls=2.0")
          .contains("coordinateCalls=4.0");
      } finally {
        meterRegistry.close();
      }
    }
  }

  private static InMemoryControlPlaneEventStore eventStore(TableId tableId) {
    InMemoryControlPlaneEventStore eventStore = new InMemoryControlPlaneEventStore(10);
    eventStore.record(
        "request-batch-42",
        List.of(
            ChangeEventTestFixtures.fromRowMaps(
                tableId,
                OperationType.UPDATE,
                CaptureOrigin.SELECT,
                java.util.Map.of("id", 42),
                null,
                java.util.Map.of("id", 42, "name", "Alice"),
                new OpaqueSourcePosition("source:42"),
                null,
                "42")));
    return eventStore;
  }

  private static TableSchema contractSchema(TableId tableId) {
    return TableSchema.create(
        tableId,
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
        Instant.parse("2026-04-01T00:00:00Z"));
  }

  private static TableSchema observedSchema(TableId tableId) {
    return TableSchema.create(
        tableId,
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true),
            new ColumnDefinition("legacy_note", "json", NeutralColumnType.UNSUPPORTED, false, true)),
        Instant.parse("2026-04-01T00:05:00Z"));
  }
}
