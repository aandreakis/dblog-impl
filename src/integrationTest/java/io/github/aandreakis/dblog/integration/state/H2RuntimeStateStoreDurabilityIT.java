package io.github.aandreakis.dblog.integration.state;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.core.request.DumpRequestState;
import io.github.aandreakis.dblog.core.request.DumpRequestStatus;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.request.DumpTableProgress;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.FullDumpRequiredSignal;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.SchemaUncertaintySignal;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag("integration-core")
class H2RuntimeStateStoreDurabilityIT {
  @TempDir Path tempDir;

  @Test
  void preservesDurableStateAcrossReopenAndPkInvalidationAtTheFacadeLevel() {
    Path databasePath = tempDir.resolve("next-state-it");
    TableSchema contractSchema = contractSchema();
    TableSchema observedSchema = observedSchema();
    TableId tableId = contractSchema.tableId();
    DumpRequest tableRequest = new DumpRequest("101", DumpScope.TABLE, tableId, List.of());
    DumpRequest allTablesRequest = new DumpRequest("102", DumpScope.ALL_TABLES, null, List.of());
    DumpRequest primaryKeysRequest =
        DumpRequest.fromPrimaryKeyLiterals(
            "103", DumpScope.PRIMARY_KEYS, tableId, contractSchema, List.of("1"));

    try (H2RuntimeStateStore store = new H2RuntimeStateStore(databasePath)) {
      store.ownership().claimSourceOwnership("source");
      store.streamPositions().saveCheckpoint("source", new OpaqueSourcePosition("checkpoint-1"));
      store.streamPositions().saveBootstrapPosition("source", new OpaqueSourcePosition("bootstrap-1"));
      store.dumpRequests().upsert(tableRequest);
      store.dumpRequests().upsert(allTablesRequest);
      store.dumpRequests().upsert(primaryKeysRequest);
      store.dumpRequests().saveStatus(DumpRequestStatus.active(tableRequest));
      store.dumpRequests().saveStatus(DumpRequestStatus.active(primaryKeysRequest));
      store.dumpProgress()
          .save(
              DumpTableProgress.initial(
                      tableRequest.requestId(), tableId.displayName(), contractSchema.fingerprint())
                  .captureRequestUpperBound(contractSchema, "10"));
      store.schemas().saveContractSchema(contractSchema);
      store.schemas().saveObservedSchema(observedSchema);
      store.schemas()
          .saveFullDumpRequiredSignal(
              new FullDumpRequiredSignal(
                  "source",
                  tableId,
                  "full dump required because data loss was detected",
                  Instant.parse("2026-03-29T00:06:00Z")));
      store.schemas()
          .saveSchemaUncertaintySignal(
              new SchemaUncertaintySignal(
                  "source",
                  tableId,
                  "schema continuity uncertain",
                  Instant.parse("2026-03-29T00:00:00Z"),
                  Instant.parse("2026-03-29T00:05:00Z"),
                  2));
    }

    try (H2RuntimeStateStore reopened = new H2RuntimeStateStore(databasePath)) {
      reopened.ownership().claimSourceOwnership("source");
      assertThat(reopened.streamPositions().loadCheckpoint("source"))
          .contains(new OpaqueSourcePosition("checkpoint-1"));
      assertThat(reopened.streamPositions().loadBootstrapPosition("source")).isEmpty();
      assertThat(reopened.dumpProgress().load(tableRequest.requestId(), tableId.displayName()))
          .isPresent();
      assertThat(reopened.dumpRequests().loadStatus(tableRequest.requestId()))
          .contains(DumpRequestStatus.active(tableRequest));
      assertThat(reopened.dumpRequests().loadStatus(primaryKeysRequest.requestId()))
          .contains(DumpRequestStatus.active(primaryKeysRequest));
      assertThat(reopened.schemas().loadContractSchema(tableId.displayName())).contains(contractSchema);
      assertThat(reopened.schemas().loadObservedSchema(tableId.displayName())).contains(observedSchema);
      assertThat(reopened.schemas().loadFullDumpRequiredSignals())
          .extracting(FullDumpRequiredSignal::reason)
          .contains("full dump required because data loss was detected");
      assertThat(reopened.schemas().loadSchemaUncertaintySignals()).hasSize(1);

      reopened.invalidateRuntimeStateForPkChange(
          "source", "primary key changed and dump progress was invalidated");
    }

    try (H2RuntimeStateStore reopened = new H2RuntimeStateStore(databasePath)) {
      assertThat(reopened.streamPositions().loadCheckpoint("source"))
          .contains(new OpaqueSourcePosition("checkpoint-1"));
      assertThat(reopened.dumpProgress().load(tableRequest.requestId(), tableId.displayName()))
          .isEmpty();
      assertThat(reopened.dumpRequests().loadStatus(tableRequest.requestId()).orElseThrow().state())
          .isEqualTo(DumpRequestState.FAILED);
      assertThat(
              reopened.dumpRequests().loadStatus(allTablesRequest.requestId()).orElseThrow().state())
          .isEqualTo(DumpRequestState.FAILED);
      assertThat(
              reopened.dumpRequests()
                  .loadStatus(primaryKeysRequest.requestId())
                  .orElseThrow()
                  .state())
          .isEqualTo(DumpRequestState.FAILED);
      assertThat(reopened.schemas().loadSchemaUncertaintySignals()).isEmpty();
      assertThat(reopened.schemas().loadObservedSchema(tableId.displayName())).contains(observedSchema);
    }
  }

  private static TableSchema contractSchema() {
    return TableSchema.create(
        new TableId("source", "appdb", "widgets"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("name", "bigint", NeutralColumnType.INTEGER, false, true)),
        Instant.parse("2026-03-29T00:00:00Z"));
  }

  private static TableSchema observedSchema() {
    return TableSchema.create(
        new TableId("source", "appdb", "widgets"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true),
            new ColumnDefinition("note", "json", NeutralColumnType.UNSUPPORTED, false, true)),
        Instant.parse("2026-03-29T00:05:00Z"));
  }
}
