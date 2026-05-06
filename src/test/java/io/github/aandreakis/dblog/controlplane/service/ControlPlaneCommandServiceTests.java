package io.github.aandreakis.dblog.controlplane.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ControlPlaneCommandServiceTests {
  @TempDir Path tempDir;

  @Test
  void submitsQueuedRequestThroughGateway() {
    Path statePath = tempDir.resolve("next-controlplane-command");

    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(statePath)) {
      TableSchema schema =
          TableSchema.create(
              new TableId("sourceA", "appdb", "customers"),
              List.of(
                  new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                  new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
              Instant.parse("2026-04-14T00:00:00Z"));
      stateStore.schemas().saveContractSchema(schema);
      ControlPlaneCommandService commandService =
          new ControlPlaneCommandService(
              stateStore,
              () ->
                  new RuntimeStatusProvider.RuntimeStatusSnapshot(
                      "runtime", "mysql", "UP", true, null),
              (scope, tableId, primaryKeyLiterals) ->
                  stateStore.dumpRequests().createGenerated(scope, tableId, primaryKeyLiterals));

      DumpRequest submitted =
          commandService.submit(
              new ControlPlaneCommandService.RequestPayload(
                  "PRIMARY_KEYS",
                  new ControlPlaneCommandService.TablePayload("sourceA", "appdb", "customers"),
                  List.of("42", "99")));

      assertThat(submitted.scope()).isEqualTo(DumpScope.PRIMARY_KEYS);
      assertThat(submitted.requestId()).isNotBlank();
    }
  }

  @Test
  void rejectsSubmissionWhenRuntimeMarksRequestSubmissionUnavailable() {
    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(tempDir.resolve("next-controlplane-unavailable"))) {
      ControlPlaneCommandService commandService =
          new ControlPlaneCommandService(
              stateStore,
              RuntimeStatusProvider.idle(),
              (scope, tableId, primaryKeyLiterals) ->
                  stateStore.dumpRequests().createGenerated(scope, tableId, primaryKeyLiterals));

      assertThatThrownBy(
              () ->
                  commandService.submit(
                      new ControlPlaneCommandService.RequestPayload(
                          "ALL_TABLES", null, List.of())))
          .isInstanceOf(ControlPlaneCommandService.RequestSubmissionUnavailableException.class);
    }
  }

  // Scope-shape validation. These tests lock the contract in AGENTS.md §7:
  //   ALL_TABLES   — must NOT carry table, must NOT carry primaryKeyLiterals
  //   TABLE        — MUST carry table, must NOT carry primaryKeyLiterals
  //   PRIMARY_KEYS — MUST carry table, MUST carry ≥1 primaryKeyLiterals
  // IllegalArgumentException here is what the HTTP layer maps to 400 bad_request; weakening
  // the switch in ControlPlaneCommandService.validateAndParseTable would regress that contract.

  @Test
  void rejectsAllTablesSubmissionWhenTableIsProvided() {
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("controlplane-shape-alltables-table"))) {
      ControlPlaneCommandService commandService = newAvailableCommandService(stateStore);
      assertThatThrownBy(
              () ->
                  commandService.submit(
                      new ControlPlaneCommandService.RequestPayload(
                          "ALL_TABLES",
                          new ControlPlaneCommandService.TablePayload("sourceA", "appdb", "customers"),
                          List.of())))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("ALL_TABLES requests must not include table");
    }
  }

  @Test
  void rejectsAllTablesSubmissionWhenPrimaryKeyLiteralsAreProvided() {
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("controlplane-shape-alltables-pks"))) {
      ControlPlaneCommandService commandService = newAvailableCommandService(stateStore);
      assertThatThrownBy(
              () ->
                  commandService.submit(
                      new ControlPlaneCommandService.RequestPayload(
                          "ALL_TABLES", null, List.of("42"))))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("ALL_TABLES requests must not include primaryKeyLiterals");
    }
  }

  @Test
  void rejectsTableSubmissionWhenTableIsAbsent() {
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("controlplane-shape-table-missing-table"))) {
      ControlPlaneCommandService commandService = newAvailableCommandService(stateStore);
      assertThatThrownBy(
              () ->
                  commandService.submit(
                      new ControlPlaneCommandService.RequestPayload("TABLE", null, List.of())))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("TABLE requests must include table");
    }
  }

  @Test
  void rejectsTableSubmissionWhenPrimaryKeyLiteralsAreProvided() {
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("controlplane-shape-table-with-pks"))) {
      ControlPlaneCommandService commandService = newAvailableCommandService(stateStore);
      assertThatThrownBy(
              () ->
                  commandService.submit(
                      new ControlPlaneCommandService.RequestPayload(
                          "TABLE",
                          new ControlPlaneCommandService.TablePayload("sourceA", "appdb", "customers"),
                          List.of("42"))))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("TABLE requests must not include primaryKeyLiterals");
    }
  }

  @Test
  void rejectsPrimaryKeysSubmissionWhenTableIsAbsent() {
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("controlplane-shape-pks-missing-table"))) {
      ControlPlaneCommandService commandService = newAvailableCommandService(stateStore);
      assertThatThrownBy(
              () ->
                  commandService.submit(
                      new ControlPlaneCommandService.RequestPayload(
                          "PRIMARY_KEYS", null, List.of("42"))))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("PRIMARY_KEYS requests must include table");
    }
  }

  @Test
  void rejectsPrimaryKeysSubmissionWhenPrimaryKeyLiteralsAreAbsent() {
    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("controlplane-shape-pks-missing-pks"))) {
      ControlPlaneCommandService commandService = newAvailableCommandService(stateStore);
      assertThatThrownBy(
              () ->
                  commandService.submit(
                      new ControlPlaneCommandService.RequestPayload(
                          "PRIMARY_KEYS",
                          new ControlPlaneCommandService.TablePayload("sourceA", "appdb", "customers"),
                          List.of())))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(
              "PRIMARY_KEYS requests must include at least one primaryKeyLiterals entry");
    }
  }

  private static ControlPlaneCommandService newAvailableCommandService(H2RuntimeStateStore stateStore) {
    return new ControlPlaneCommandService(
        stateStore,
        () -> new RuntimeStatusProvider.RuntimeStatusSnapshot("runtime", "mysql", "UP", true, null),
        (scope, tableId, primaryKeyLiterals) ->
            stateStore.dumpRequests().createGenerated(scope, tableId, primaryKeyLiterals));
  }
}
