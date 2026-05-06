package io.github.aandreakis.dblog.support;

import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.reconcile.WatermarkMetadata;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public final class NextTestFixtures {
  private NextTestFixtures() {}

  public static TableSchema schema() {
    return TableSchema.create(
        new TableId("app", "public", "users"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true)),
        Instant.parse("2026-03-17T00:00:00Z"));
  }

  public static TableSchema ordersSchema() {
    return TableSchema.create(
        new TableId("app", "public", "orders"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true)),
        Instant.parse("2026-03-17T00:00:04Z"));
  }

  public static Map<String, Object> row(String id, String name) {
    return Map.of("id", id, "name", name);
  }

  public static ChangeEvent watermark(TableId tableId, String token) {
    return new ChangeEvent(
        WatermarkMetadata.tableIdFor(tableId.databaseName()),
        OperationType.WATERMARK,
        CaptureOrigin.LOG,
        WatermarkMetadata.singletonPrimaryKey(),
        null,
        ImmutableRowImage.of(Map.of(WatermarkMetadata.TOKEN_COLUMN, token)),
        new OpaqueSourcePosition("wm:" + token),
        null,
        null);
  }

  public static ChangeEvent logUpdate(TableId tableId, String id, String name, long pos) {
    return new ChangeEvent(
        tableId,
        OperationType.UPDATE,
        CaptureOrigin.LOG,
        ImmutableRowImage.of(Map.of("id", id)),
        ImmutableRowImage.of(Map.of("id", id, "name", "old-" + name)),
        ImmutableRowImage.of(Map.of("id", id, "name", name)),
        new OpaqueSourcePosition("lsn:" + pos),
        "tx-" + pos,
        null);
  }

  public static ChangeEvent logInsert(TableId tableId, String id, String name, long pos) {
    return new ChangeEvent(
        tableId,
        OperationType.INSERT,
        CaptureOrigin.LOG,
        ImmutableRowImage.of(Map.of("id", id)),
        null,
        ImmutableRowImage.of(Map.of("id", id, "name", name)),
        new OpaqueSourcePosition("lsn:" + pos),
        "tx-" + pos,
        null);
  }

  public static ChangeEvent logDelete(TableId tableId, String id, long pos) {
    return new ChangeEvent(
        tableId,
        OperationType.DELETE,
        CaptureOrigin.LOG,
        ImmutableRowImage.of(Map.of("id", id)),
        ImmutableRowImage.of(Map.of("id", id, "name", "gone")),
        null,
        new OpaqueSourcePosition("lsn:" + pos),
        "tx-" + pos,
        null);
  }
}
