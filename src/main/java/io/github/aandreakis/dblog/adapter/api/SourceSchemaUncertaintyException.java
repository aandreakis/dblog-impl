package io.github.aandreakis.dblog.adapter.api;

import io.github.aandreakis.dblog.core.model.TableId;
import java.util.Objects;

/** Fail-closed adapter signal for source metadata events that make schema/control state uncertain. */
public final class SourceSchemaUncertaintyException extends IllegalStateException {
  private final TableId tableId;

  private SourceSchemaUncertaintyException(TableId tableId, String reason) {
    this(tableId, reason, null);
  }

  private SourceSchemaUncertaintyException(TableId tableId, String reason, Throwable cause) {
    super(requireNonBlank(reason, "reason"), cause);
    this.tableId = tableId;
  }

  public static SourceSchemaUncertaintyException sourceLevel(String reason) {
    return new SourceSchemaUncertaintyException(null, reason);
  }

  public static SourceSchemaUncertaintyException sourceLevel(String reason, Throwable cause) {
    return new SourceSchemaUncertaintyException(
        null, reason, Objects.requireNonNull(cause, "cause"));
  }

  public static SourceSchemaUncertaintyException forTable(TableId tableId, String reason) {
    return new SourceSchemaUncertaintyException(
        Objects.requireNonNull(tableId, "tableId"), reason);
  }

  public static SourceSchemaUncertaintyException forTable(
      TableId tableId, String reason, Throwable cause) {
    return new SourceSchemaUncertaintyException(
        Objects.requireNonNull(tableId, "tableId"),
        reason,
        Objects.requireNonNull(cause, "cause"));
  }

  public TableId tableId() {
    return tableId;
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
