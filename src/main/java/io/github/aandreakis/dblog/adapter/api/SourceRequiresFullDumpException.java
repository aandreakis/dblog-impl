package io.github.aandreakis.dblog.adapter.api;

import io.github.aandreakis.dblog.core.model.TableId;
import java.util.Objects;

/** Fail-closed adapter signal for source events that require operator rebootstrap. */
public final class SourceRequiresFullDumpException extends IllegalStateException {
  private final TableId tableId;

  private SourceRequiresFullDumpException(TableId tableId, String reason) {
    this(tableId, reason, null);
  }

  private SourceRequiresFullDumpException(TableId tableId, String reason, Throwable cause) {
    super(requireNonBlank(reason, "reason"), cause);
    this.tableId = tableId;
  }

  public static SourceRequiresFullDumpException sourceLevel(String reason) {
    return new SourceRequiresFullDumpException(null, reason);
  }

  public static SourceRequiresFullDumpException sourceLevel(String reason, Throwable cause) {
    return new SourceRequiresFullDumpException(
        null, reason, Objects.requireNonNull(cause, "cause"));
  }

  public static SourceRequiresFullDumpException forTable(TableId tableId, String reason) {
    return new SourceRequiresFullDumpException(
        Objects.requireNonNull(tableId, "tableId"), reason);
  }

  public static SourceRequiresFullDumpException forTable(
      TableId tableId, String reason, Throwable cause) {
    return new SourceRequiresFullDumpException(
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
