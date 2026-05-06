package io.github.aandreakis.dblog.sink.ndjson;

import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import io.github.aandreakis.dblog.core.model.TableId;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public final class NdjsonChangeEventEncoder {
  public String encode(ChangeEvent event) {
    Objects.requireNonNull(event, "event");
    StringBuilder builder = new StringBuilder();
    builder.append('{');
    boolean first = true;
    first = appendField(builder, "tableId", event.tableId(), first);
    first = appendField(builder, "operationType", event.operationType().name(), first);
    first = appendField(builder, "captureOrigin", event.captureOrigin().name(), first);
    first = appendField(builder, "primaryKey", event.primaryKey(), first);
    first = appendField(builder, "beforeRow", event.beforeRow(), first);
    first = appendField(builder, "afterRow", event.afterRow(), first);
    first =
        appendField(
            builder,
            "sourcePosition",
            event.sourcePosition() == null ? null : event.sourcePosition().displayValue(),
            first);
    first = appendField(builder, "transactionId", event.transactionId(), first);
    appendField(builder, "dumpId", event.dumpId(), first);
    builder.append('}');
    return builder.toString();
  }

  private static boolean appendField(
      StringBuilder builder, String name, Object value, boolean first) {
    if (!first) {
      builder.append(',');
    }
    appendString(builder, name);
    builder.append(':');
    appendValue(builder, value);
    return false;
  }

  @SuppressWarnings("unchecked")
  private static void appendValue(StringBuilder builder, Object value) {
    if (value == null) {
      builder.append("null");
      return;
    }
    if (value instanceof String stringValue) {
      appendString(builder, stringValue);
      return;
    }
    if (value instanceof Boolean
        || value instanceof Integer
        || value instanceof Long
        || value instanceof Short
        || value instanceof Byte
        || value instanceof Float
        || value instanceof Double
        || value instanceof BigInteger
        || value instanceof BigDecimal) {
      builder.append(String.valueOf(value));
      return;
    }
    if (value instanceof byte[] bytes) {
      appendString(builder, "hex:" + HexFormat.of().formatHex(bytes));
      return;
    }
    if (value instanceof Instant
        || value instanceof LocalDate
        || value instanceof LocalTime
        || value instanceof LocalDateTime
        || value instanceof UUID
        || value instanceof Enum<?>) {
      appendString(builder, String.valueOf(value));
      return;
    }
    if (value instanceof TableId tableId) {
      builder.append('{');
      boolean first = true;
      first = appendField(builder, "databaseName", tableId.databaseName(), first);
      first = appendField(builder, "schemaName", tableId.schemaName(), first);
      first = appendField(builder, "tableName", tableId.tableName(), first);
      appendField(builder, "displayName", tableId.displayName(), first);
      builder.append('}');
      return;
    }
    if (value instanceof ImmutableRowImage rowImage) {
      builder.append('{');
      boolean[] first = {true};
      rowImage.forEach((column, columnValue) -> first[0] = appendField(builder, column, columnValue, first[0]));
      builder.append('}');
      return;
    }
    if (value instanceof Map<?, ?> mapValue) {
      builder.append('{');
      boolean first = true;
      for (Map.Entry<String, Object> entry : ((Map<String, Object>) mapValue).entrySet()) {
        first = appendField(builder, entry.getKey(), entry.getValue(), first);
      }
      builder.append('}');
      return;
    }
    appendString(builder, String.valueOf(value));
  }

  private static void appendString(StringBuilder builder, String value) {
    builder.append('"');
    for (int index = 0; index < value.length(); index++) {
      char ch = value.charAt(index);
      switch (ch) {
        case '"' -> builder.append("\\\"");
        case '\\' -> builder.append("\\\\");
        case '\b' -> builder.append("\\b");
        case '\f' -> builder.append("\\f");
        case '\n' -> builder.append("\\n");
        case '\r' -> builder.append("\\r");
        case '\t' -> builder.append("\\t");
        default -> {
          if (ch < 0x20) {
            builder.append(String.format("\\u%04x", (int) ch));
          } else {
            builder.append(ch);
          }
        }
      }
    }
    builder.append('"');
  }
}
