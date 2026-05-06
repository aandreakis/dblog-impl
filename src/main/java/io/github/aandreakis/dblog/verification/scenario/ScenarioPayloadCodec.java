package io.github.aandreakis.dblog.verification.scenario;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** Stable text encoding for scenario sink rows and event payloads. */
public final class ScenarioPayloadCodec {
  private ScenarioPayloadCodec() {}

  public static String encodeRow(Map<String, Object> payload) {
    Objects.requireNonNull(payload, "payload");
    LinkedHashMap<String, Object> ordered = new LinkedHashMap<>(payload);
    StringBuilder builder = new StringBuilder();
    builder.append('{');
    boolean first = true;
    for (Map.Entry<String, Object> entry : ordered.entrySet()) {
      if (!first) {
        builder.append(',');
      }
      first = false;
      builder.append(escape(entry.getKey()));
      builder.append('=');
      builder.append(encodeValue(entry.getValue()));
    }
    builder.append('}');
    return builder.toString();
  }

  public static String encodeValue(Object value) {
    if (value == null) {
      return "null";
    }
    if (value instanceof byte[] bytes) {
      return "hex:" + HexFormat.of().formatHex(bytes);
    }
    if (value instanceof String stringValue) {
      return "str:" + escape(stringValue);
    }
    if (value instanceof Boolean
        || value instanceof Long
        || value instanceof Integer
        || value instanceof Short
        || value instanceof Byte
        || value instanceof BigInteger
        || value instanceof BigDecimal
        || value instanceof Instant
        || value instanceof LocalDate
        || value instanceof LocalTime
        || value instanceof LocalDateTime
        || value instanceof UUID) {
      return value.getClass().getSimpleName() + ":" + escape(String.valueOf(value));
    }
    return value.getClass().getName() + ":" + escape(String.valueOf(value));
  }

  private static String escape(String value) {
    return value
        .replace("\\", "\\\\")
        .replace(",", "\\,")
        .replace("=", "\\=")
        .replace("{", "\\{")
        .replace("}", "\\}");
  }
}
