package io.github.aandreakis.dblog.core.schema;

import java.nio.ByteBuffer;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Small helper that widens decode assumptions only when the current neutral type stops working. */
public final class AdaptiveColumnDecodeSupport {
  private AdaptiveColumnDecodeSupport() {}

  public static DecodedColumn decode(ColumnDefinition column, Object rawValue) {
    Objects.requireNonNull(column, "column");
    if (rawValue == null) {
      return new DecodedColumn(column, null);
    }

    RuntimeException firstFailure = null;
    for (NeutralColumnType candidate : candidateTypes(column, rawValue)) {
      ColumnDefinition candidateColumn =
          candidate == column.neutralType()
              ? column
              : new ColumnDefinition(
                  column.name(),
                  column.sourceType(),
                  candidate,
                  column.primaryKey(),
                  column.primaryKeyOrdinal(),
                  column.nullable());
      try {
        Object normalizedValue = NeutralValueNormalizer.normalize(candidateColumn, rawValue);
        if (candidateColumn.neutralType() == NeutralColumnType.BOOLEAN
            && rawValue instanceof Number number
            && number.longValue() != 0L
            && number.longValue() != 1L) {
          throw new IllegalStateException(
              "numeric value no longer matches strict boolean decode family: " + rawValue);
        }
        if (!isValidNormalizedValue(candidateColumn.neutralType(), normalizedValue)) {
          throw new IllegalStateException(
              "normalized value no longer matches decode family "
                  + candidateColumn.neutralType()
                  + ": "
                  + normalizedValue);
        }
        return new DecodedColumn(candidateColumn, normalizedValue);
      } catch (RuntimeException failure) {
        if (firstFailure == null) {
          firstFailure = failure;
        }
      }
    }

    throw Objects.requireNonNull(firstFailure, "firstFailure");
  }

  private static List<NeutralColumnType> candidateTypes(ColumnDefinition column, Object rawValue) {
    LinkedHashSet<NeutralColumnType> candidates = new LinkedHashSet<>();
    candidates.add(column.neutralType());
    candidates.add(inferNeutralType(rawValue));
    addFallbacks(candidates, column.neutralType());
    addFallbacks(candidates, inferNeutralType(rawValue));
    candidates.add(NeutralColumnType.STRING);
    return List.copyOf(candidates);
  }

  private static void addFallbacks(Set<NeutralColumnType> candidates, NeutralColumnType type) {
    if (type == null) {
      return;
    }
    switch (type) {
      case INTEGER -> {
        candidates.add(NeutralColumnType.DECIMAL);
        candidates.add(NeutralColumnType.STRING);
      }
      case FLOAT, DECIMAL, BOOLEAN, UUID, DATE, TIME, TIMESTAMP, BINARY, UNSUPPORTED ->
          candidates.add(NeutralColumnType.STRING);
      default -> {
        // STRING-like families already tolerate broad values.
      }
    }
  }

  private static NeutralColumnType inferNeutralType(Object rawValue) {
    if (rawValue == null) {
      return NeutralColumnType.UNSUPPORTED;
    }
    if (rawValue instanceof Boolean) {
      return NeutralColumnType.BOOLEAN;
    }
    if (rawValue instanceof Byte
        || rawValue instanceof Short
        || rawValue instanceof Integer
        || rawValue instanceof Long
        || rawValue instanceof java.math.BigInteger) {
      return NeutralColumnType.INTEGER;
    }
    if (rawValue instanceof Float
        || rawValue instanceof Double
        || rawValue instanceof java.math.BigDecimal) {
      return NeutralColumnType.DECIMAL;
    }
    if (rawValue instanceof byte[] || rawValue instanceof ByteBuffer) {
      return NeutralColumnType.BINARY;
    }
    if (rawValue instanceof java.time.Instant
        || rawValue instanceof java.time.OffsetDateTime
        || rawValue instanceof java.time.LocalDateTime
        || rawValue instanceof java.sql.Timestamp
        || rawValue instanceof java.util.Date) {
      return NeutralColumnType.TIMESTAMP;
    }
    if (rawValue instanceof java.time.LocalDate || rawValue instanceof java.sql.Date) {
      return NeutralColumnType.DATE;
    }
    if (rawValue instanceof java.time.LocalTime || rawValue instanceof java.sql.Time) {
      return NeutralColumnType.TIME;
    }
    if (rawValue instanceof java.util.UUID) {
      return NeutralColumnType.UUID;
    }
    if (rawValue instanceof String text) {
      String normalized = text.trim();
      if (normalized.isEmpty()) {
        return NeutralColumnType.STRING;
      }
      if (normalized.equalsIgnoreCase("true")
          || normalized.equalsIgnoreCase("false")
          || normalized.equalsIgnoreCase("t")
          || normalized.equalsIgnoreCase("f")
          || normalized.equals("1")
          || normalized.equals("0")) {
        return NeutralColumnType.BOOLEAN;
      }
      try {
        new java.math.BigInteger(normalized);
        return NeutralColumnType.INTEGER;
      } catch (RuntimeException ignored) {
      }
      try {
        new java.math.BigDecimal(normalized);
        return NeutralColumnType.DECIMAL;
      } catch (RuntimeException ignored) {
      }
      try {
        java.util.UUID.fromString(normalized);
        return NeutralColumnType.UUID;
      } catch (RuntimeException ignored) {
      }
      try {
        java.time.Instant.parse(normalized);
        return NeutralColumnType.TIMESTAMP;
      } catch (RuntimeException ignored) {
      }
      try {
        java.time.OffsetDateTime.parse(normalized.replace(' ', 'T'));
        return NeutralColumnType.TIMESTAMP;
      } catch (RuntimeException ignored) {
      }
      try {
        java.time.LocalDateTime.parse(normalized.replace(' ', 'T'));
        return NeutralColumnType.TIMESTAMP;
      } catch (RuntimeException ignored) {
      }
      try {
        java.time.LocalDate.parse(normalized);
        return NeutralColumnType.DATE;
      } catch (RuntimeException ignored) {
      }
      try {
        java.time.LocalTime.parse(normalized);
        return NeutralColumnType.TIME;
      } catch (RuntimeException ignored) {
      }
      return NeutralColumnType.STRING;
    }
    return NeutralColumnType.STRING;
  }

  private static boolean isValidNormalizedValue(
      NeutralColumnType neutralType, Object normalizedValue) {
    if (normalizedValue == null) {
      return true;
    }
    return switch (neutralType) {
      case BOOLEAN -> normalizedValue instanceof Boolean;
      case INTEGER -> normalizedValue instanceof Long || normalizedValue instanceof java.math.BigInteger;
      case FLOAT, DECIMAL -> normalizedValue instanceof java.math.BigDecimal;
      case STRING, ENUM_STRING, JSON, XML -> normalizedValue instanceof String;
      case BINARY -> normalizedValue instanceof byte[];
      case DATE -> normalizedValue instanceof java.time.LocalDate;
      case TIME -> normalizedValue instanceof java.time.LocalTime;
      case TIMESTAMP ->
          normalizedValue instanceof java.time.Instant
              || normalizedValue instanceof java.time.LocalDateTime;
      case UUID -> normalizedValue instanceof java.util.UUID;
      case UNSUPPORTED -> true;
    };
  }

  public record DecodedColumn(ColumnDefinition column, Object value) {
    public DecodedColumn {
      column = Objects.requireNonNull(column, "column");
    }
  }
}
