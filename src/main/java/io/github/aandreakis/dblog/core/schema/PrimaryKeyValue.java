package io.github.aandreakis.dblog.core.schema;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.HexFormat;
import java.util.Objects;
import java.util.UUID;

public final class PrimaryKeyValue implements Comparable<PrimaryKeyValue> {
  private final NeutralColumnType neutralType;
  private final Object normalizedValue;
  private final String literal;

  private PrimaryKeyValue(NeutralColumnType neutralType, Object normalizedValue, String literal) {
    this.neutralType = Objects.requireNonNull(neutralType, "neutralType");
    this.normalizedValue = copyIfNeeded(normalizedValue);
    this.literal = Objects.requireNonNull(literal, "literal");
  }

  public static boolean isSupportedPrimaryKeyType(NeutralColumnType neutralType) {
    return switch (Objects.requireNonNull(neutralType, "neutralType")) {
      case BOOLEAN, INTEGER, FLOAT, DECIMAL, STRING, BINARY, DATE, TIME, TIMESTAMP, UUID,
          ENUM_STRING -> true;
      case JSON, XML, UNSUPPORTED -> false;
    };
  }

  public static PrimaryKeyValue fromColumn(ColumnDefinition column, Object rawValue) {
    Objects.requireNonNull(column, "column");
    return normalize(column.neutralType(), rawValue);
  }

  /**
   * Hot-path helper used by {@link io.github.aandreakis.dblog.core.model.PrimaryKeyHash}
   * and reconciler lookups: returns the normalised {@link Object} for a raw primary-key value
   * without allocating the {@link PrimaryKeyValue} wrapper, the literal {@link String}, or the
   * defensive {@code copyIfNeeded} roundtrips. Matches the normalisation applied by
   * {@link #fromColumn(ColumnDefinition, Object)} so hash equality holds across code paths.
   */
  public static Object normalizedValueFor(ColumnDefinition column, Object rawValue) {
    Objects.requireNonNull(column, "column");
    Objects.requireNonNull(rawValue, "rawValue");
    return switch (column.neutralType()) {
      case BOOLEAN -> normalizeBoolean(rawValue);
      case INTEGER -> normalizeInteger(rawValue);
      case FLOAT, DECIMAL -> normalizeDecimal(rawValue);
      case STRING, ENUM_STRING -> normalizeString(rawValue);
      case BINARY -> normalizeBinary(rawValue);
      case DATE -> normalizeDate(rawValue);
      case TIME -> normalizeTime(rawValue);
      case TIMESTAMP -> normalizeTimestamp(rawValue);
      case UUID -> normalizeUuid(rawValue);
      case JSON, XML, UNSUPPORTED -> throw unsupportedType(column.neutralType());
    };
  }

  public static PrimaryKeyValue fromLiteral(NeutralColumnType neutralType, String literal) {
    Objects.requireNonNull(neutralType, "neutralType");
    Objects.requireNonNull(literal, "literal");
    return normalize(neutralType, literal);
  }

  public static PrimaryKeyValue fromLiteral(ColumnDefinition column, String literal) {
    Objects.requireNonNull(literal, "literal");
    return fromColumn(column, literal);
  }

  public NeutralColumnType neutralType() {
    return neutralType;
  }

  public String literal() {
    return literal;
  }

  public Object normalizedValue() {
    return copyIfNeeded(normalizedValue);
  }

  @Override
  public int compareTo(PrimaryKeyValue other) {
    Objects.requireNonNull(other, "other");
    if (neutralType != other.neutralType) {
      throw new IllegalArgumentException(
          "cannot compare different primary key types: %s vs %s"
              .formatted(neutralType, other.neutralType));
    }
    return switch (neutralType) {
      case BOOLEAN -> ((Boolean) normalizedValue).compareTo((Boolean) other.normalizedValue);
      case INTEGER -> ((BigInteger) normalizedValue).compareTo((BigInteger) other.normalizedValue);
      case FLOAT, DECIMAL ->
          ((BigDecimal) normalizedValue).compareTo((BigDecimal) other.normalizedValue);
      case STRING, ENUM_STRING -> ((String) normalizedValue).compareTo((String) other.normalizedValue);
      case BINARY -> compareBinary((byte[]) normalizedValue, (byte[]) other.normalizedValue);
      case DATE -> ((LocalDate) normalizedValue).compareTo((LocalDate) other.normalizedValue);
      case TIME -> ((LocalTime) normalizedValue).compareTo((LocalTime) other.normalizedValue);
      case TIMESTAMP -> compareTimestamp(normalizedValue, other.normalizedValue);
      case UUID -> compareUuid((UUID) normalizedValue, (UUID) other.normalizedValue);
      case JSON, XML, UNSUPPORTED -> throw unsupportedType(neutralType);
    };
  }

  private static PrimaryKeyValue normalize(NeutralColumnType neutralType, Object rawValue) {
    Objects.requireNonNull(rawValue, "rawValue");
    return switch (neutralType) {
      case BOOLEAN -> {
        Boolean value = normalizeBoolean(rawValue);
        yield new PrimaryKeyValue(neutralType, value, value.toString());
      }
      case INTEGER -> {
        BigInteger value = normalizeInteger(rawValue);
        yield new PrimaryKeyValue(neutralType, value, value.toString());
      }
      case FLOAT, DECIMAL -> {
        BigDecimal value = normalizeDecimal(rawValue);
        yield new PrimaryKeyValue(neutralType, value, decimalLiteral(value));
      }
      case STRING, ENUM_STRING -> {
        String value = normalizeString(rawValue);
        yield new PrimaryKeyValue(neutralType, value, value);
      }
      case BINARY -> {
        byte[] value = normalizeBinary(rawValue);
        yield new PrimaryKeyValue(neutralType, value, HexFormat.of().formatHex(value));
      }
      case DATE -> {
        LocalDate value = normalizeDate(rawValue);
        yield new PrimaryKeyValue(neutralType, value, value.toString());
      }
      case TIME -> {
        LocalTime value = normalizeTime(rawValue);
        yield new PrimaryKeyValue(neutralType, value, value.toString());
      }
      case TIMESTAMP -> {
        Object value = normalizeTimestamp(rawValue);
        yield new PrimaryKeyValue(neutralType, value, timestampLiteral(value));
      }
      case UUID -> {
        UUID value = normalizeUuid(rawValue);
        yield new PrimaryKeyValue(neutralType, value, value.toString());
      }
      case JSON, XML, UNSUPPORTED -> throw unsupportedType(neutralType);
    };
  }

  private static IllegalArgumentException unsupportedType(NeutralColumnType neutralType) {
    return new IllegalArgumentException(
        "primary key type is not supported for phase-1 ordering: " + neutralType);
  }

  private static Boolean normalizeBoolean(Object rawValue) {
    if (rawValue instanceof Boolean value) {
      return value;
    }
    if (rawValue instanceof String value) {
      if ("true".equalsIgnoreCase(value)) {
        return Boolean.TRUE;
      }
      if ("false".equalsIgnoreCase(value)) {
        return Boolean.FALSE;
      }
    }
    throw new IllegalArgumentException("invalid boolean primary key value: " + rawValue);
  }

  private static BigInteger normalizeInteger(Object rawValue) {
    if (rawValue instanceof BigInteger value) {
      return value;
    }
    if (rawValue instanceof Byte
        || rawValue instanceof Short
        || rawValue instanceof Integer
        || rawValue instanceof Long) {
      return BigInteger.valueOf(((Number) rawValue).longValue());
    }
    if (rawValue instanceof BigDecimal value) {
      try {
        return value.toBigIntegerExact();
      } catch (ArithmeticException e) {
        throw new IllegalArgumentException(
            "integer primary key value must be integral: " + rawValue, e);
      }
    }
    if (rawValue instanceof String value) {
      try {
        return new BigInteger(value);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("invalid integer primary key literal: " + value, e);
      }
    }
    throw new IllegalArgumentException(
        "unsupported integer primary key value: " + rawValue.getClass().getName());
  }

  private static BigDecimal normalizeDecimal(Object rawValue) {
    if (rawValue instanceof BigDecimal value) {
      return value.stripTrailingZeros();
    }
    if (rawValue instanceof BigInteger value) {
      return new BigDecimal(value).stripTrailingZeros();
    }
    if (rawValue instanceof Byte
        || rawValue instanceof Short
        || rawValue instanceof Integer
        || rawValue instanceof Long) {
      return BigDecimal.valueOf(((Number) rawValue).longValue()).stripTrailingZeros();
    }
    if (rawValue instanceof Float || rawValue instanceof Double) {
      return BigDecimal.valueOf(((Number) rawValue).doubleValue()).stripTrailingZeros();
    }
    if (rawValue instanceof String value) {
      try {
        return new BigDecimal(value).stripTrailingZeros();
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("invalid decimal primary key literal: " + value, e);
      }
    }
    throw new IllegalArgumentException(
        "unsupported decimal primary key value: " + rawValue.getClass().getName());
  }

  private static String normalizeString(Object rawValue) {
    if (rawValue instanceof String value) {
      return value;
    }
    return String.valueOf(rawValue);
  }

  private static byte[] normalizeBinary(Object rawValue) {
    if (rawValue instanceof byte[] value) {
      return value.clone();
    }
    if (rawValue instanceof ByteBuffer buffer) {
      ByteBuffer duplicate = buffer.duplicate();
      byte[] bytes = new byte[duplicate.remaining()];
      duplicate.get(bytes);
      return bytes;
    }
    if (rawValue instanceof String literal) {
      try {
        return HexFormat.of().parseHex(literal);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("invalid binary primary key literal: " + literal, e);
      }
    }
    throw new IllegalArgumentException(
        "unsupported binary primary key value: " + rawValue.getClass().getName());
  }

  private static LocalDate normalizeDate(Object rawValue) {
    if (rawValue instanceof LocalDate value) {
      return value;
    }
    if (rawValue instanceof java.sql.Date value) {
      return value.toLocalDate();
    }
    if (rawValue instanceof String value) {
      return LocalDate.parse(value);
    }
    throw new IllegalArgumentException(
        "unsupported date primary key value: " + rawValue.getClass().getName());
  }

  private static LocalTime normalizeTime(Object rawValue) {
    if (rawValue instanceof LocalTime value) {
      return value;
    }
    if (rawValue instanceof java.sql.Time value) {
      return value.toLocalTime();
    }
    if (rawValue instanceof OffsetTime value) {
      return value.toLocalTime();
    }
    if (rawValue instanceof String value) {
      return NeutralValueNormalizer.normalizeTimeValue(value);
    }
    throw new IllegalArgumentException(
        "unsupported time primary key value: " + rawValue.getClass().getName());
  }

  private static Object normalizeTimestamp(Object rawValue) {
    if (rawValue instanceof Instant value) {
      return value;
    }
    if (rawValue instanceof OffsetDateTime value) {
      return value.toInstant();
    }
    if (rawValue instanceof ZonedDateTime value) {
      return value.toInstant();
    }
    if (rawValue instanceof Date value) {
      return value.toInstant();
    }
    if (rawValue instanceof LocalDateTime value) {
      return value;
    }
    if (rawValue instanceof String value) {
      try {
        return Instant.parse(value);
      } catch (RuntimeException ignored) {
        try {
          return OffsetDateTime.parse(value).toInstant();
        } catch (RuntimeException alsoIgnored) {
          return LocalDateTime.parse(value);
        }
      }
    }
    throw new IllegalArgumentException(
        "unsupported timestamp primary key value: " + rawValue.getClass().getName());
  }

  private static UUID normalizeUuid(Object rawValue) {
    if (rawValue instanceof UUID value) {
      return value;
    }
    if (rawValue instanceof String value) {
      return UUID.fromString(value);
    }
    throw new IllegalArgumentException(
        "unsupported UUID primary key value: " + rawValue.getClass().getName());
  }

  private static String decimalLiteral(BigDecimal value) {
    BigDecimal normalized = value.stripTrailingZeros();
    if (normalized.compareTo(BigDecimal.ZERO) == 0) {
      return "0";
    }
    return normalized.toPlainString();
  }

  private static String timestampLiteral(Object value) {
    if (value instanceof Instant instant) {
      return instant.toString();
    }
    if (value instanceof LocalDateTime localDateTime) {
      return localDateTime.toString();
    }
    throw new IllegalArgumentException(
        "unsupported normalized timestamp primary key value: " + value);
  }

  private static int compareBinary(byte[] left, byte[] right) {
    int limit = Math.min(left.length, right.length);
    for (int index = 0; index < limit; index++) {
      int comparison =
          Integer.compare(Byte.toUnsignedInt(left[index]), Byte.toUnsignedInt(right[index]));
      if (comparison != 0) {
        return comparison;
      }
    }
    return Integer.compare(left.length, right.length);
  }

  private static int compareTimestamp(Object left, Object right) {
    if (left instanceof Instant leftInstant && right instanceof Instant rightInstant) {
      return leftInstant.compareTo(rightInstant);
    }
    if (left instanceof LocalDateTime leftLocal && right instanceof LocalDateTime rightLocal) {
      return leftLocal.compareTo(rightLocal);
    }
    throw new IllegalArgumentException(
        "timestamp primary key values used inconsistent representations: %s vs %s"
            .formatted(left.getClass().getName(), right.getClass().getName()));
  }

  private static int compareUuid(UUID left, UUID right) {
    int most = Long.compareUnsigned(left.getMostSignificantBits(), right.getMostSignificantBits());
    if (most != 0) {
      return most;
    }
    return Long.compareUnsigned(left.getLeastSignificantBits(), right.getLeastSignificantBits());
  }

  private static Object copyIfNeeded(Object value) {
    if (value instanceof byte[] bytes) {
      return bytes.clone();
    }
    return value;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof PrimaryKeyValue that)) {
      return false;
    }
    return neutralType == that.neutralType
        && literal.equals(that.literal)
        && valuesEqual(normalizedValue, that.normalizedValue);
  }

  @Override
  public int hashCode() {
    return 31 * neutralType.hashCode() + valueHash(normalizedValue);
  }

  private static boolean valuesEqual(Object left, Object right) {
    if (left instanceof byte[] leftBytes && right instanceof byte[] rightBytes) {
      return Arrays.equals(leftBytes, rightBytes);
    }
    return Objects.equals(left, right);
  }

  private static int valueHash(Object value) {
    if (value instanceof byte[] bytes) {
      return Arrays.hashCode(bytes);
    }
    return value.hashCode();
  }

  @Override
  public String toString() {
    return literal;
  }
}
