package io.github.aandreakis.dblog.sink.jdbc;

import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.NeutralValueNormalizer;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.UUID;

/** Target-side value coercions for sink datatype migrations. */
final class TargetValueCoercions {
  private TargetValueCoercions() {}

  static boolean isCompatible(NeutralColumnType sourceType, NeutralColumnType targetType) {
    Objects.requireNonNull(sourceType, "sourceType");
    Objects.requireNonNull(targetType, "targetType");
    if (sourceType == targetType) {
      return true;
    }
    return switch (targetType) {
      case STRING ->
          sourceType != NeutralColumnType.BINARY && sourceType != NeutralColumnType.UNSUPPORTED;
      case BOOLEAN ->
          sourceType == NeutralColumnType.BOOLEAN
              || sourceType == NeutralColumnType.INTEGER
              || sourceType == NeutralColumnType.STRING;
      case INTEGER ->
          sourceType == NeutralColumnType.INTEGER
              || sourceType == NeutralColumnType.BOOLEAN
              || sourceType == NeutralColumnType.STRING;
      case FLOAT ->
          sourceType == NeutralColumnType.INTEGER
              || sourceType == NeutralColumnType.FLOAT
              || sourceType == NeutralColumnType.DECIMAL
              || sourceType == NeutralColumnType.STRING;
      case DECIMAL ->
          sourceType == NeutralColumnType.INTEGER
              || sourceType == NeutralColumnType.FLOAT
              || sourceType == NeutralColumnType.DECIMAL
              || sourceType == NeutralColumnType.STRING;
      case DATE ->
          sourceType == NeutralColumnType.DATE
              || sourceType == NeutralColumnType.TIMESTAMP
              || sourceType == NeutralColumnType.STRING;
      case TIME ->
          sourceType == NeutralColumnType.TIME
              || sourceType == NeutralColumnType.TIMESTAMP
              || sourceType == NeutralColumnType.STRING;
      case TIMESTAMP ->
          sourceType == NeutralColumnType.TIMESTAMP
              || sourceType == NeutralColumnType.DATE
              || sourceType == NeutralColumnType.STRING;
      case UUID -> sourceType == NeutralColumnType.UUID || sourceType == NeutralColumnType.STRING;
      case JSON -> sourceType == NeutralColumnType.JSON || sourceType == NeutralColumnType.STRING;
      case XML -> sourceType == NeutralColumnType.XML || sourceType == NeutralColumnType.STRING;
      case ENUM_STRING ->
          sourceType == NeutralColumnType.ENUM_STRING || sourceType == NeutralColumnType.STRING;
      case BINARY -> sourceType == NeutralColumnType.BINARY;
      case UNSUPPORTED -> false;
    };
  }

  static Object coerce(
      Object value, JdbcApplyTargetSchemaInspector.TargetColumnMetadata targetColumn) {
    Objects.requireNonNull(targetColumn, "targetColumn");
    if (value == null) {
      return null;
    }
    if (isAlreadyNormalizedForTarget(value, targetColumn.neutralType())) {
      return value;
    }
    Object preCoerced = preCoerce(value, targetColumn.neutralType());
    if (targetColumn.neutralType() == NeutralColumnType.STRING) {
      return stringify(preCoerced);
    }
    return NeutralValueNormalizer.normalize(targetColumn.coercionDefinition(), preCoerced);
  }

  private static Object preCoerce(Object value, NeutralColumnType targetType) {
    return switch (targetType) {
      case INTEGER -> value instanceof Boolean bool ? (bool ? 1L : 0L) : value;
      case FLOAT, DECIMAL ->
          value instanceof Boolean bool ? (bool ? BigDecimal.ONE : BigDecimal.ZERO) : value;
      case DATE -> coerceDateInput(value);
      case TIME -> coerceTimeInput(value);
      case TIMESTAMP -> coerceTimestampInput(value);
      default -> value;
    };
  }

  private static Object coerceDateInput(Object value) {
    if (value instanceof Instant instant) {
      return instant.atOffset(ZoneOffset.UTC).toLocalDate();
    }
    if (value instanceof OffsetDateTime offsetDateTime) {
      return offsetDateTime.toLocalDate();
    }
    if (value instanceof LocalDateTime localDateTime) {
      return localDateTime.toLocalDate();
    }
    return value;
  }

  private static Object coerceTimeInput(Object value) {
    if (value instanceof Instant instant) {
      return instant.atOffset(ZoneOffset.UTC).toLocalTime();
    }
    if (value instanceof OffsetDateTime offsetDateTime) {
      return offsetDateTime.toLocalTime();
    }
    if (value instanceof LocalDateTime localDateTime) {
      return localDateTime.toLocalTime();
    }
    return value;
  }

  private static Object coerceTimestampInput(Object value) {
    if (value instanceof LocalDate localDate) {
      return localDate.atStartOfDay();
    }
    return value;
  }

  private static String stringify(Object value) {
    if (value instanceof String stringValue) {
      return stringValue;
    }
    if (value instanceof Instant instant) {
      return instant.toString();
    }
    if (value instanceof OffsetDateTime offsetDateTime) {
      return offsetDateTime.toInstant().toString();
    }
    if (value instanceof LocalDate localDate) {
      return localDate.toString();
    }
    if (value instanceof LocalTime localTime) {
      return localTime.toString();
    }
    if (value instanceof LocalDateTime localDateTime) {
      return localDateTime.toString();
    }
    if (value instanceof UUID uuid) {
      return uuid.toString();
    }
    return String.valueOf(value);
  }

  private static boolean isAlreadyNormalizedForTarget(Object value, NeutralColumnType targetType) {
    return switch (targetType) {
      case BOOLEAN -> value instanceof Boolean;
      case INTEGER -> value instanceof Long || value instanceof java.math.BigInteger;
      case FLOAT, DECIMAL -> value instanceof BigDecimal;
      case STRING, JSON, XML, ENUM_STRING -> value instanceof String;
      case BINARY -> value instanceof byte[];
      case DATE -> value instanceof LocalDate;
      case TIME -> value instanceof LocalTime;
      case TIMESTAMP -> value instanceof Instant || value instanceof LocalDateTime;
      case UUID -> value instanceof UUID;
      case UNSUPPORTED -> false;
    };
  }
}
