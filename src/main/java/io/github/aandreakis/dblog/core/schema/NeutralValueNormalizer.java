package io.github.aandreakis.dblog.core.schema;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLXML;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.BitSet;
import java.util.HexFormat;
import java.util.Objects;
import java.util.UUID;

/**
 * Dialect-free normalization of supported row payload values. Callers that receive raw wire
 * forms requiring dialect-specific decoding (notably MySQL's binary JSON format) must decode
 * first via their adapter's {@link io.github.aandreakis.dblog.adapter.api.ValueDecoder} and then
 * pass the decoded form through this normalizer.
 */
public final class NeutralValueNormalizer {
  private NeutralValueNormalizer() {}

  public static Object normalize(ColumnDefinition column, Object value) {
    Objects.requireNonNull(column, "column");
    if (value == null) {
      return null;
    }
    return switch (column.neutralType()) {
      case BOOLEAN -> normalizeBooleanValue(value);
      case INTEGER -> normalizeIntegerValue(value);
      case FLOAT, DECIMAL -> normalizeDecimalValue(value);
      case STRING, ENUM_STRING -> normalizeStringLikeValue(value);
      case JSON -> normalizeJsonValue(value);
      case XML -> normalizeStringLikeValue(value);
      case BINARY -> normalizeBinaryValue(value);
      case DATE -> normalizeDateValue(value);
      case TIME -> normalizeTimeValue(value);
      case TIMESTAMP -> normalizeTimestampValue(value);
      case UUID -> normalizeUuidValue(value);
      case UNSUPPORTED -> value;
    };
  }

  public static Boolean normalizeBooleanValue(Object value) {
    if (value instanceof Boolean bool) {
      return bool;
    }
    if (value instanceof Number number) {
      return number.longValue() != 0L;
    }
    if (value instanceof BitSet bits) {
      // MySQL BIT(1) is decoded as java.util.BitSet by mysql-binlog-connector-java. Empty
      // bitset or bit 0 clear ⇒ false; bit 0 set ⇒ true. Without this branch the binlog
      // path for any captured table with a BIT(1) column crashes with the "Unsupported
      // boolean value representation: {}" error the leak-hunt reproduced live.
      return bits.get(0);
    }
    if (value instanceof byte[] bytes) {
      // Some source representations hand BIT(1) over as a single-byte array where the
      // low bit holds the value. Treat empty as false to match BitSet semantics above.
      return bytes.length > 0 && (bytes[0] & 1) != 0;
    }
    String normalized = String.valueOf(value).trim().toLowerCase(java.util.Locale.ROOT);
    return switch (normalized) {
      case "t", "true", "1", "yes", "on" -> true;
      case "f", "false", "0", "no", "off" -> false;
      default ->
          throw new IllegalStateException("Unsupported boolean value representation: " + value);
    };
  }

  public static Object normalizeIntegerValue(Object value) {
    BigInteger integer;
    if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
      integer = BigInteger.valueOf(((Number) value).longValue());
    } else if (value instanceof BigInteger bigInteger) {
      integer = bigInteger;
    } else if (value instanceof BigDecimal bigDecimal) {
      integer = bigDecimal.toBigIntegerExact();
    } else if (value instanceof Number number) {
      integer = BigDecimal.valueOf(number.doubleValue()).toBigIntegerExact();
    } else {
      integer = new BigInteger(String.valueOf(value));
    }
    if (integer.bitLength() <= 63) {
      return integer.longValueExact();
    }
    return integer;
  }

  public static BigDecimal normalizeDecimalValue(Object value) {
    if (value instanceof BigDecimal bigDecimal) {
      return bigDecimal;
    }
    if (value instanceof BigInteger bigInteger) {
      return new BigDecimal(bigInteger);
    }
    if (value instanceof Number number) {
      return BigDecimal.valueOf(number.doubleValue());
    }
    return new BigDecimal(String.valueOf(value));
  }

  public static String normalizeStringLikeValue(Object value) {
    if (value instanceof SQLXML sqlxml) {
      try {
        return sqlxml.getString();
      } catch (Exception ex) {
        throw new IllegalStateException("Failed to read SQLXML value", ex);
      }
    }
    if (value instanceof byte[] bytes) {
      return new String(bytes, StandardCharsets.UTF_8);
    }
    if (value instanceof ByteBuffer byteBuffer) {
      ByteBuffer duplicate = byteBuffer.duplicate();
      byte[] bytes = new byte[duplicate.remaining()];
      duplicate.get(bytes);
      return new String(bytes, StandardCharsets.UTF_8);
    }
    return String.valueOf(value);
  }

  public static String normalizeJsonValue(Object value) {
    // byte[] / ByteBuffer are treated as UTF-8 JSON text — the default dialect-free shape JDBC
    // drivers and sink coercion paths deliver. Adapters whose wire format is NOT UTF-8 JSON
    // (notably MySQL's binary binlog JSON) must decode via SourceDialect.valueDecoder() BEFORE
    // calling this method; otherwise the raw bytes will be misinterpreted as UTF-8.
    return normalizeStringLikeValue(value);
  }

  public static byte[] normalizeBinaryValue(Object value) {
    if (value instanceof byte[] bytes) {
      return bytes.clone();
    }
    if (value instanceof ByteBuffer byteBuffer) {
      ByteBuffer duplicate = byteBuffer.duplicate();
      byte[] bytes = new byte[duplicate.remaining()];
      duplicate.get(bytes);
      return bytes;
    }
    if (value instanceof BitSet bits) {
      // MySQL BIT(n>1) is mapped to neutral BINARY by MySqlDialect, and the binlog
      // decoder emits the column as java.util.BitSet. BitSet.toByteArray() gives a
      // little-endian byte[] representation that faithfully preserves bit-index
      // semantics; downstream sinks treat it as opaque binary, which matches how bit
      // strings flow through target databases.
      return bits.toByteArray();
    }
    if (value instanceof String stringValue) {
      if (stringValue.startsWith("\\x") || stringValue.startsWith("\\X")) {
        return HexFormat.of().parseHex(stringValue.substring(2));
      }
      return stringValue.getBytes(StandardCharsets.ISO_8859_1);
    }
    throw new IllegalStateException(
        "Unsupported binary value representation: " + value.getClass().getName());
  }

  public static LocalDate normalizeDateValue(Object value) {
    if (value instanceof LocalDate localDate) {
      return localDate;
    }
    if (value instanceof java.sql.Date sqlDate) {
      return sqlDate.toLocalDate();
    }
    if (value instanceof java.util.Date date) {
      return date.toInstant().atOffset(ZoneOffset.UTC).toLocalDate();
    }
    return LocalDate.parse(String.valueOf(value));
  }

  public static LocalTime normalizeTimeValue(Object value) {
    if (value instanceof LocalTime localTime) {
      return localTime;
    }
    if (value instanceof java.sql.Time sqlTime) {
      return sqlTime.toLocalTime();
    }
    if (value instanceof java.util.Date date) {
      return date.toInstant().atOffset(ZoneOffset.UTC).toLocalTime();
    }
    if (value instanceof OffsetDateTime offsetDateTime) {
      return offsetDateTime.toLocalTime();
    }
    if (value instanceof OffsetTime offsetTime) {
      return offsetTime.toLocalTime();
    }
    return parseLocalTime(String.valueOf(value));
  }

  public static Object normalizeTimestampValue(Object value) {
    if (value instanceof Instant instant) {
      return instant;
    }
    if (value instanceof OffsetDateTime offsetDateTime) {
      return offsetDateTime.toInstant();
    }
    if (value instanceof Timestamp timestamp) {
      return timestamp.toInstant();
    }
    if (value instanceof java.util.Date date) {
      return date.toInstant();
    }
    if (value instanceof LocalDateTime localDateTime) {
      return localDateTime;
    }
    String text = String.valueOf(value).trim();
    if (text.isEmpty()) {
      return text;
    }
    try {
      return Instant.parse(text);
    } catch (DateTimeParseException ignored) {
    }
    try {
      return OffsetDateTime.parse(normalizeIsoWhitespace(text)).toInstant();
    } catch (DateTimeParseException ignored) {
    }
    try {
      return LocalDateTime.parse(normalizeIsoWhitespace(text));
    } catch (DateTimeParseException ignored) {
    }
    return text;
  }

  public static UUID normalizeUuidValue(Object value) {
    if (value instanceof UUID uuid) {
      return uuid;
    }
    return UUID.fromString(String.valueOf(value));
  }

  private static LocalTime parseLocalTime(String value) {
    String trimmed = value.trim();
    try {
      return LocalTime.parse(trimmed);
    } catch (DateTimeParseException localTimeFailure) {
      try {
        return OffsetTime.parse(normalizeOffsetSuffix(trimmed)).toLocalTime();
      } catch (DateTimeParseException ignored) {
      }
      String normalized = normalizeIsoWhitespace(trimmed);
      int tIndex = normalized.indexOf('T');
      if (tIndex >= 0 && tIndex + 1 < normalized.length()) {
        String timePart = normalized.substring(tIndex + 1);
        try {
          return LocalTime.parse(timePart);
        } catch (DateTimeParseException ignored) {
          return OffsetTime.parse(normalizeOffsetSuffix(timePart)).toLocalTime();
        }
      }
      throw localTimeFailure;
    }
  }

  private static String normalizeOffsetSuffix(String value) {
    if (value.matches(".*[+-]\\d{2}$")) {
      return value + ":00";
    }
    if (value.matches(".*[+-]\\d{4}$")) {
      int offsetStart = value.length() - 5;
      return value.substring(0, offsetStart + 3) + ":" + value.substring(offsetStart + 3);
    }
    return value;
  }

  private static String normalizeIsoWhitespace(String text) {
    if (text.indexOf(' ') < 0 || text.indexOf('T') >= 0) {
      return text;
    }
    return text.replace(' ', 'T');
  }
}
