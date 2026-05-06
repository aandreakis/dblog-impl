package io.github.aandreakis.dblog.adapter.postgres;

import io.github.aandreakis.dblog.core.model.SourcePosition;
import java.util.Locale;
import java.util.Objects;

/**
 * Typed PostgreSQL log-sequence number for the adapter.
 */
public record PostgresLsn(long log, long offset)
    implements SourcePosition, Comparable<PostgresLsn> {
  private static final long MAX_UNSIGNED_INT = 0xFFFF_FFFFL;

  public PostgresLsn {
    requireUnsignedInt(log, "log");
    requireUnsignedInt(offset, "offset");
  }

  public static PostgresLsn parse(String value) {
    Objects.requireNonNull(value, "value");
    if (value.isBlank()) {
      throw new IllegalArgumentException("PostgreSQL LSN must not be blank");
    }

    int separator = value.indexOf('/');
    if (separator <= 0
        || separator == value.length() - 1
        || value.indexOf('/', separator + 1) >= 0) {
      throw new IllegalArgumentException(
          "PostgreSQL LSN must use the form HEX/HEX, but was: " + value);
    }

    long parsedLog = parseHalf(value.substring(0, separator), "log", value);
    long parsedOffset = parseHalf(value.substring(separator + 1), "offset", value);
    return new PostgresLsn(parsedLog, parsedOffset);
  }

  public static PostgresLsn fromLong(long value) {
    return new PostgresLsn((value >>> 32) & MAX_UNSIGNED_INT, value & MAX_UNSIGNED_INT);
  }

  /**
   * Returns the 64-bit unsigned LSN packed into a Java {@code long}. The value is treated as
   * unsigned: for LSNs at or above {@code 0x8000_0000_0000_0000L} the returned long is negative
   * when interpreted in two's complement. Do <b>not</b> order two LSNs with Java's signed
   * {@code <} / {@code >} on the raw long; use {@link #compareTo(PostgresLsn)} (which is already
   * unsigned-safe) or {@link Long#compareUnsigned(long, long)} if you must compare at the long
   * level. Round-trips correctly via {@link #fromLong(long)} and {@link
   * org.postgresql.replication.LogSequenceNumber#valueOf(long)}, both of which treat the input as
   * unsigned.
   */
  public long asLong() {
    return (log << 32) | offset;
  }

  @Override
  public String displayValue() {
    return Long.toHexString(log).toUpperCase(Locale.ROOT)
        + "/"
        + Long.toHexString(offset).toUpperCase(Locale.ROOT);
  }

  @Override
  public int compareTo(PostgresLsn other) {
    Objects.requireNonNull(other, "other");
    int logComparison = Long.compareUnsigned(log, other.log);
    if (logComparison != 0) {
      return logComparison;
    }
    return Long.compareUnsigned(offset, other.offset);
  }

  @Override
  public String toString() {
    return displayValue();
  }

  private static long parseHalf(String half, String label, String originalValue) {
    if (half.isBlank() || half.length() > 8) {
      throw new IllegalArgumentException(
          "PostgreSQL LSN " + label + " half must contain 1 to 8 hex digits: " + originalValue);
    }
    for (int index = 0; index < half.length(); index++) {
      char candidate = half.charAt(index);
      if (Character.digit(candidate, 16) < 0) {
        throw new IllegalArgumentException(
            "PostgreSQL LSN contains a non-hex character in the "
                + label
                + " half: "
                + originalValue);
      }
    }
    return Long.parseUnsignedLong(half, 16);
  }

  private static void requireUnsignedInt(long value, String label) {
    if (value < 0 || value > MAX_UNSIGNED_INT) {
      throw new IllegalArgumentException(
          "PostgreSQL LSN " + label + " half must fit into an unsigned 32-bit value");
    }
  }
}
