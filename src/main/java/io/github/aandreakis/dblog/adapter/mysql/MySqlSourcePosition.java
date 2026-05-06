package io.github.aandreakis.dblog.adapter.mysql;

import io.github.aandreakis.dblog.core.model.SourcePosition;
import java.util.Objects;

/**
 * Typed durable MySQL binlog position for the MySQL adapter.
 *
 * <p>The position contract records a binlog file and byte position, with an optional GTID set.
 */
public record MySqlSourcePosition(String binlogFilename, long binlogPosition, String gtidSet)
    implements SourcePosition, Comparable<MySqlSourcePosition> {
  private static final String GTID_PREFIX = ";gtid=";

  public MySqlSourcePosition {
    binlogFilename = requireNonBlank(binlogFilename, "binlogFilename");
    if (binlogPosition < 0) {
      throw new IllegalArgumentException("binlogPosition must be >= 0");
    }
    gtidSet = normalizeOptional(gtidSet);
  }

  public static MySqlSourcePosition parse(String value) {
    Objects.requireNonNull(value, "value");
    if (value.isBlank()) {
      throw new IllegalArgumentException("MySQL source position must not be blank");
    }
    String fileAndPosition = value;
    String gtidSet = null;
    int gtidIndex = value.indexOf(GTID_PREFIX);
    if (gtidIndex >= 0) {
      fileAndPosition = value.substring(0, gtidIndex);
      gtidSet = value.substring(gtidIndex + GTID_PREFIX.length());
      if (gtidSet.isBlank()) {
        throw new IllegalArgumentException(
            "MySQL source position gtidSet must not be blank when present");
      }
    }
    int separator = fileAndPosition.lastIndexOf(':');
    if (separator <= 0 || separator == fileAndPosition.length() - 1) {
      throw new IllegalArgumentException(
          "MySQL source position must use the form <binlogFilename>:<binlogPosition>[;gtid=<set>], but was: "
              + value);
    }
    String filename = fileAndPosition.substring(0, separator);
    long position;
    try {
      position = Long.parseLong(fileAndPosition.substring(separator + 1));
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException(
          "MySQL source position binlogPosition must be a base-10 integer: " + value, ex);
    }
    return new MySqlSourcePosition(filename, position, gtidSet);
  }

  @Override
  public String displayValue() {
    String value = binlogFilename + ":" + binlogPosition;
    return gtidSet == null ? value : value + GTID_PREFIX + gtidSet;
  }

  /**
   * Compares two MySQL binlog positions by numeric sequence, not lexicographic string.
   *
   * <p>MySQL binlog filenames are {@code <prefix>.NNNNNN} where NNNNNN is a zero-padded numeric
   * sequence. Lexicographic comparison sorts {@code mysql-bin.1000000} before
   * {@code mysql-bin.999999}, which is wrong on digit-width transitions. We parse the suffix as
   * a {@code long} and compare numerically. Positions with different prefixes are not
   * comparable — ordering across a server-level binlog rename is meaningless — so we throw.
   */
  @Override
  public int compareTo(MySqlSourcePosition other) {
    Objects.requireNonNull(other, "other");
    ParsedBinlogFilename thisParsed = ParsedBinlogFilename.parse(binlogFilename);
    ParsedBinlogFilename otherParsed = ParsedBinlogFilename.parse(other.binlogFilename);
    if (!thisParsed.prefix().equals(otherParsed.prefix())) {
      throw new IllegalStateException(
          "MySQL binlog positions have different filename prefixes and are not comparable: "
              + binlogFilename
              + " vs "
              + other.binlogFilename);
    }
    int sequenceComparison = Long.compareUnsigned(thisParsed.sequence(), otherParsed.sequence());
    if (sequenceComparison != 0) {
      return sequenceComparison;
    }
    return Long.compareUnsigned(binlogPosition, other.binlogPosition);
  }

  private record ParsedBinlogFilename(String prefix, long sequence) {
    static ParsedBinlogFilename parse(String filename) {
      Objects.requireNonNull(filename, "filename");
      int dot = filename.lastIndexOf('.');
      if (dot <= 0 || dot == filename.length() - 1) {
        throw new IllegalStateException(
            "MySQL binlog filename must have the form <prefix>.<numeric-sequence> but was: "
                + filename);
      }
      String suffix = filename.substring(dot + 1);
      long sequence;
      try {
        sequence = Long.parseUnsignedLong(suffix);
      } catch (NumberFormatException ex) {
        throw new IllegalStateException(
            "MySQL binlog filename suffix must be a non-negative integer but was: " + filename,
            ex);
      }
      return new ParsedBinlogFilename(filename.substring(0, dot), sequence);
    }
  }

  @Override
  public String toString() {
    return displayValue();
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static String normalizeOptional(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value;
  }
}
