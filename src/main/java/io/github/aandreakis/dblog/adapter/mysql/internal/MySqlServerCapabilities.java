package io.github.aandreakis.dblog.adapter.mysql.internal;

import java.util.Locale;
import java.util.Objects;

/** Snapshot of the narrow MySQL server capabilities required by the live adapter. */
public record MySqlServerCapabilities(
    boolean binaryLoggingEnabled,
    String binlogFormat,
    String binlogRowImage,
    boolean gtidEnabled,
    int lowerCaseTableNames,
    String binlogRowMetadata) {
  public MySqlServerCapabilities {
    binlogFormat = normalize(binlogFormat, "binlogFormat");
    binlogRowImage = normalize(binlogRowImage, "binlogRowImage");
    if (lowerCaseTableNames < 0 || lowerCaseTableNames > 2) {
      throw new IllegalArgumentException("lowerCaseTableNames must be in the range 0..2");
    }
    binlogRowMetadata = normalize(binlogRowMetadata, "binlogRowMetadata");
  }

  public void requireStreamingPrerequisites() {
    if (!binaryLoggingEnabled) {
      throw new IllegalStateException(
          "MySQL binary logging must be enabled for the current MySQL adapter slice");
    }
    if (!"ROW".equals(binlogFormat)) {
      throw new IllegalStateException(
          "MySQL binlog_format must be ROW for the current MySQL adapter slice, but was "
              + binlogFormat);
    }
    if (!"FULL".equals(binlogRowImage)) {
      throw new IllegalStateException(
          "MySQL binlog_row_image must be FULL for the current MySQL adapter slice, but was "
              + binlogRowImage);
    }
    if (!"FULL".equals(binlogRowMetadata)) {
      throw new IllegalStateException(
          "MySQL binlog_row_metadata must be FULL for the current MySQL adapter slice"
              + " (needed so TABLE_MAP events carry column names and remain decodable across an"
              + " offline ALTER), but was "
              + binlogRowMetadata);
    }
  }

  private static String normalize(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value.trim().toUpperCase(Locale.ROOT);
  }
}
