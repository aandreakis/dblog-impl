package io.github.aandreakis.dblog.adapter.mysql.internal;

import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import io.github.aandreakis.dblog.core.reconcile.HeartbeatMetadata;
import io.github.aandreakis.dblog.core.reconcile.WatermarkMetadata;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.NeutralValueNormalizer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Shared MySQL singleton metadata-row validation and projection helpers. */
final class MySqlMetadataRows {
  private MySqlMetadataRows() {}

  static ImmutableRowImage watermarkRow(
      Object[] rowValues, String adapterLabel, boolean requireNonBlankToken) {
    validateMetadataRowWidth(rowValues, adapterLabel, "watermark");
    requireSingletonMetadataPrimaryKey(rowValues[0], adapterLabel, "watermark");
    String normalizedRunId = normalizeRunId(rowValues[1]);
    Object tokenValue = rowValues[2];
    String normalizedToken = tokenValue == null ? null : normalizeTokenValue(tokenValue);
    if (requireNonBlankToken && (normalizedToken == null || normalizedToken.isBlank())) {
      throw new IllegalStateException(
          adapterLabel + " watermark metadata row is missing a non-blank token value");
    }
    Map<String, Object> row = new LinkedHashMap<>();
    row.put(WatermarkMetadata.RUN_ID_COLUMN, normalizedRunId);
    row.put(WatermarkMetadata.TOKEN_COLUMN, normalizedToken);
    return ImmutableRowImage.of(row);
  }

  static ImmutableRowImage heartbeatRow(
      Object[] rowValues, String adapterLabel, boolean requireNonBlankTimestamp) {
    validateHeartbeatMetadataRowWidth(rowValues, adapterLabel);
    requireSingletonMetadataPrimaryKey(rowValues[0], adapterLabel, "heartbeat");
    String normalizedRunId = normalizeRunId(rowValues[1]);
    String normalizedSourceStreamId = rowValues.length >= 4 ? normalizeRunId(rowValues[2]) : null;
    Object timestampValue = rowValues.length >= 4 ? rowValues[3] : rowValues[2];
    if (requireNonBlankTimestamp
        && (timestampValue == null || String.valueOf(timestampValue).isBlank())) {
      throw new IllegalStateException(
          adapterLabel + " heartbeat metadata row is missing a non-blank heartbeat timestamp");
    }
    Object normalizedTimestamp =
        timestampValue == null
            ? null
            : NeutralValueNormalizer.normalize(HeartbeatMetadataColumns.LAST_BEAT_AT, timestampValue);
    if (normalizedTimestamp instanceof String) {
      throw new IllegalStateException(
          adapterLabel + " heartbeat metadata row does not contain a parseable heartbeat timestamp");
    }
    Map<String, Object> row = new LinkedHashMap<>();
    row.put(HeartbeatMetadata.RUN_ID_COLUMN, normalizedRunId);
    row.put(HeartbeatMetadata.SOURCE_STREAM_ID_COLUMN, normalizedSourceStreamId);
    row.put(HeartbeatMetadata.TIMESTAMP_COLUMN, normalizedTimestamp);
    return ImmutableRowImage.of(row);
  }

  static ImmutableRowImage metadataPrimaryKey() {
    return WatermarkMetadata.singletonPrimaryKey();
  }

  private static void validateMetadataRowWidth(Object[] rowValues, String adapterLabel, String label) {
    Objects.requireNonNull(rowValues, "rowValues");
    if (rowValues.length != 3) {
      throw new IllegalStateException(
          adapterLabel + " " + label + " metadata row does not contain the expected singleton columns");
    }
  }

  private static void validateHeartbeatMetadataRowWidth(Object[] rowValues, String adapterLabel) {
    Objects.requireNonNull(rowValues, "rowValues");
    if (rowValues.length != 3 && rowValues.length != 4) {
      throw new IllegalStateException(
          adapterLabel + " heartbeat metadata row does not contain the expected singleton columns");
    }
  }

  private static String normalizeRunId(Object runIdValue) {
    if (runIdValue == null) {
      return null;
    }
    String normalized = normalizeTokenValue(runIdValue);
    return normalized.isBlank() ? null : normalized;
  }

  private static void requireSingletonMetadataPrimaryKey(
      Object value, String adapterLabel, String label) {
    if (value == null) {
      throw new IllegalStateException(
          adapterLabel + " " + label + " metadata row is missing the singleton primary key value");
    }
    if (value instanceof Number number) {
      if (number.longValue() != WatermarkMetadata.SINGLETON_ROW_ID
          || number.doubleValue() != WatermarkMetadata.SINGLETON_ROW_ID) {
        throw new IllegalStateException(
            adapterLabel + " " + label + " metadata row targeted a non-singleton primary key: " + value);
      }
      return;
    }
    if (!Long.toString(WatermarkMetadata.SINGLETON_ROW_ID).equals(String.valueOf(value))) {
      throw new IllegalStateException(
          adapterLabel + " " + label + " metadata row targeted a non-singleton primary key: " + value);
    }
  }

  private static String normalizeTokenValue(Object tokenValue) {
    if (tokenValue instanceof byte[] bytes) {
      return new String(bytes, StandardCharsets.UTF_8);
    }
    if (tokenValue instanceof ByteBuffer byteBuffer) {
      ByteBuffer duplicate = byteBuffer.duplicate();
      byte[] bytes = new byte[duplicate.remaining()];
      duplicate.get(bytes);
      return new String(bytes, StandardCharsets.UTF_8);
    }
    return String.valueOf(tokenValue);
  }

  private static final class HeartbeatMetadataColumns {
    private static final ColumnDefinition LAST_BEAT_AT =
        new ColumnDefinition(
            HeartbeatMetadata.TIMESTAMP_COLUMN,
            "timestamp",
            NeutralColumnType.TIMESTAMP,
            false,
            false);
  }
}
