package io.github.aandreakis.dblog.adapter.mysql.internal;

import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import io.github.aandreakis.dblog.core.model.RowLayout;
import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.NeutralValueNormalizer;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyValue;
import io.github.aandreakis.dblog.core.schema.SchemaDriftException;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TimeZone;

public final class JdbcMySqlChunkReader implements SourceChunkReader {
  @Override
  public Optional<PrimaryKeyTuple> tableScanUpperBoundPrimaryKeyTuple(
      Connection connection, TableSchema schema) throws SQLException {
    Objects.requireNonNull(connection, "connection");
    Objects.requireNonNull(schema, "schema");

    try (PreparedStatement statement =
            connection.prepareStatement(MySqlSql.tableScanUpperBoundPrimaryKeySql(schema));
        ResultSet resultSet = statement.executeQuery()) {
      if (!resultSet.next()) {
        return Optional.empty();
      }
      return Optional.of(readPrimaryKeyTuple(resultSet, schema));
    }
  }

  @Override
  public Optional<Chunk> nextTableChunk(
      Connection connection,
      String jobId,
      TableSchema schema,
      PrimaryKeyTuple startAfterPrimaryKey,
      PrimaryKeyTuple stopAtPrimaryKey,
      int chunkSize)
      throws SQLException {
    Objects.requireNonNull(connection, "connection");
    Objects.requireNonNull(schema, "schema");
    requireNonBlank(jobId, "jobId");
    int queryLimit = queryLimit(chunkSize);
    PrimaryKeyTuple normalizedStartAfter =
        startAfterPrimaryKey == null
            ? null
            : schema.primaryKeyTupleFor(schema.primaryKeyRowFromTuple(startAfterPrimaryKey));
    PrimaryKeyTuple normalizedStopAt =
        stopAtPrimaryKey == null
            ? null
            : schema.primaryKeyTupleFor(schema.primaryKeyRowFromTuple(stopAtPrimaryKey));
    if (normalizedStartAfter != null
        && normalizedStopAt != null
        && (normalizedStartAfter.equals(normalizedStopAt)
            || (schema.canComparePrimaryKeyOrderInMemory()
                && schema.comparePrimaryKeyTuples(normalizedStartAfter, normalizedStopAt) > 0))) {
      return Optional.empty();
    }

    try (PreparedStatement statement =
        connection.prepareStatement(
            MySqlSql.tableChunkReadSql(schema, normalizedStartAfter != null, normalizedStopAt != null))) {
      int parameterIndex = 1;
      if (normalizedStartAfter != null) {
        parameterIndex = bindPrimaryKeyTuple(statement, parameterIndex, schema, normalizedStartAfter);
      }
      if (normalizedStopAt != null) {
        parameterIndex = bindPrimaryKeyTuple(statement, parameterIndex, schema, normalizedStopAt);
      }
      statement.setInt(parameterIndex, queryLimit);
      try (ResultSet resultSet = statement.executeQuery()) {
        List<ImmutableRowImage> rowImages = readRowImages(resultSet, schema);
        if (rowImages.isEmpty()) {
          return Optional.empty();
        }
        boolean finalChunk = rowImages.size() <= chunkSize;
        List<ImmutableRowImage> selectedRows =
            finalChunk ? rowImages : new ArrayList<>(rowImages.subList(0, chunkSize));
        PrimaryKeyTuple lastPrimaryKey =
            schema.primaryKeyTupleFor(selectedRows.get(selectedRows.size() - 1));
        return Optional.of(
            new Chunk(
                jobId,
                schema.tableId().displayName(),
                schema,
                normalizedStartAfter,
                selectedRows,
                null,
                lastPrimaryKey,
                finalChunk));
      }
    }
  }

  @Override
  public Optional<Chunk> targetedPrimaryKeyTuples(
      Connection connection,
      String jobId,
      TableSchema schema,
      List<PrimaryKeyTuple> requestedPrimaryKeys)
      throws SQLException {
    Objects.requireNonNull(connection, "connection");
    Objects.requireNonNull(schema, "schema");
    Objects.requireNonNull(requestedPrimaryKeys, "requestedPrimaryKeys");
    requireNonBlank(jobId, "jobId");

    LinkedHashSet<PrimaryKeyTuple> dedupedKeys = new LinkedHashSet<>();
    for (PrimaryKeyTuple requestedPrimaryKey : requestedPrimaryKeys) {
      dedupedKeys.add(schema.primaryKeyTupleFor(schema.primaryKeyRowFromTuple(requestedPrimaryKey)));
    }
    if (dedupedKeys.isEmpty()) {
      return Optional.empty();
    }

    List<PrimaryKeyTuple> orderedKeys = List.copyOf(dedupedKeys);
    List<ImmutableRowImage> rowImages = readTargetedRows(connection, schema, orderedKeys);
    if (rowImages.isEmpty()) {
      return Optional.empty();
    }
    Map<PrimaryKeyTuple, ImmutableRowImage> rowsByPrimaryKey = new LinkedHashMap<>();
    for (ImmutableRowImage row : rowImages) {
      PrimaryKeyTuple primaryKeyTuple = primaryKeyTuple(schema, row, "targeted chunk read");
      ImmutableRowImage previous = rowsByPrimaryKey.put(primaryKeyTuple, row);
      if (previous != null) {
        throw new IllegalStateException(
            "MySQL targeted chunk read returned duplicate primary key "
                + primaryKeyTuple.literal()
                + " for "
                + schema.tableId().displayName());
      }
    }

    Map<PrimaryKeyTuple, ImmutableRowImage> selectedByActualPrimaryKey = new LinkedHashMap<>();
    List<PrimaryKeyTuple> matchedRequestedKeys = new ArrayList<>();
    PrimaryKeyTuple lastPrimaryKey = null;
    for (PrimaryKeyTuple requestedKey : orderedKeys) {
      ImmutableRowImage row = rowsByPrimaryKey.get(requestedKey);
      if (row == null && !schema.canComparePrimaryKeyOrderInMemory()) {
        List<ImmutableRowImage> sourceMatchedRows =
            readTargetedRows(connection, schema, List.of(requestedKey));
        if (sourceMatchedRows.size() > 1) {
          throw new IllegalStateException(
              "MySQL targeted chunk read returned multiple rows for source-equivalent primary key "
                  + requestedKey.literal()
                  + " for "
                  + schema.tableId().displayName());
        }
        row = sourceMatchedRows.isEmpty() ? null : sourceMatchedRows.getFirst();
      }
      if (row != null) {
        PrimaryKeyTuple actualPrimaryKey = primaryKeyTuple(schema, row, "targeted chunk read");
        if (selectedByActualPrimaryKey.putIfAbsent(actualPrimaryKey, row) == null) {
          lastPrimaryKey = actualPrimaryKey;
        }
        matchedRequestedKeys.add(requestedKey);
      }
    }
    if (selectedByActualPrimaryKey.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        new Chunk(
            jobId,
            schema.tableId().displayName(),
            schema,
            null,
            List.copyOf(selectedByActualPrimaryKey.values()),
            null,
            lastPrimaryKey,
            true,
            matchedRequestedKeys));
  }

  private List<ImmutableRowImage> readTargetedRows(
      Connection connection, TableSchema schema, List<PrimaryKeyTuple> primaryKeys)
      throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(MySqlSql.targetedPrimaryKeysReadSql(schema, primaryKeys.size()))) {
      int parameterIndex = 1;
      for (PrimaryKeyTuple primaryKey : primaryKeys) {
        parameterIndex = bindPrimaryKeyTuple(statement, parameterIndex, schema, primaryKey);
      }
      try (ResultSet resultSet = statement.executeQuery()) {
        return readRowImages(resultSet, schema);
      }
    }
  }

  private List<ImmutableRowImage> readRowImages(ResultSet resultSet, TableSchema schema)
      throws SQLException {
    List<ImmutableRowImage> rowImages = new ArrayList<>();
    List<ColumnDefinition> selectedColumns = schema.selectedColumns();
    List<ColumnDefinition> primaryKeyDefinitions = schema.primaryKeyDefinitions();
    RowLayout layout = schema.selectedRowLayout();
    while (resultSet.next()) {
      Object[] values = new Object[selectedColumns.size()];
      for (int index = 0; index < selectedColumns.size(); index++) {
        ColumnDefinition column = selectedColumns.get(index);
        Object raw = readColumnValue(resultSet, index + 1, column);
        // Route chunk-origin values through the same normalizer the binlog path uses, so the
        // reconcile step and downstream sinks see one consistent shape regardless of capture
        // origin (SELECT vs LOG). A normalization failure indicates the source value cannot be
        // coerced into the column's declared neutral type — that is schema drift, not a bug.
        Object normalized;
        try {
          normalized = NeutralValueNormalizer.normalize(column, raw);
        } catch (RuntimeException failure) {
          String columnKind = column.primaryKey() ? "primary key column " : "column ";
          throw new SchemaDriftException(
              "MySQL JDBC chunk read detected value that does not fit declared neutral type for "
                  + columnKind
                  + schema.tableId().displayName()
                  + "."
                  + column.name()
                  + ": "
                  + failure.getMessage());
        }
        values[index] = normalized;
      }
      ImmutableRowImage image = ImmutableRowImage.ofLayout(layout, values);
      verifyPrimaryKeyPresent(schema, image, primaryKeyDefinitions, "chunk read");
      rowImages.add(image);
    }
    return List.copyOf(rowImages);
  }

  private static void verifyPrimaryKeyPresent(
      TableSchema schema,
      ImmutableRowImage image,
      List<ColumnDefinition> primaryKeyDefinitions,
      String operation) {
    for (ColumnDefinition definition : primaryKeyDefinitions) {
      if (image.get(definition.name()) == null) {
        throw new SchemaDriftException(
            "MySQL JDBC " + operation + " detected primary key drift for "
                + schema.tableId().displayName()
                + ": row is missing primary key column: " + definition.name());
      }
    }
  }

  private PrimaryKeyTuple readPrimaryKeyTuple(ResultSet resultSet, TableSchema schema)
      throws SQLException {
    LinkedHashMap<String, Object> row = new LinkedHashMap<>();
    List<ColumnDefinition> primaryKeyColumns = schema.primaryKeyDefinitions();
    for (int index = 0; index < primaryKeyColumns.size(); index++) {
      ColumnDefinition column = primaryKeyColumns.get(index);
      row.put(column.name(), readColumnValue(resultSet, index + 1, column));
    }
    return primaryKeyTuple(schema, row, "primary-key read");
  }

  /**
   * Reads one ResultSet column using a type-aware JDBC path so the value semantics align
   * with the binlog decoder.
   *
   * <p>MySQL {@code TIMESTAMP} is UTC-stored but session-TZ-displayed. A bare
   * {@code getObject} on a {@code TIMESTAMP} column returns a {@link LocalDateTime} whose
   * wall-clock reflects the MySQL session's {@code time_zone}, which the downstream apply
   * sink then reinterprets using the client JVM's default zone — producing a silent offset
   * drift between the chunk path and the binlog path (the binlog decoder always emits UTC
   * {@link Instant}s). Reading {@code TIMESTAMP} via
   * {@code getTimestamp(col, utcCalendar).toInstant()} pins the interpretation of the
   * returned wall-clock to UTC independent of the client JVM's default zone, and this path
   * is supported by every JDBC 3+ driver (unlike the newer Connector/J-specific
   * {@code getObject(col, Instant.class)} which has version-dependent support for
   * {@code TIMESTAMP} columns).
   *
   * <p>MySQL {@code DATETIME} is deliberately zone-less; it stays on the generic
   * {@code getObject} path so the existing normalizer keeps it as {@link LocalDateTime}.
   */
  private static final TimeZone UTC_TIMEZONE = TimeZone.getTimeZone("UTC");

  private static Object readColumnValue(ResultSet resultSet, int columnIndex, ColumnDefinition column)
      throws SQLException {
    if (column.neutralType() == NeutralColumnType.TIMESTAMP
        && isMysqlZoneAwareTimestamp(column.sourceType())) {
      // Calendar is not thread-safe and JDBC drivers may mutate it; allocate fresh per call.
      Timestamp timestamp = resultSet.getTimestamp(columnIndex, Calendar.getInstance(UTC_TIMEZONE));
      return timestamp == null ? null : timestamp.toInstant();
    }
    if (column.neutralType() == NeutralColumnType.INTEGER
        && isMysqlYearType(column.sourceType())) {
      // MySQL YEAR maps to neutral INTEGER, but Connector/J returns a java.sql.Date by
      // default (yearIsDateType=true). The normalizer's INTEGER path then tries to parse
      // that Date's toString ("2026-01-01") as a BigInteger and fails with "Illegal
      // embedded sign character", taking down the dump request. Reading the column
      // through getInt bypasses the Date conversion — Connector/J returns the year as
      // an integer regardless of the yearIsDateType flag. Mirrors the same type-aware
      // pattern used for TIMESTAMP above.
      int year = resultSet.getInt(columnIndex);
      return resultSet.wasNull() ? null : year;
    }
    return resultSet.getObject(columnIndex);
  }

  private static boolean isMysqlZoneAwareTimestamp(String sourceType) {
    if (sourceType == null) {
      return false;
    }
    // MySQL surface types canonicalised by MySqlDialect are "timestamp" (zone-aware,
    // UTC-stored) vs "datetime" (zone-less). Only the former needs UTC-Instant handling;
    // "datetime" stays as LocalDateTime through the existing normalizer passthrough.
    return sourceType.toLowerCase(Locale.ROOT).startsWith("timestamp");
  }

  private static boolean isMysqlYearType(String sourceType) {
    if (sourceType == null) {
      return false;
    }
    // MySqlDialect canonicalises YEAR source types as "year" (optionally with
    // " unsigned" / " zerofill" suffixes via the integer-like canonicaliser).
    return sourceType.toLowerCase(Locale.ROOT).startsWith("year");
  }

  private PrimaryKeyTuple primaryKeyTuple(TableSchema schema, Map<String, Object> row, String operation) {
    try {
      return schema.primaryKeyTupleFor(row);
    } catch (IllegalArgumentException failure) {
      throw new SchemaDriftException(
          "MySQL JDBC " + operation + " detected primary key drift for "
              + schema.tableId().displayName()
              + ": "
              + failure.getMessage());
    }
  }

  private PrimaryKeyTuple primaryKeyTuple(TableSchema schema, ImmutableRowImage row, String operation) {
    try {
      return schema.primaryKeyTupleFor(row);
    } catch (IllegalArgumentException failure) {
      throw new SchemaDriftException(
          "MySQL JDBC " + operation + " detected primary key drift for "
              + schema.tableId().displayName()
              + ": "
              + failure.getMessage());
    }
  }

  private static int bindPrimaryKeyTuple(
      PreparedStatement statement, int parameterIndex, TableSchema schema, PrimaryKeyTuple primaryKeyTuple)
      throws SQLException {
    List<ColumnDefinition> primaryKeyDefinitions = schema.primaryKeyDefinitions();
    for (int index = 0; index < primaryKeyDefinitions.size(); index++) {
      statement.setObject(
          parameterIndex++,
          jdbcPrimaryKeyValue(primaryKeyDefinitions.get(index), primaryKeyTuple.values().get(index)));
    }
    return parameterIndex;
  }

  private static Object jdbcPrimaryKeyValue(
      ColumnDefinition primaryKeyColumn, PrimaryKeyValue primaryKeyValue) {
    Objects.requireNonNull(primaryKeyValue, "primaryKeyValue");
    Objects.requireNonNull(primaryKeyColumn, "primaryKeyColumn");
    Object normalizedValue = primaryKeyValue.normalizedValue();
    return switch (primaryKeyColumn.neutralType()) {
      case BOOLEAN, STRING, BINARY, UUID, JSON, XML, ENUM_STRING, UNSUPPORTED -> normalizedValue;
      case INTEGER -> jdbcIntegerValue((BigInteger) normalizedValue);
      case FLOAT, DECIMAL -> normalizedValue;
      case DATE -> jdbcDateValue(normalizedValue);
      case TIME -> jdbcTimeValue(normalizedValue);
      case TIMESTAMP -> jdbcTimestampValue(normalizedValue);
    };
  }

  private static Object jdbcIntegerValue(BigInteger value) {
    if (value.bitLength() <= 63) {
      return value.longValueExact();
    }
    return new BigDecimal(value);
  }

  private static Object jdbcDateValue(Object normalizedValue) {
    if (normalizedValue instanceof LocalDate localDate) {
      return localDate;
    }
    return normalizedValue;
  }

  private static Object jdbcTimeValue(Object normalizedValue) {
    if (normalizedValue instanceof LocalTime localTime) {
      return localTime;
    }
    return normalizedValue;
  }

  private static Object jdbcTimestampValue(Object normalizedValue) {
    if (normalizedValue instanceof Instant instant) {
      return Timestamp.from(instant);
    }
    if (normalizedValue instanceof LocalDateTime localDateTime) {
      return Timestamp.valueOf(localDateTime);
    }
    return normalizedValue;
  }

  private static int queryLimit(int chunkSize) {
    if (chunkSize <= 0) {
      throw new IllegalArgumentException("chunkSize must be > 0");
    }
    if (chunkSize == Integer.MAX_VALUE) {
      throw new IllegalArgumentException("chunkSize is too large for lookahead chunk reads");
    }
    return chunkSize + 1;
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
