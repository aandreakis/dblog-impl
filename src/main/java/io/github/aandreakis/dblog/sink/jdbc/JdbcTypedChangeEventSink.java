package io.github.aandreakis.dblog.sink.jdbc;

import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.sink.api.ChangeEventSink;
import io.github.aandreakis.dblog.sink.api.SinkSchemaValidator;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * H2-backed inspection sink that mirrors source state with one target table per captured source
 * table. Schema is derived once from the {@link SinkSchemaValidator#validateCapturedSchemas} hook
 * at runtime bootstrap; from then on each change event is applied as a MERGE (insert / update /
 * select-origin refresh) or DELETE keyed on the source primary key. There are no event-log or
 * value-detail tables — only the latest row per primary key per source table — so write
 * amplification is one statement per event regardless of how many columns the row carries.
 *
 * <p>The sink keeps in-memory counters of total / log-origin / select-origin events applied since
 * open so existing callers can still ask for an event-count summary, but no event history is
 * persisted.
 */
public final class JdbcTypedChangeEventSink implements ChangeEventSink, SinkSchemaValidator {
  private static final String H2_DRIVER_CLASS_NAME = "org.h2.Driver";

  private final String driverClassName;
  private final String jdbcUrl;
  private final SqlConnectionSource connectionSource;
  private final Map<TableId, TargetTable> targetTablesBySource = new ConcurrentHashMap<>();
  private final AtomicLong totalEvents = new AtomicLong();
  private final AtomicLong logEvents = new AtomicLong();
  private final AtomicLong selectEvents = new AtomicLong();

  public JdbcTypedChangeEventSink(String driverClassName, String jdbcUrl) {
    this(
        driverClassName,
        jdbcUrl,
        () -> java.sql.DriverManager.getConnection(Objects.requireNonNull(jdbcUrl, "jdbcUrl")));
  }

  JdbcTypedChangeEventSink(
      String driverClassName, String jdbcUrl, SqlConnectionSource connectionSource) {
    this.driverClassName = requireNonBlank(driverClassName, "driverClassName");
    this.jdbcUrl = requireNonBlank(jdbcUrl, "jdbcUrl");
    this.connectionSource = Objects.requireNonNull(connectionSource, "connectionSource");
    loadDriver(this.driverClassName);
    ensureParentDirectoryExists(jdbcUrl);
  }

  public static JdbcTypedChangeEventSink forH2(Path path) {
    String jdbcUrl = h2JdbcUrl(path);
    return new JdbcTypedChangeEventSink(
        H2_DRIVER_CLASS_NAME,
        jdbcUrl,
        new HikariTypedSinkConnectionPool(H2_DRIVER_CLASS_NAME, jdbcUrl, "typed-h2-sink"));
  }

  public static String h2JdbcUrl(Path path) {
    Objects.requireNonNull(path, "path");
    return "jdbc:h2:file:"
        + path.toAbsolutePath().normalize().toString().replace('\\', '/')
        + ";DB_CLOSE_ON_EXIT=FALSE";
  }

  /**
   * Creates one mirror table per captured source schema. Called once by the runtime bootstrap
   * via {@link SinkSchemaValidator}. Idempotent — re-applying the same schema list against an
   * existing sink file is a no-op when the tables already exist.
   */
  @Override
  public void validateCapturedSchemas(List<TableSchema> capturedSchemas) {
    Objects.requireNonNull(capturedSchemas, "capturedSchemas");
    withTransaction(
        connection -> {
          for (TableSchema schema : capturedSchemas) {
            TargetTable target = TargetTable.from(schema);
            ensureMirrorTable(connection, target);
            targetTablesBySource.put(schema.tableId(), target);
          }
          return null;
        });
  }

  @Override
  public void appendEvents(List<ChangeEvent> events) {
    Objects.requireNonNull(events, "events");
    if (events.isEmpty()) {
      return;
    }
    // Group events by source TableId so we can prepare one MERGE and one DELETE statement per
    // target table and batch all of that table's writes in a single round trip.
    Map<TableId, List<ChangeEvent>> grouped = new LinkedHashMap<>();
    for (ChangeEvent event : events) {
      OperationType op = event.operationType();
      if (op == OperationType.WATERMARK || op == OperationType.HEARTBEAT) {
        continue;
      }
      TargetTable target = targetTablesBySource.get(event.tableId());
      if (target == null) {
        // No mirror table for this source table — accept silently. Bootstrap may not yet have
        // populated targets for tables outside the captured set (e.g. metadata-only events
        // surfacing from adapters that decode past the captured surface).
        continue;
      }
      grouped.computeIfAbsent(event.tableId(), key -> new ArrayList<>()).add(event);
    }
    if (grouped.isEmpty()) {
      return;
    }
    withTransaction(
        connection -> {
          for (Map.Entry<TableId, List<ChangeEvent>> entry : grouped.entrySet()) {
            applyEventsForTable(connection, targetTablesBySource.get(entry.getKey()), entry.getValue());
          }
          return null;
        });
  }

  /**
   * Returns counter snapshots from the in-memory accumulators rather than from a persisted log.
   * Counts are best-effort — they are not persisted and reset to zero whenever the sink is
   * reopened.
   */
  public EventSummary summarizeEvents() {
    return new EventSummary(totalEvents.get(), logEvents.get(), selectEvents.get());
  }

  /**
   * Returns the first non-null value of {@code columnName} in the mirror table for {@code
   * tableId}, ordered by primary key. Used by benchmark / inspection tests to verify a column's
   * type round-tripped end-to-end without scanning the whole mirror.
   */
  public Optional<Object> loadFirstNonNullAfterValue(TableId tableId, String columnName) {
    TargetTable target = requireKnownTable(tableId);
    requireNonBlank(columnName, "columnName");
    String quotedColumn = target.quotedColumnNames.get(columnName.toLowerCase(Locale.ROOT));
    if (quotedColumn == null) {
      // Unknown column → empty result (rather than throw). Matches the original sink's
      // behavior for queries against columns that aren't part of the captured schema.
      return Optional.empty();
    }
    String sql =
        "SELECT "
            + quotedColumn
            + " FROM "
            + target.quotedTableName
            + " WHERE "
            + quotedColumn
            + " IS NOT NULL ORDER BY "
            + target.primaryKeyOrderByClause
            + " FETCH FIRST 1 ROW ONLY";
    return withTransaction(
        connection -> {
          try (PreparedStatement statement = connection.prepareStatement(sql);
              ResultSet resultSet = statement.executeQuery()) {
            if (!resultSet.next()) {
              return Optional.empty();
            }
            Object value = resultSet.getObject(1);
            return Optional.ofNullable(value);
          } catch (SQLException ex) {
            throw new IllegalStateException(
                "Typed JDBC sink loadFirstNonNullAfterValue failed for "
                    + tableId.displayName()
                    + "/"
                    + columnName,
                ex);
          }
        });
  }

  /**
   * Returns the row currently mirrored for {@code primaryKey} in {@code tableId}, or empty when
   * no row matches. Useful for tests that want to assert the latest applied state for a known
   * primary key.
   */
  public Optional<Map<String, Object>> loadCurrentRow(
      TableId tableId, ImmutableRowImage primaryKey) {
    Objects.requireNonNull(primaryKey, "primaryKey");
    TargetTable target = requireKnownTable(tableId);
    String sql =
        "SELECT * FROM "
            + target.quotedTableName
            + " WHERE "
            + target.primaryKeyWhereClause;
    return withTransaction(
        connection -> {
          try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int index = 0; index < target.primaryKeyColumnNames.size(); index++) {
              String pkColumn = target.primaryKeyColumnNames.get(index);
              Object value = primaryKey.get(pkColumn);
              bindValue(statement, index + 1, value, target.columnNeutralTypes.get(pkColumn));
            }
            try (ResultSet resultSet = statement.executeQuery()) {
              if (!resultSet.next()) {
                return Optional.empty();
              }
              return Optional.of(toRowMap(resultSet));
            }
          } catch (SQLException ex) {
            throw new IllegalStateException(
                "Typed JDBC sink loadCurrentRow failed for " + tableId.displayName(), ex);
          }
        });
  }

  /** Returns the number of rows currently mirrored for {@code tableId}. */
  public long countRows(TableId tableId) {
    TargetTable target = requireKnownTable(tableId);
    return withTransaction(
        connection -> {
          try (PreparedStatement statement =
                  connection.prepareStatement("SELECT COUNT(*) FROM " + target.quotedTableName);
              ResultSet resultSet = statement.executeQuery()) {
            return resultSet.next() ? resultSet.getLong(1) : 0L;
          } catch (SQLException ex) {
            throw new IllegalStateException(
                "Typed JDBC sink countRows failed for " + tableId.displayName(), ex);
          }
        });
  }

  @Override
  public void close() {
    try {
      connectionSource.close();
    } catch (Exception ex) {
      throw new IllegalStateException("Failed to close typed JDBC sink for " + jdbcUrl, ex);
    }
  }

  // ---- internals -----------------------------------------------------------------------------

  private void applyEventsForTable(
      Connection connection, TargetTable target, List<ChangeEvent> events) throws SQLException {
    PreparedStatement mergeStatement = null;
    PreparedStatement deleteStatement = null;
    boolean mergeBatched = false;
    boolean deleteBatched = false;
    try {
      for (ChangeEvent event : events) {
        totalEvents.incrementAndGet();
        if (event.captureOrigin() == CaptureOrigin.LOG) {
          logEvents.incrementAndGet();
        } else if (event.captureOrigin() == CaptureOrigin.SELECT) {
          selectEvents.incrementAndGet();
        }
        if (event.operationType() == OperationType.DELETE) {
          if (deleteStatement == null) {
            deleteStatement = connection.prepareStatement(target.deleteSql);
          }
          bindPrimaryKey(deleteStatement, target, event.primaryKey());
          deleteStatement.addBatch();
          deleteBatched = true;
        } else {
          // INSERT / UPDATE / SELECT-origin refresh — all upsert to current state. We use the
          // afterRow when present, else the primaryKey (e.g. for a SELECT-origin row image with
          // only the key materialized).
          ImmutableRowImage source = event.afterRow() != null ? event.afterRow() : event.primaryKey();
          if (mergeStatement == null) {
            mergeStatement = connection.prepareStatement(target.mergeSql);
          }
          bindMergeValues(mergeStatement, target, source);
          mergeStatement.addBatch();
          mergeBatched = true;
        }
      }
      if (mergeBatched) {
        mergeStatement.executeBatch();
      }
      if (deleteBatched) {
        deleteStatement.executeBatch();
      }
    } finally {
      closeQuietly(mergeStatement);
      closeQuietly(deleteStatement);
    }
  }

  private static void bindPrimaryKey(
      PreparedStatement statement, TargetTable target, ImmutableRowImage primaryKey)
      throws SQLException {
    for (int index = 0; index < target.primaryKeyColumnNames.size(); index++) {
      String pkColumn = target.primaryKeyColumnNames.get(index);
      Object value = primaryKey.get(pkColumn);
      bindValue(statement, index + 1, value, target.columnNeutralTypes.get(pkColumn));
    }
  }

  private static void bindMergeValues(
      PreparedStatement statement, TargetTable target, ImmutableRowImage source)
      throws SQLException {
    for (int index = 0; index < target.allColumnNames.size(); index++) {
      String columnName = target.allColumnNames.get(index);
      Object value = source.containsKey(columnName) ? source.get(columnName) : null;
      bindValue(statement, index + 1, value, target.columnNeutralTypes.get(columnName));
    }
  }

  private static void bindValue(
      PreparedStatement statement, int parameterIndex, Object value, NeutralColumnType type)
      throws SQLException {
    if (value == null) {
      statement.setNull(parameterIndex, sqlTypeFor(type));
      return;
    }
    if (value instanceof byte[] bytes) {
      statement.setBytes(parameterIndex, bytes);
      return;
    }
    if (value instanceof BigInteger bigInteger) {
      statement.setObject(parameterIndex, new BigDecimal(bigInteger), Types.DECIMAL);
      return;
    }
    if (value instanceof java.time.Instant
        || value instanceof java.time.LocalDate
        || value instanceof java.time.LocalDateTime
        || value instanceof java.time.LocalTime
        || value instanceof java.time.OffsetDateTime
        || value instanceof java.time.ZonedDateTime
        || value instanceof java.util.UUID
        || value instanceof BigDecimal
        || value instanceof Boolean
        || value instanceof Number
        || value instanceof CharSequence) {
      statement.setObject(parameterIndex, value);
      return;
    }
    // Fallback: render via toString. Keeps the sink usable for unanticipated value classes
    // without throwing — callers expecting strict typing can read back via setObject decode.
    statement.setObject(parameterIndex, value.toString(), Types.VARCHAR);
  }

  private static int sqlTypeFor(NeutralColumnType type) {
    if (type == null) {
      return Types.VARCHAR;
    }
    return switch (type) {
      case BOOLEAN -> Types.BOOLEAN;
      case INTEGER -> Types.BIGINT;
      case FLOAT -> Types.DOUBLE;
      case DECIMAL -> Types.DECIMAL;
      case STRING, JSON, XML, ENUM_STRING, UNSUPPORTED -> Types.VARCHAR;
      case BINARY -> Types.VARBINARY;
      case DATE -> Types.DATE;
      case TIME -> Types.TIME;
      case TIMESTAMP -> Types.TIMESTAMP;
      case UUID -> Types.OTHER;
    };
  }

  private static void ensureMirrorTable(Connection connection, TargetTable target)
      throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate(target.createTableSql);
    }
  }

  private static Map<String, Object> toRowMap(ResultSet resultSet) throws SQLException {
    ResultSetMetaData metadata = resultSet.getMetaData();
    Map<String, Object> row = new LinkedHashMap<>();
    for (int index = 1; index <= metadata.getColumnCount(); index++) {
      row.put(metadata.getColumnLabel(index), resultSet.getObject(index));
    }
    return row;
  }

  private TargetTable requireKnownTable(TableId tableId) {
    Objects.requireNonNull(tableId, "tableId");
    TargetTable target = targetTablesBySource.get(tableId);
    if (target == null) {
      throw new IllegalStateException(
          "Mirror table not configured for "
              + tableId.displayName()
              + "; call validateCapturedSchemas first or include this table in the captured set");
    }
    return target;
  }

  private <T> T withTransaction(SqlWork<T> work) {
    try (Connection connection = connectionSource.open()) {
      boolean originalAutoCommit = connection.getAutoCommit();
      connection.setAutoCommit(false);
      try {
        T value = work.execute(connection);
        connection.commit();
        return value;
      } catch (RuntimeException | SQLException ex) {
        rollbackQuietly(connection);
        throw ex instanceof RuntimeException
            ? (RuntimeException) ex
            : new IllegalStateException("Typed JDBC sink operation failed", ex);
      } finally {
        try {
          connection.setAutoCommit(originalAutoCommit);
        } catch (SQLException ignored) {
          // ignore restore failures on close path
        }
      }
    } catch (SQLException ex) {
      throw new IllegalStateException("Typed JDBC sink operation failed", ex);
    }
  }

  private static void rollbackQuietly(Connection connection) {
    try {
      connection.rollback();
    } catch (SQLException ignored) {
      // best effort only
    }
  }

  private static void closeQuietly(PreparedStatement statement) {
    if (statement == null) {
      return;
    }
    try {
      statement.close();
    } catch (SQLException ignored) {
      // best effort only
    }
  }

  private static void ensureParentDirectoryExists(String jdbcUrl) {
    if (!jdbcUrl.startsWith("jdbc:h2:file:")) {
      return;
    }
    String pathValue = jdbcUrl.substring("jdbc:h2:file:".length());
    int paramsIndex = pathValue.indexOf(';');
    if (paramsIndex >= 0) {
      pathValue = pathValue.substring(0, paramsIndex);
    }
    Path path = Path.of(pathValue);
    Path parent = path.toAbsolutePath().normalize().getParent();
    if (parent == null) {
      return;
    }
    try {
      Files.createDirectories(parent);
    } catch (Exception ex) {
      throw new IllegalStateException(
          "Failed to create typed sink parent directory: " + parent, ex);
    }
  }

  private static void loadDriver(String driverClassName) {
    try {
      Class.forName(driverClassName);
    } catch (ClassNotFoundException ex) {
      throw new IllegalStateException(
          "Required JDBC driver is not on the classpath: " + driverClassName, ex);
    }
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  /** Pre-rendered mirror schema for one source table. */
  private record TargetTable(
      String quotedTableName,
      List<String> allColumnNames,
      List<String> primaryKeyColumnNames,
      Map<String, NeutralColumnType> columnNeutralTypes,
      Map<String, String> quotedColumnNames,
      String createTableSql,
      String mergeSql,
      String deleteSql,
      String primaryKeyWhereClause,
      String primaryKeyOrderByClause) {

    static TargetTable from(TableSchema schema) {
      List<ColumnDefinition> columns = schema.selectedColumns();
      if (columns.isEmpty()) {
        throw new IllegalStateException(
            "Cannot build mirror table for "
                + schema.tableId().displayName()
                + ": no selected columns");
      }
      List<String> allColumnNames = new ArrayList<>(columns.size());
      Map<String, NeutralColumnType> typesByLowerName = new LinkedHashMap<>();
      Map<String, String> quotedByLowerName = new LinkedHashMap<>();
      List<String> quotedColumnDdls = new ArrayList<>(columns.size());
      List<String> quotedAllNames = new ArrayList<>(columns.size());
      for (ColumnDefinition column : columns) {
        String quoted = quoteIdentifier(column.name());
        quotedColumnDdls.add(
            quoted
                + " "
                + h2ColumnType(column.neutralType(), column.primaryKey(), column.sourceType()));
        quotedAllNames.add(quoted);
        allColumnNames.add(column.name());
        typesByLowerName.put(column.name().toLowerCase(Locale.ROOT), column.neutralType());
        quotedByLowerName.put(column.name().toLowerCase(Locale.ROOT), quoted);
      }
      List<String> primaryKeyColumnNames = new ArrayList<>();
      List<String> quotedPrimaryKeyNames = new ArrayList<>();
      for (ColumnDefinition pk : schema.primaryKeyDefinitions()) {
        primaryKeyColumnNames.add(pk.name());
        quotedPrimaryKeyNames.add(quoteIdentifier(pk.name()));
      }
      if (primaryKeyColumnNames.isEmpty()) {
        throw new IllegalStateException(
            "Cannot build mirror table for "
                + schema.tableId().displayName()
                + ": source schema has no primary key");
      }
      String quotedTableName = quoteIdentifier(MirrorTableNaming.deriveMirrorTableName(schema.tableId()));
      String createSql =
          "CREATE TABLE IF NOT EXISTS "
              + quotedTableName
              + " ("
              + String.join(", ", quotedColumnDdls)
              + ", PRIMARY KEY ("
              + String.join(", ", quotedPrimaryKeyNames)
              + "))";
      String placeholders = String.join(", ", repeat("?", quotedAllNames.size()));
      String mergeSql =
          "MERGE INTO "
              + quotedTableName
              + " ("
              + String.join(", ", quotedAllNames)
              + ") KEY ("
              + String.join(", ", quotedPrimaryKeyNames)
              + ") VALUES ("
              + placeholders
              + ")";
      String pkPredicate = String.join(" AND ", suffixEach(quotedPrimaryKeyNames, " = ?"));
      String deleteSql = "DELETE FROM " + quotedTableName + " WHERE " + pkPredicate;
      String pkOrderBy = String.join(", ", quotedPrimaryKeyNames);
      return new TargetTable(
          quotedTableName,
          List.copyOf(allColumnNames),
          List.copyOf(primaryKeyColumnNames),
          Map.copyOf(typesByLowerName),
          Map.copyOf(quotedByLowerName),
          createSql,
          mergeSql,
          deleteSql,
          pkPredicate,
          pkOrderBy);
    }
  }

  private static String quoteIdentifier(String identifier) {
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }

  private static List<String> repeat(String token, int count) {
    List<String> out = new ArrayList<>(count);
    for (int index = 0; index < count; index++) {
      out.add(token);
    }
    return out;
  }

  private static List<String> suffixEach(List<String> values, String suffix) {
    List<String> out = new ArrayList<>(values.size());
    for (String value : values) {
      out.add(value + suffix);
    }
    return out;
  }

  private static final Pattern DECIMAL_SPEC_PATTERN =
      Pattern.compile(
          "(?:decimal|numeric)\\s*\\((\\d+)(?:\\s*,\\s*(\\d+))?\\)", Pattern.CASE_INSENSITIVE);

  /**
   * Returns the H2 column-type clause for a source column. For DECIMAL columns we parse the
   * source's declared precision and scale (e.g. {@code decimal(18,4)}) when the {@code
   * sourceType} string carries it so the mirror preserves the source's scale exactly. Falls back
   * to a wide default when the source type cannot be parsed.
   */
  private static String h2ColumnType(NeutralColumnType type, boolean primaryKey, String sourceType) {
    return switch (type) {
      case BOOLEAN -> "BOOLEAN";
      case INTEGER -> "BIGINT";
      case FLOAT -> "DOUBLE PRECISION";
      case DECIMAL -> {
        String parsed = parseDecimalSpec(sourceType);
        yield parsed == null ? "DECIMAL(38, 10)" : "DECIMAL" + parsed;
      }
      // Use VARCHAR (not CLOB) for non-PK strings so H2 returns String values directly via
      // getObject(), avoiding CLOB materialization in test assertions and benchmark verifiers.
      // 1 MB is generous for typical change-event payloads while still fitting in a regular
      // VARCHAR column on every supported H2 build.
      case STRING, JSON, XML, ENUM_STRING, UNSUPPORTED -> primaryKey ? "VARCHAR(512)" : "VARCHAR(1048576)";
      case BINARY -> primaryKey ? "VARBINARY(512)" : "VARBINARY(1048576)";
      case DATE -> "DATE";
      case TIME -> "TIME(6)";
      case TIMESTAMP -> "TIMESTAMP(6)";
      case UUID -> "UUID";
    };
  }

  private static String parseDecimalSpec(String sourceType) {
    if (sourceType == null) {
      return null;
    }
    Matcher matcher = DECIMAL_SPEC_PATTERN.matcher(sourceType);
    if (!matcher.find()) {
      return null;
    }
    String precision = matcher.group(1);
    String scale = matcher.group(2);
    return scale == null ? "(" + precision + ")" : "(" + precision + ", " + scale + ")";
  }

  public record EventSummary(long totalEvents, long logEvents, long selectEvents) {}

  @FunctionalInterface
  private interface SqlWork<T> {
    T execute(Connection connection) throws SQLException;
  }

  @FunctionalInterface
  interface SqlConnectionSource extends AutoCloseable {
    Connection open() throws SQLException;

    @Override
    default void close() throws Exception {
      // default no-op
    }
  }
}
