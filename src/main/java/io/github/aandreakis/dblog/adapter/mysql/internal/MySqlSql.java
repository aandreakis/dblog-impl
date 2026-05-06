package io.github.aandreakis.dblog.adapter.mysql.internal;

import io.github.aandreakis.dblog.adapter.api.CachedPerSchemaSql;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public final class MySqlSql {
  private static final CachedPerSchemaSql<CachedSql> CACHED_SQL =
      new CachedPerSchemaSql<>(CachedSql::new);

  private MySqlSql() {}

  public static String serverCapabilitiesSql() {
    return "SELECT @@GLOBAL.log_bin, @@GLOBAL.binlog_format, @@GLOBAL.binlog_row_image, @@GLOBAL.gtid_mode, @@GLOBAL.lower_case_table_names, @@GLOBAL.binlog_row_metadata";
  }

  public static String sessionBinlogSettingsSql() {
    return "SELECT @@SESSION.binlog_format, @@SESSION.binlog_row_image";
  }

  public static String capturedTableColumnsSql() {
    return "SELECT c.column_name, c.data_type, c.column_type, c.is_nullable,"
        + " CASE WHEN s.index_name = 'PRIMARY' THEN COALESCE(s.seq_in_index, 0) ELSE 0 END AS primary_key_ordinal"
        + " FROM information_schema.columns c"
        + " LEFT JOIN information_schema.statistics s"
        + " ON s.table_schema = c.table_schema"
        + " AND s.table_name = c.table_name"
        + " AND s.column_name = c.column_name"
        + " AND s.index_name = 'PRIMARY'"
        + " WHERE c.table_schema = ? AND c.table_name = ?"
        + " ORDER BY c.ordinal_position ASC";
  }

  public static String tableScanUpperBoundPrimaryKeySql(TableSchema schema) {
    return CACHED_SQL.get(schema).upperBoundSql();
  }

  public static String tableChunkReadSql(
      TableSchema schema, boolean resumeAfterPrimaryKey, boolean stopAtPrimaryKey) {
    return CACHED_SQL.get(schema).tableChunkReadSql(resumeAfterPrimaryKey, stopAtPrimaryKey);
  }

  public static String targetedPrimaryKeysReadSql(TableSchema schema, int primaryKeyCount) {
    return CACHED_SQL.get(schema).targetedPrimaryKeysReadSql(primaryKeyCount);
  }

  public static String qualifiedTableName(TableSchema schema) {
    Objects.requireNonNull(schema, "schema");
    return qualifiedTableName(schema.tableId());
  }

  public static String qualifiedTableName(TableId tableId) {
    Objects.requireNonNull(tableId, "tableId");
    return quoteIdentifier(tableId.schemaName()) + "." + quoteIdentifier(tableId.tableName());
  }

  public static String quoteIdentifier(String identifier) {
    Objects.requireNonNull(identifier, "identifier");
    if (identifier.isBlank()) {
      throw new IllegalArgumentException("identifier must not be blank");
    }
    return "`" + identifier.replace("`", "``") + "`";
  }

  private static String primaryKeyTupleExpression(TableSchema schema) {
    if (schema.hasSinglePrimaryKey()) {
      return quoteIdentifier(schema.primaryKeyColumn());
    }
    return "("
        + schema.primaryKeyColumns().stream()
            .map(MySqlSql::quoteIdentifier)
            .reduce((left, right) -> left + ", " + right)
            .orElseThrow()
        + ")";
  }

  private static String primaryKeyOrderBy(TableSchema schema, String direction) {
    return schema.primaryKeyColumns().stream()
        .map(MySqlSql::quoteIdentifier)
        .map(column -> column + " " + direction)
        .reduce((left, right) -> left + ", " + right)
        .orElseThrow();
  }

  private static String primaryKeyTuplePlaceholders(
      boolean singlePrimaryKey, int primaryKeyWidth, int tupleCount) {
    if (tupleCount <= 0) {
      throw new IllegalArgumentException("tupleCount must be > 0");
    }
    if (singlePrimaryKey) {
      return placeholders(tupleCount);
    }
    String tuplePlaceholder = "(" + placeholders(primaryKeyWidth) + ")";
    StringBuilder builder = new StringBuilder();
    for (int index = 0; index < tupleCount; index++) {
      if (index > 0) {
        builder.append(", ");
      }
      builder.append(tuplePlaceholder);
    }
    return builder.toString();
  }

  private static String placeholders(int count) {
    StringBuilder builder = new StringBuilder();
    for (int index = 0; index < count; index++) {
      if (index > 0) {
        builder.append(", ");
      }
      builder.append("?");
    }
    return builder.toString();
  }

  private static String selectList(List<ColumnDefinition> columns) {
    return columns.stream()
        .map(ColumnDefinition::name)
        .map(MySqlSql::quoteIdentifier)
        .reduce((left, right) -> left + ", " + right)
        .orElseThrow();
  }

  private static final class CachedSql {
    private final boolean singlePrimaryKey;
    private final int primaryKeyWidth;
    private final String selectedProjection;
    private final String primaryKeyProjection;
    private final String qualifiedTableName;
    private final String primaryKeyTupleExpression;
    private final String primaryKeyOrderAsc;
    private final String primaryKeyOrderDesc;
    private final String upperBoundSql;
    private final String tableChunkNoBoundsSql;
    private final String tableChunkStartOnlySql;
    private final String tableChunkStopOnlySql;
    private final String tableChunkStartAndStopSql;
    // Entry per distinct PRIMARY_KEYS request size ever seen against this schema. Bounded
    // indirectly: the outer CACHED_SQL is a WeakHashMap keyed by TableSchema, so when the
    // schema is replaced the entire CachedSql (and this inner map) becomes eligible for GC.
    // Within one schema the entry count is bounded by the distinct batch sizes callers
    // actually use; real workloads converge on a small number of canonical sizes.
    private final ConcurrentHashMap<Integer, String> targetedByCount = new ConcurrentHashMap<>();

    private CachedSql(TableSchema schema) {
      this.singlePrimaryKey = schema.hasSinglePrimaryKey();
      this.primaryKeyWidth = schema.primaryKeyColumns().size();
      this.selectedProjection = selectList(schema.selectedColumns());
      this.primaryKeyProjection = selectList(schema.primaryKeyDefinitions());
      this.qualifiedTableName = qualifiedTableName(schema);
      this.primaryKeyTupleExpression = primaryKeyTupleExpression(schema);
      this.primaryKeyOrderAsc = primaryKeyOrderBy(schema, "ASC");
      this.primaryKeyOrderDesc = primaryKeyOrderBy(schema, "DESC");
      this.upperBoundSql =
          "SELECT "
              + primaryKeyProjection
              + " FROM "
              + qualifiedTableName
              + " ORDER BY "
              + primaryKeyOrderDesc
              + " LIMIT 1";
      this.tableChunkNoBoundsSql =
          "SELECT "
              + selectedProjection
              + " FROM "
              + qualifiedTableName
              + " ORDER BY "
              + primaryKeyOrderAsc
              + " LIMIT ?";
      this.tableChunkStartOnlySql =
          "SELECT "
              + selectedProjection
              + " FROM "
              + qualifiedTableName
              + " WHERE "
              + primaryKeyTupleExpression
              + " > "
              + primaryKeyTuplePlaceholders(singlePrimaryKey, primaryKeyWidth, 1)
              + " ORDER BY "
              + primaryKeyOrderAsc
              + " LIMIT ?";
      this.tableChunkStopOnlySql =
          "SELECT "
              + selectedProjection
              + " FROM "
              + qualifiedTableName
              + " WHERE "
              + primaryKeyTupleExpression
              + " <= "
              + primaryKeyTuplePlaceholders(singlePrimaryKey, primaryKeyWidth, 1)
              + " ORDER BY "
              + primaryKeyOrderAsc
              + " LIMIT ?";
      this.tableChunkStartAndStopSql =
          "SELECT "
              + selectedProjection
              + " FROM "
              + qualifiedTableName
              + " WHERE "
              + primaryKeyTupleExpression
              + " > "
              + primaryKeyTuplePlaceholders(singlePrimaryKey, primaryKeyWidth, 1)
              + " AND "
              + primaryKeyTupleExpression
              + " <= "
              + primaryKeyTuplePlaceholders(singlePrimaryKey, primaryKeyWidth, 1)
              + " ORDER BY "
              + primaryKeyOrderAsc
              + " LIMIT ?";
    }

    private String upperBoundSql() {
      return upperBoundSql;
    }

    private String tableChunkReadSql(boolean resumeAfterPrimaryKey, boolean stopAtPrimaryKey) {
      if (resumeAfterPrimaryKey) {
        return stopAtPrimaryKey ? tableChunkStartAndStopSql : tableChunkStartOnlySql;
      }
      return stopAtPrimaryKey ? tableChunkStopOnlySql : tableChunkNoBoundsSql;
    }

    private String targetedPrimaryKeysReadSql(int primaryKeyCount) {
      if (primaryKeyCount <= 0) {
        throw new IllegalArgumentException("primaryKeyCount must be > 0");
      }
      return targetedByCount.computeIfAbsent(
          primaryKeyCount,
          count ->
              "SELECT "
                  + selectedProjection
                  + " FROM "
                  + qualifiedTableName
                  + " WHERE "
                  + primaryKeyTupleExpression
                  + " IN ("
                  + primaryKeyTuplePlaceholders(singlePrimaryKey, primaryKeyWidth, count)
                  + ")");
    }
  }
}
