package io.github.aandreakis.dblog.adapter.postgres.internal;

import io.github.aandreakis.dblog.adapter.api.CachedPerSchemaSql;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public final class PostgresSql {
  private static final PostgresServerVersion CURRENT_TARGET = new PostgresServerVersion(18);

  private static final CachedPerSchemaSql<CachedSql> CACHED_SQL =
      new CachedPerSchemaSql<>(CachedSql::new);
  private static final String CAPTURED_TABLE_COLUMNS_SQL =
      "SELECT a.attname AS column_name, "
          + "pg_catalog.format_type(a.atttypid, a.atttypmod) AS source_type, "
          + "t.typname AS type_name, "
          + "t.typtype AS type_kind, "
          + "NOT a.attnotnull AS nullable, "
          + "COALESCE(("
          + "  SELECT kcu.ordinal_position "
          + "  FROM information_schema.table_constraints tc "
          + "  JOIN information_schema.key_column_usage kcu "
          + "    ON tc.constraint_name = kcu.constraint_name "
          + "   AND tc.table_schema = kcu.table_schema "
          + "   AND tc.table_name = kcu.table_name "
          + "  WHERE tc.constraint_type = 'PRIMARY KEY' "
          + "    AND tc.table_schema = n.nspname "
          + "    AND tc.table_name = c.relname "
          + "    AND kcu.column_name = a.attname "
          + "), 0) AS primary_key_ordinal "
          + "FROM pg_catalog.pg_attribute a "
          + "JOIN pg_catalog.pg_class c ON c.oid = a.attrelid "
          + "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
          + "JOIN pg_catalog.pg_type t ON t.oid = a.atttypid "
          + "WHERE n.nspname = ? "
          + "  AND c.relname = ? "
          + "  AND c.relkind IN ('r', 'p') "
          + "  AND a.attnum > 0 "
          + "  AND NOT a.attisdropped "
          + "ORDER BY a.attnum";

  private PostgresSql() {}

  public static String replicaIdentityStateSql() {
    return "SELECT c.relreplident"
        + " FROM pg_catalog.pg_class c"
        + " JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace"
        + " WHERE n.nspname = ? AND c.relname = ? AND c.relkind IN ('r', 'p')";
  }

  public static String capturedTableColumnsSql() {
    return CAPTURED_TABLE_COLUMNS_SQL;
  }

  public static String publicationStateSql(PostgresServerVersion serverVersion) {
    Objects.requireNonNull(serverVersion, "serverVersion");
    String schemaScopedProjection =
        serverVersion.supportsPublicationNamespaceCatalog()
            ? " EXISTS ("
                + " SELECT 1 FROM pg_catalog.pg_publication_namespace pn"
                + " WHERE pn.pnpubid = p.oid) AS schema_scoped,"
            : " FALSE AS schema_scoped,";
    return "SELECT p.puballtables,"
        + schemaScopedProjection
        + " p.pubinsert, p.pubupdate, p.pubdelete, p.pubtruncate, p.pubviaroot"
        + " FROM pg_catalog.pg_publication p"
        + " WHERE p.pubname = ?";
  }

  public static String publicationRelationStateSql(PostgresServerVersion serverVersion) {
    Objects.requireNonNull(serverVersion, "serverVersion");
    String filterProjection =
        serverVersion.supportsPublicationRowFilters()
            ? " pr.prqual IS NOT NULL AS row_filter_present,"
                + " pr.prattrs IS NOT NULL AS column_list_present"
            : " FALSE AS row_filter_present,"
                + " FALSE AS column_list_present";
    return "SELECT n.nspname AS schemaname,"
        + " c.relname AS tablename,"
        + filterProjection
        + " FROM pg_catalog.pg_publication_rel pr"
        + " JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid"
        + " JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid"
        + " JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace"
        + " WHERE p.pubname = ?"
        + " ORDER BY n.nspname ASC, c.relname ASC";
  }

  public static String createPublicationSql(String publicationName, List<TableId> capturedTables) {
    return "CREATE PUBLICATION "
        + quoteIdentifier(publicationName)
        + " FOR TABLE "
        + publicationTableList(capturedTables)
        + " WITH (publish = 'insert, update, delete')";
  }

  public static String alterPublicationSetTablesSql(
      String publicationName, List<TableId> capturedTables) {
    return "ALTER PUBLICATION "
        + quoteIdentifier(publicationName)
        + " SET TABLE "
        + publicationTableList(capturedTables);
  }

  public static String alterPublicationPublishOperationsSql(String publicationName) {
    return "ALTER PUBLICATION "
        + quoteIdentifier(publicationName)
        + " SET (publish = 'insert, update, delete')";
  }

  public static String replicationSlotStateSql(PostgresServerVersion serverVersion) {
    Objects.requireNonNull(serverVersion, "serverVersion");
    String failoverProjection =
        serverVersion.supportsReplicationSlotFailover()
            ? " failover,"
            : " FALSE AS failover,";
    String invalidationReasonProjection =
        serverVersion.supportsReplicationSlotInvalidationReason()
            ? " invalidation_reason"
            : " '' AS invalidation_reason";
    return "SELECT slot_name, slot_type, database, plugin, temporary, active,"
        + " two_phase,"
        + failoverProjection
        + " restart_lsn::text AS restart_lsn,"
        + " confirmed_flush_lsn::text AS confirmed_flush_lsn, wal_status,"
        + invalidationReasonProjection
        + " FROM pg_catalog.pg_replication_slots"
        + " WHERE slot_name = ?";
  }

  public static String createLogicalReplicationSlotSql(PostgresServerVersion serverVersion) {
    Objects.requireNonNull(serverVersion, "serverVersion");
    return serverVersion.supportsReplicationSlotCreationFailoverArgument()
        ? "SELECT slot_name, lsn::text AS lsn"
            + " FROM pg_catalog.pg_create_logical_replication_slot(?, ?, ?, ?, ?)"
        : "SELECT slot_name, lsn::text AS lsn"
            + " FROM pg_catalog.pg_create_logical_replication_slot(?, ?, ?, ?)";
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
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }

  private static String primaryKeyTupleExpression(TableSchema schema) {
    if (schema.hasSinglePrimaryKey()) {
      return quoteIdentifier(schema.primaryKeyColumn());
    }
    return "("
        + schema.primaryKeyColumns().stream()
            .map(PostgresSql::quoteIdentifier)
            .reduce((left, right) -> left + ", " + right)
            .orElseThrow()
        + ")";
  }

  private static String primaryKeyOrderBy(TableSchema schema, String direction) {
    return schema.primaryKeyColumns().stream()
        .map(PostgresSql::quoteIdentifier)
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
        .map(PostgresSql::quoteIdentifier)
        .reduce((left, right) -> left + ", " + right)
        .orElseThrow();
  }

  private static String publicationTableList(List<TableId> capturedTables) {
    Objects.requireNonNull(capturedTables, "capturedTables");
    if (capturedTables.isEmpty()) {
      throw new IllegalArgumentException("capturedTables must not be empty");
    }
    return capturedTables.stream()
        .map(PostgresSql::qualifiedTableName)
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
