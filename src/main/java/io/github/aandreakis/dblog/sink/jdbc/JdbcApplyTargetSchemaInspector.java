package io.github.aandreakis.dblog.sink.jdbc;

import io.github.aandreakis.dblog.adapter.mysql.MySqlDialect;
import io.github.aandreakis.dblog.adapter.postgres.PostgresDialect;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

final class JdbcApplyTargetSchemaInspector {
  private static final String MYSQL_COLUMNS_SQL =
      "SELECT c.column_name, c.data_type, c.column_type, c.is_nullable = 'YES' AS nullable, "
          + "COALESCE(s.seq_in_index, 0) AS primary_key_ordinal "
          + "FROM information_schema.columns c "
          + "LEFT JOIN information_schema.statistics s "
          + "  ON s.table_schema = c.table_schema "
          + " AND s.table_name = c.table_name "
          + " AND s.index_name = 'PRIMARY' "
          + " AND s.column_name = c.column_name "
          + "WHERE c.table_schema = ? AND c.table_name = ? "
          + "ORDER BY c.ordinal_position";

  private static final String POSTGRES_COLUMNS_SQL =
      "SELECT a.attname AS column_name, "
          + "pg_catalog.format_type(a.atttypid, a.atttypmod) AS source_type, "
          + "t.typname AS type_name, "
          + "t.typtype AS type_kind, "
          + "tn.nspname AS type_schema, "
          + "NOT a.attnotnull AS nullable, "
          + "COALESCE(( "
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
          + "JOIN pg_catalog.pg_namespace tn ON tn.oid = t.typnamespace "
          + "WHERE n.nspname = ? AND c.relname = ? "
          + "  AND c.relkind IN ('r', 'p') "
          + "  AND a.attnum > 0 AND NOT a.attisdropped "
          + "ORDER BY a.attnum";

  Optional<TargetTableMetadata> inspect(
      Connection connection, JdbcApplyTargetDialect dialect, TableId tableId) throws SQLException {
    Objects.requireNonNull(connection, "connection");
    Objects.requireNonNull(dialect, "dialect");
    Objects.requireNonNull(tableId, "tableId");
    return switch (dialect) {
      case MYSQL -> inspectMySql(connection, tableId);
      case POSTGRES -> inspectPostgres(connection, tableId);
    };
  }

  private Optional<TargetTableMetadata> inspectMySql(Connection connection, TableId tableId)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(MYSQL_COLUMNS_SQL)) {
      statement.setString(1, tableId.schemaName());
      statement.setString(2, tableId.tableName());
      try (ResultSet resultSet = statement.executeQuery()) {
        List<TargetColumnMetadata> columns = new ArrayList<>();
        while (resultSet.next()) {
          int primaryKeyOrdinal = resultSet.getInt("primary_key_ordinal");
          String dataType = resultSet.getString("data_type");
          String columnType = resultSet.getString("column_type");
          columns.add(
                  new TargetColumnMetadata(
                      resultSet.getString("column_name"),
                      MySqlDialect.INSTANCE.canonicalSourceType(dataType, columnType),
                      MySqlDialect.INSTANCE.neutralType(dataType, columnType),
                      primaryKeyOrdinal > 0,
                      primaryKeyOrdinal,
                      resultSet.getBoolean("nullable"),
                  null,
                  dataType,
                  null));
        }
        return columns.isEmpty()
            ? Optional.empty()
            : Optional.of(new TargetTableMetadata(tableId, List.copyOf(columns), false));
      }
    }
  }

  private Optional<TargetTableMetadata> inspectPostgres(Connection connection, TableId tableId)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(POSTGRES_COLUMNS_SQL)) {
      statement.setString(1, tableId.schemaName());
      statement.setString(2, tableId.tableName());
      try (ResultSet resultSet = statement.executeQuery()) {
        List<TargetColumnMetadata> columns = new ArrayList<>();
        while (resultSet.next()) {
          int primaryKeyOrdinal = resultSet.getInt("primary_key_ordinal");
          columns.add(
                  new TargetColumnMetadata(
                      resultSet.getString("column_name"),
                      resultSet.getString("source_type"),
                      PostgresDialect.INSTANCE.neutralType(
                          resultSet.getString("type_name"), resultSet.getString("type_kind")),
                      primaryKeyOrdinal > 0,
                      primaryKeyOrdinal,
                      resultSet.getBoolean("nullable"),
                  resultSet.getString("type_schema"),
                  resultSet.getString("type_name"),
                  resultSet.getString("type_kind")));
        }
        return columns.isEmpty()
            ? Optional.empty()
            : Optional.of(new TargetTableMetadata(tableId, List.copyOf(columns), true));
      }
    }
  }

  static final class TargetTableMetadata {
    private final TableId tableId;
    private final List<TargetColumnMetadata> columns;
    private final List<String> primaryKeyColumns;
    private final boolean caseSensitiveIdentifiers;
    private final Map<String, TargetColumnMetadata> columnsByExactName;
    private final Map<String, TargetColumnMetadata> columnsByLowerCaseName;

    TargetTableMetadata(TableId tableId, List<TargetColumnMetadata> columns) {
      this(tableId, columns, false);
    }

    TargetTableMetadata(
        TableId tableId, List<TargetColumnMetadata> columns, boolean caseSensitiveIdentifiers) {
      this.tableId = Objects.requireNonNull(tableId, "tableId");
      this.columns = List.copyOf(Objects.requireNonNull(columns, "columns"));
      this.caseSensitiveIdentifiers = caseSensitiveIdentifiers;
      if (this.columns.isEmpty()) {
        throw new IllegalArgumentException("columns must not be empty");
      }

      LinkedHashMap<String, TargetColumnMetadata> byExactName = new LinkedHashMap<>();
      LinkedHashMap<String, TargetColumnMetadata> byLowerCaseName = new LinkedHashMap<>();
      ArrayList<TargetColumnMetadata> primaryKeys = new ArrayList<>();
      for (TargetColumnMetadata column : this.columns) {
        byExactName.put(column.name(), column);
        byLowerCaseName.put(column.name().toLowerCase(java.util.Locale.ROOT), column);
        if (column.primaryKey()) {
          primaryKeys.add(column);
        }
      }
      primaryKeys.sort(
          java.util.Comparator.comparingInt(TargetColumnMetadata::primaryKeyOrdinal)
              .thenComparing(TargetColumnMetadata::name));
      this.primaryKeyColumns = primaryKeys.stream().map(TargetColumnMetadata::name).toList();
      this.columnsByExactName = Map.copyOf(byExactName);
      this.columnsByLowerCaseName = Map.copyOf(byLowerCaseName);
    }

    List<String> primaryKeyColumns() {
      return primaryKeyColumns;
    }

    Optional<TargetColumnMetadata> findColumn(String name) {
      Objects.requireNonNull(name, "name");
      if (caseSensitiveIdentifiers) {
        return Optional.ofNullable(columnsByExactName.get(name));
      }
      return Optional.ofNullable(columnsByLowerCaseName.get(name.toLowerCase(java.util.Locale.ROOT)));
    }

    TargetColumnMetadata requireColumn(String name) {
      return findColumn(name)
          .orElseThrow(
              () ->
                  new IllegalStateException(
                      "Target table is missing required column metadata: "
                          + tableId.displayName()
                          + " column="
                          + name));
    }

    boolean matchesPrimaryKeyColumns(List<String> expectedPrimaryKeyColumns) {
      Objects.requireNonNull(expectedPrimaryKeyColumns, "expectedPrimaryKeyColumns");
      if (expectedPrimaryKeyColumns.size() != primaryKeyColumns.size()) {
        return false;
      }
      for (int index = 0; index < expectedPrimaryKeyColumns.size(); index++) {
        String expected = expectedPrimaryKeyColumns.get(index);
        String actual = primaryKeyColumns.get(index);
        if (caseSensitiveIdentifiers) {
          if (!actual.equals(expected)) {
            return false;
          }
          continue;
        }
        if (!actual.equalsIgnoreCase(expected)) {
          return false;
        }
      }
      return true;
    }
  }

  static final class TargetColumnMetadata {
    private final String name;
    private final String sourceType;
    private final NeutralColumnType neutralType;
    private final boolean primaryKey;
    private final int primaryKeyOrdinal;
    private final boolean nullable;
    private final String typeSchema;
    private final String typeName;
    private final String typeKind;
    private final ColumnDefinition coercionDefinition;

    TargetColumnMetadata(
        String name,
        String sourceType,
        NeutralColumnType neutralType,
        boolean primaryKey,
        int primaryKeyOrdinal,
        boolean nullable,
        String typeSchema,
        String typeName,
        String typeKind) {
      this.name = Objects.requireNonNull(name, "name");
      this.sourceType = Objects.requireNonNull(sourceType, "sourceType");
      this.neutralType = Objects.requireNonNull(neutralType, "neutralType");
      this.primaryKey = primaryKey;
      this.primaryKeyOrdinal = primaryKey ? Math.max(1, primaryKeyOrdinal) : 0;
      this.nullable = nullable;
      this.typeSchema = typeSchema;
      this.typeName = typeName;
      this.typeKind = typeKind;
      this.coercionDefinition =
          new ColumnDefinition(
              this.name,
              this.sourceType,
              this.neutralType,
              this.primaryKey,
              this.primaryKeyOrdinal,
              this.nullable);
    }

    String name() {
      return name;
    }

    String sourceType() {
      return sourceType;
    }

    NeutralColumnType neutralType() {
      return neutralType;
    }

    boolean primaryKey() {
      return primaryKey;
    }

    int primaryKeyOrdinal() {
      return primaryKeyOrdinal;
    }

    boolean nullable() {
      return nullable;
    }

    String typeSchema() {
      return typeSchema;
    }

    String typeName() {
      return typeName;
    }

    String typeKind() {
      return typeKind;
    }

    ColumnDefinition coercionDefinition() {
      return coercionDefinition;
    }
  }
}
