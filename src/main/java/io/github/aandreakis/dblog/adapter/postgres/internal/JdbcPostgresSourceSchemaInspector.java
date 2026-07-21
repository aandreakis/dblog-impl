package io.github.aandreakis.dblog.adapter.postgres.internal;

import io.github.aandreakis.dblog.adapter.postgres.PostgresDialect;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

public final class JdbcPostgresSourceSchemaInspector {
  public List<TableSchema> inspectCapturedSchemas(
      Connection connection,
      String databaseName,
      List<String> configuredTables)
      throws SQLException {
    Objects.requireNonNull(connection, "connection");
    requireNonBlank(databaseName, "databaseName");
    Objects.requireNonNull(configuredTables, "configuredTables");

    List<TableSchema> capturedSchemas = new ArrayList<>(configuredTables.size());
    for (String configuredTable : configuredTables) {
      String[] parts = parseQualifiedTable(configuredTable);
      TableSchema schema = readTableSchema(connection, new TableId(databaseName, parts[0], parts[1]));
      if (schema == null) {
        throw new IllegalStateException(
            "PostgreSQL table not found while reading captured-table schema: " + configuredTable);
      }
      capturedSchemas.add(schema);
    }
    ensureNoDuplicateDisplayNames(capturedSchemas);
    return List.copyOf(capturedSchemas);
  }

  public TableSchema readTableSchema(Connection connection, TableId tableId) throws SQLException {
    Objects.requireNonNull(connection, "connection");
    Objects.requireNonNull(tableId, "tableId");

    try (PreparedStatement statement = connection.prepareStatement(PostgresSql.capturedTableColumnsSql())) {
      statement.setString(1, tableId.schemaName());
      statement.setString(2, tableId.tableName());
      try (ResultSet resultSet = statement.executeQuery()) {
        List<ColumnDefinition> columns = new ArrayList<>();
        while (resultSet.next()) {
          int primaryKeyOrdinal = resultSet.getInt("primary_key_ordinal");
          columns.add(
              new ColumnDefinition(
                  resultSet.getString("column_name"),
                  resultSet.getString("source_type"),
                  PostgresDialect.INSTANCE.neutralType(
                      resultSet.getString("type_name"), resultSet.getString("type_kind")),
                  primaryKeyOrdinal > 0,
                  primaryKeyOrdinal,
                  resultSet.getBoolean("nullable")));
        }
        if (columns.isEmpty()) {
          return null;
        }
        TableSchema schema = TableSchema.create(tableId, columns, Instant.now());
        PostgresPrimaryKeyPolicy.requireSupportedForCapture(schema);
        return schema;
      }
    }
  }

  private static void ensureNoDuplicateDisplayNames(List<TableSchema> schemas) {
    LinkedHashSet<String> displayNames = new LinkedHashSet<>();
    for (TableSchema schema : schemas) {
      if (!displayNames.add(schema.tableId().displayName())) {
        throw new IllegalArgumentException(
            "PostgreSQL configured captured tables contain a duplicate: "
                + schema.tableId().displayName());
      }
    }
  }

  private static String[] parseQualifiedTable(String configuredTable) {
    String normalized = requireNonBlank(configuredTable, "configuredTable");
    String[] parts = normalized.split("\\.", 2);
    if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
      throw new IllegalArgumentException(
          "PostgreSQL configured tables must use adapter-native two-part names such as schema.table");
    }
    return new String[] {parts[0].trim(), parts[1].trim()};
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value.trim();
  }
}
