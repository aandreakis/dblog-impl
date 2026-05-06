package io.github.aandreakis.dblog.adapter.mysql.internal;

import io.github.aandreakis.dblog.adapter.mysql.MySqlDialect;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public final class JdbcMySqlSourceSchemaInspector {
  public MySqlServerCapabilities readServerCapabilities(Connection connection) throws SQLException {
    Objects.requireNonNull(connection, "connection");

    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(MySqlSql.serverCapabilitiesSql())) {
      if (!resultSet.next()) {
        throw new IllegalStateException("MySQL server capability query did not return a row");
      }
      boolean binaryLoggingEnabled = parseBooleanCapability(resultSet.getObject(1));
      String binlogFormat = resultSet.getString(2);
      String binlogRowImage = resultSet.getString(3);
      boolean gtidEnabled = parseEnabledMode(resultSet.getObject(4));
      int lowerCaseTableNames = resultSet.getInt(5);
      String binlogRowMetadata = resultSet.getString(6);
      return new MySqlServerCapabilities(
          binaryLoggingEnabled,
          binlogFormat,
          binlogRowImage,
          gtidEnabled,
          lowerCaseTableNames,
          binlogRowMetadata);
    }
  }

  /**
   * Reads session-level binlog settings on the caller's connection and fails closed if the session
   * overrides {@code binlog_format} / {@code binlog_row_image} to values other than {@code ROW} /
   * {@code FULL}. This is distinct from the global preflight: the global settings govern what
   * other sessions emit into the binlog; the session settings govern what <em>this</em> connection
   * emits — specifically, DBLog's own watermark write must land as a row event, not a statement
   * event.
   */
  public void requireSessionBinlogSettings(Connection connection) throws SQLException {
    Objects.requireNonNull(connection, "connection");
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(MySqlSql.sessionBinlogSettingsSql())) {
      if (!resultSet.next()) {
        throw new IllegalStateException(
            "MySQL session binlog-settings query did not return a row");
      }
      String sessionBinlogFormat = resultSet.getString(1);
      String sessionBinlogRowImage = resultSet.getString(2);
      if (sessionBinlogFormat == null || !"ROW".equalsIgnoreCase(sessionBinlogFormat.trim())) {
        throw new IllegalStateException(
            "MySQL @@SESSION.binlog_format must be ROW for the runtime connection so DBLog's own"
                + " watermark writes land as row events (set SESSION binlog_format=ROW on this"
                + " connection, e.g. via JDBC URL sessionVariables=binlog_format=ROW), but was "
                + sessionBinlogFormat);
      }
      if (sessionBinlogRowImage == null || !"FULL".equalsIgnoreCase(sessionBinlogRowImage.trim())) {
        throw new IllegalStateException(
            "MySQL @@SESSION.binlog_row_image must be FULL for the runtime connection, but was "
                + sessionBinlogRowImage);
      }
    }
  }

  public List<TableSchema> inspectCapturedSchemas(
      Connection connection,
      String sourceId,
      String databaseName,
      List<String> configuredTables)
      throws SQLException {
    Objects.requireNonNull(connection, "connection");
    requireNonBlank(sourceId, "sourceId");
    requireNonBlank(databaseName, "databaseName");
    Objects.requireNonNull(configuredTables, "configuredTables");

    List<TableSchema> capturedSchemas = new ArrayList<>(configuredTables.size());
    for (String configuredTable : configuredTables) {
      String[] parts = parseQualifiedTable(configuredTable);
      if (!databaseName.equals(parts[0])) {
        throw new IllegalArgumentException(
            "Configured MySQL table does not belong to runtime database "
                + databaseName
                + ": "
                + configuredTable);
      }
      TableSchema schema = readTableSchema(connection, new TableId(sourceId, databaseName, parts[1]));
      if (schema == null) {
        throw new IllegalStateException(
            "MySQL table not found while reading captured-table schema: " + configuredTable);
      }
      capturedSchemas.add(schema);
    }
    ensureNoDuplicateDisplayNames(capturedSchemas);
    return List.copyOf(capturedSchemas);
  }

  public TableSchema readTableSchema(Connection connection, TableId tableId) throws SQLException {
    Objects.requireNonNull(connection, "connection");
    Objects.requireNonNull(tableId, "tableId");

    try (PreparedStatement statement = connection.prepareStatement(MySqlSql.capturedTableColumnsSql())) {
      statement.setString(1, tableId.schemaName());
      statement.setString(2, tableId.tableName());
      try (ResultSet resultSet = statement.executeQuery()) {
        List<ColumnDefinition> columns = new ArrayList<>();
        while (resultSet.next()) {
          String columnName = resultSet.getString(1);
          String dataType = resultSet.getString(2);
          String columnType = resultSet.getString(3);
          boolean nullable = "YES".equalsIgnoreCase(resultSet.getString(4));
          int primaryKeyOrdinal = resultSet.getInt(5);
          boolean primaryKey = primaryKeyOrdinal > 0;
          columns.add(
              new ColumnDefinition(
                  columnName,
                  MySqlDialect.INSTANCE.canonicalSourceType(dataType, columnType),
                  MySqlDialect.INSTANCE.neutralType(dataType, columnType),
                  primaryKey,
                  primaryKeyOrdinal,
                  nullable));
        }
        if (columns.isEmpty()) {
          return null;
        }
        return TableSchema.create(tableId, columns, Instant.now());
      }
    }
  }

  private static void ensureNoDuplicateDisplayNames(List<TableSchema> schemas) {
    LinkedHashSet<String> displayNames = new LinkedHashSet<>();
    for (TableSchema schema : schemas) {
      if (!displayNames.add(schema.tableId().displayName())) {
        throw new IllegalArgumentException(
            "MySQL configured captured tables contain a duplicate: "
                + schema.tableId().displayName());
      }
    }
  }

  private static String[] parseQualifiedTable(String configuredTable) {
    String normalized = requireNonBlank(configuredTable, "configuredTable");
    String[] parts = normalized.split("\\.", 2);
    if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
      throw new IllegalArgumentException(
          "MySQL configured tables must use adapter-native two-part names such as schema.table");
    }
    return new String[] {
      parts[0].trim().toLowerCase(Locale.ROOT),
      parts[1].trim()
    };
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value.trim();
  }

  private static boolean parseBooleanCapability(Object value) {
    if (value == null) {
      return false;
    }
    if (value instanceof Number number) {
      return number.longValue() != 0L;
    }
    String normalized = normalize(String.valueOf(value));
    return "1".equals(normalized)
        || "on".equals(normalized)
        || "yes".equals(normalized)
        || "true".equals(normalized);
  }

  private static boolean parseEnabledMode(Object value) {
    if (value == null) {
      return false;
    }
    String normalized = normalize(String.valueOf(value));
    return "on".equals(normalized)
        || "1".equals(normalized)
        || "yes".equals(normalized)
        || "true".equals(normalized);
  }

  private static String normalize(String value) {
    Objects.requireNonNull(value, "value");
    return value.trim().toLowerCase(Locale.ROOT);
  }
}
