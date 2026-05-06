package io.github.aandreakis.dblog.state.jdbc;

import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.FullDumpRequiredSignal;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.SchemaUncertaintySignal;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.state.api.SchemaStateRepository;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class JdbcSchemaStateRepository implements SchemaStateRepository {
  private static final String GLOBAL_SIGNAL_TABLE_KEY = "";

  private final JdbcStateStoreSupport jdbc;

  public JdbcSchemaStateRepository(JdbcStateStoreSupport jdbc) {
    this.jdbc = Objects.requireNonNull(jdbc, "jdbc");
  }

  @Override
  public void saveContractSchema(TableSchema schema) {
    saveSchema("TABLE_SCHEMA_CURRENT", "TABLE_SCHEMA_COLUMN", schema);
  }

  @Override
  public Optional<TableSchema> loadContractSchema(String tableDisplayName) {
    return loadSchema("TABLE_SCHEMA_CURRENT", "TABLE_SCHEMA_COLUMN", tableDisplayName);
  }

  @Override
  public List<TableSchema> loadAllContractSchemas() {
    return loadAllSchemas("TABLE_SCHEMA_CURRENT", "TABLE_SCHEMA_COLUMN");
  }

  @Override
  public void saveObservedSchema(TableSchema schema) {
    saveSchema("TABLE_SCHEMA_OBSERVED", "TABLE_SCHEMA_OBSERVED_COLUMN", schema);
  }

  @Override
  public Optional<TableSchema> loadObservedSchema(String tableDisplayName) {
    return loadSchema("TABLE_SCHEMA_OBSERVED", "TABLE_SCHEMA_OBSERVED_COLUMN", tableDisplayName);
  }

  @Override
  public List<TableSchema> loadAllObservedSchemas() {
    return loadAllSchemas("TABLE_SCHEMA_OBSERVED", "TABLE_SCHEMA_OBSERVED_COLUMN");
  }

  @Override
  public void saveFullDumpRequiredSignal(FullDumpRequiredSignal signal) {
    Objects.requireNonNull(signal, "signal");
    jdbc.withTransaction(
        connection -> {
          String tableDisplayName =
              signal.tableId() == null ? GLOBAL_SIGNAL_TABLE_KEY : signal.tableId().displayName();
          int updated =
              jdbc.update(
                  connection,
                  "UPDATE FULL_DUMP_REQUIRED_SIGNAL SET DATABASE_NAME = ?, SCHEMA_NAME_VALUE = ?, TABLE_NAME_VALUE = ?, REASON = ?, DETECTED_AT = ? WHERE SOURCE_ID = ? AND TABLE_DISPLAY_NAME = ?",
                  signal.tableId() == null ? null : signal.tableId().databaseName(),
                  signal.tableId() == null ? null : signal.tableId().schemaName(),
                  signal.tableId() == null ? null : signal.tableId().tableName(),
                  signal.reason(),
                  signal.detectedAt().toString(),
                  signal.sourceId(),
                  tableDisplayName);
          if (updated == 0) {
            jdbc.insert(
                connection,
                "INSERT INTO FULL_DUMP_REQUIRED_SIGNAL (SOURCE_ID, TABLE_DISPLAY_NAME, DATABASE_NAME, SCHEMA_NAME_VALUE, TABLE_NAME_VALUE, REASON, DETECTED_AT) VALUES (?, ?, ?, ?, ?, ?, ?)",
                signal.sourceId(),
                tableDisplayName,
                signal.tableId() == null ? null : signal.tableId().databaseName(),
                signal.tableId() == null ? null : signal.tableId().schemaName(),
                signal.tableId() == null ? null : signal.tableId().tableName(),
                signal.reason(),
                signal.detectedAt().toString());
          }
          return null;
        });
  }

  @Override
  public List<FullDumpRequiredSignal> loadFullDumpRequiredSignals() {
    return jdbc.withTransaction(
        connection ->
            jdbc.queryList(
                connection,
                "SELECT SOURCE_ID, TABLE_DISPLAY_NAME, DATABASE_NAME, SCHEMA_NAME_VALUE, TABLE_NAME_VALUE, REASON, DETECTED_AT FROM FULL_DUMP_REQUIRED_SIGNAL ORDER BY SOURCE_ID, TABLE_DISPLAY_NAME",
                resultSet -> {
                  String tableDisplayName = resultSet.getString("TABLE_DISPLAY_NAME");
                  String databaseName = resultSet.getString("DATABASE_NAME");
                  String schemaName = resultSet.getString("SCHEMA_NAME_VALUE");
                  String tableName = resultSet.getString("TABLE_NAME_VALUE");
                  TableId tableId =
                      GLOBAL_SIGNAL_TABLE_KEY.equals(tableDisplayName)
                              || databaseName == null
                              || schemaName == null
                              || tableName == null
                          ? null
                          : new TableId(databaseName, schemaName, tableName);
                  return new FullDumpRequiredSignal(
                      resultSet.getString("SOURCE_ID"),
                      tableId,
                      resultSet.getString("REASON"),
                      Instant.parse(resultSet.getString("DETECTED_AT")));
                }));
  }

  @Override
  public void saveSchemaUncertaintySignal(SchemaUncertaintySignal signal) {
    Objects.requireNonNull(signal, "signal");
    jdbc.withTransaction(
        connection -> {
          String tableDisplayName =
              signal.tableId() == null ? GLOBAL_SIGNAL_TABLE_KEY : signal.tableId().displayName();
          int updated =
              jdbc.update(
                  connection,
                  "UPDATE SCHEMA_UNCERTAINTY_SIGNAL SET DATABASE_NAME = ?, SCHEMA_NAME_VALUE = ?, TABLE_NAME_VALUE = ?, REASON = ?, FIRST_DETECTED_AT = ?, LAST_DETECTED_AT = ?, OCCURRENCE_COUNT = ? WHERE SOURCE_ID = ? AND TABLE_DISPLAY_NAME = ?",
                  signal.tableId() == null ? null : signal.tableId().databaseName(),
                  signal.tableId() == null ? null : signal.tableId().schemaName(),
                  signal.tableId() == null ? null : signal.tableId().tableName(),
                  signal.reason(),
                  signal.firstDetectedAt().toString(),
                  signal.lastDetectedAt().toString(),
                  signal.occurrenceCount(),
                  signal.sourceId(),
                  tableDisplayName);
          if (updated == 0) {
            jdbc.insert(
                connection,
                "INSERT INTO SCHEMA_UNCERTAINTY_SIGNAL (SOURCE_ID, TABLE_DISPLAY_NAME, DATABASE_NAME, SCHEMA_NAME_VALUE, TABLE_NAME_VALUE, REASON, FIRST_DETECTED_AT, LAST_DETECTED_AT, OCCURRENCE_COUNT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                signal.sourceId(),
                tableDisplayName,
                signal.tableId() == null ? null : signal.tableId().databaseName(),
                signal.tableId() == null ? null : signal.tableId().schemaName(),
                signal.tableId() == null ? null : signal.tableId().tableName(),
                signal.reason(),
                signal.firstDetectedAt().toString(),
                signal.lastDetectedAt().toString(),
                signal.occurrenceCount());
          }
          return null;
        });
  }

  @Override
  public void clearSchemaUncertaintySignal(String sourceId, String tableDisplayName) {
    Objects.requireNonNull(sourceId, "sourceId");
    Objects.requireNonNull(tableDisplayName, "tableDisplayName");
    jdbc.withTransaction(
        connection -> {
          jdbc.update(
              connection,
              "DELETE FROM SCHEMA_UNCERTAINTY_SIGNAL WHERE SOURCE_ID = ? AND TABLE_DISPLAY_NAME = ?",
              sourceId,
              tableDisplayName);
          return null;
        });
  }

  @Override
  public void deleteSchemaUncertaintySignals(String sourceId) {
    Objects.requireNonNull(sourceId, "sourceId");
    jdbc.withTransaction(
        connection -> {
          deleteSchemaUncertaintySignalsInTxn(connection, sourceId);
          return null;
        });
  }

  /**
   * In-transaction variant for composite atomic invalidations.
   */
  void deleteSchemaUncertaintySignalsInTxn(java.sql.Connection connection, String sourceId)
      throws java.sql.SQLException {
    Objects.requireNonNull(connection, "connection");
    Objects.requireNonNull(sourceId, "sourceId");
    jdbc.update(
        connection, "DELETE FROM SCHEMA_UNCERTAINTY_SIGNAL WHERE SOURCE_ID = ?", sourceId);
  }

  @Override
  public List<SchemaUncertaintySignal> loadSchemaUncertaintySignals() {
    return jdbc.withTransaction(
        connection ->
            jdbc.queryList(
                connection,
                "SELECT SOURCE_ID, TABLE_DISPLAY_NAME, DATABASE_NAME, SCHEMA_NAME_VALUE, TABLE_NAME_VALUE, REASON, FIRST_DETECTED_AT, LAST_DETECTED_AT, OCCURRENCE_COUNT FROM SCHEMA_UNCERTAINTY_SIGNAL ORDER BY SOURCE_ID, TABLE_DISPLAY_NAME",
                resultSet -> {
                  String tableDisplayName = resultSet.getString("TABLE_DISPLAY_NAME");
                  String databaseName = resultSet.getString("DATABASE_NAME");
                  String schemaName = resultSet.getString("SCHEMA_NAME_VALUE");
                  String tableName = resultSet.getString("TABLE_NAME_VALUE");
                  TableId tableId =
                      GLOBAL_SIGNAL_TABLE_KEY.equals(tableDisplayName)
                              || databaseName == null
                              || schemaName == null
                              || tableName == null
                          ? null
                          : new TableId(databaseName, schemaName, tableName);
                  return new SchemaUncertaintySignal(
                      resultSet.getString("SOURCE_ID"),
                      tableId,
                      resultSet.getString("REASON"),
                      Instant.parse(resultSet.getString("FIRST_DETECTED_AT")),
                      Instant.parse(resultSet.getString("LAST_DETECTED_AT")),
                      resultSet.getInt("OCCURRENCE_COUNT"));
                }));
  }

  private void saveSchema(String headerTable, String columnTable, TableSchema tableSchema) {
    Objects.requireNonNull(tableSchema, "tableSchema");
    jdbc.withTransaction(
        connection -> {
          int updated =
              jdbc.update(
                  connection,
                  "UPDATE "
                      + headerTable
                      + " SET DATABASE_NAME = ?, SCHEMA_NAME_VALUE = ?, TABLE_NAME_VALUE = ?, PRIMARY_KEY_COLUMN = ?, FINGERPRINT = ?, REFRESHED_AT = ? WHERE TABLE_DISPLAY_NAME = ?",
                  tableSchema.tableId().databaseName(),
                  tableSchema.tableId().schemaName(),
                  tableSchema.tableId().tableName(),
                  String.join(",", tableSchema.primaryKeyColumns()),
                  tableSchema.fingerprint(),
                  tableSchema.refreshedAt().toString(),
                  tableSchema.tableId().displayName());
          if (updated == 0) {
            jdbc.insert(
                connection,
                "INSERT INTO "
                    + headerTable
                    + " (TABLE_DISPLAY_NAME, DATABASE_NAME, SCHEMA_NAME_VALUE, TABLE_NAME_VALUE, PRIMARY_KEY_COLUMN, FINGERPRINT, REFRESHED_AT) VALUES (?, ?, ?, ?, ?, ?, ?)",
                tableSchema.tableId().displayName(),
                tableSchema.tableId().databaseName(),
                tableSchema.tableId().schemaName(),
                tableSchema.tableId().tableName(),
                String.join(",", tableSchema.primaryKeyColumns()),
                tableSchema.fingerprint(),
                tableSchema.refreshedAt().toString());
          }

          jdbc.update(
              connection,
              "DELETE FROM " + columnTable + " WHERE TABLE_DISPLAY_NAME = ?",
              tableSchema.tableId().displayName());
          for (int index = 0; index < tableSchema.columns().size(); index++) {
            ColumnDefinition column = tableSchema.columns().get(index);
            jdbc.insert(
                connection,
                "INSERT INTO "
                    + columnTable
                    + " (TABLE_DISPLAY_NAME, ORDINAL_POSITION, COLUMN_NAME, SOURCE_TYPE, NEUTRAL_TYPE, PRIMARY_KEY_FLAG, PRIMARY_KEY_ORDINAL, IGNORED_FLAG, NULLABLE_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                tableSchema.tableId().displayName(),
                index,
                column.name(),
                column.sourceType(),
                column.neutralType().name(),
                column.primaryKey() ? 1 : 0,
                column.primaryKeyOrdinal(),
                tableSchema.ignoredColumns().contains(column.name()) ? 1 : 0,
                column.nullable() ? 1 : 0);
          }
          return null;
        });
  }

  private Optional<TableSchema> loadSchema(
      String headerTable, String columnTable, String tableDisplayName) {
    Objects.requireNonNull(tableDisplayName, "tableDisplayName");
    return jdbc.withTransaction(
        connection ->
            jdbc.queryOptional(
                connection,
                "SELECT DATABASE_NAME, SCHEMA_NAME_VALUE, TABLE_NAME_VALUE, PRIMARY_KEY_COLUMN, FINGERPRINT, REFRESHED_AT FROM "
                    + headerTable
                    + " WHERE TABLE_DISPLAY_NAME = ?",
                resultSet -> {
                  TableId tableId =
                      new TableId(
                          resultSet.getString("DATABASE_NAME"),
                          resultSet.getString("SCHEMA_NAME_VALUE"),
                          resultSet.getString("TABLE_NAME_VALUE"));
                  List<ColumnDefinition> columns = loadSchemaColumns(connection, columnTable, tableDisplayName);
                  List<String> primaryKeyColumns =
                      columns.stream().filter(ColumnDefinition::primaryKey).map(ColumnDefinition::name).toList();
                  List<String> ignoredColumns = loadIgnoredColumns(connection, columnTable, tableDisplayName);
                  return new TableSchema(
                      tableId,
                      columns,
                      primaryKeyColumns,
                      resultSet.getString("FINGERPRINT"),
                      Instant.parse(resultSet.getString("REFRESHED_AT")),
                      ignoredColumns);
                },
                tableDisplayName));
  }

  private List<TableSchema> loadAllSchemas(String headerTable, String columnTable) {
    return jdbc.withTransaction(
        connection ->
            jdbc.queryList(
                connection,
                "SELECT TABLE_DISPLAY_NAME, DATABASE_NAME, SCHEMA_NAME_VALUE, TABLE_NAME_VALUE, PRIMARY_KEY_COLUMN, FINGERPRINT, REFRESHED_AT FROM "
                    + headerTable
                    + " ORDER BY TABLE_DISPLAY_NAME",
                resultSet -> {
                  String tableDisplayName = resultSet.getString("TABLE_DISPLAY_NAME");
                  TableId tableId =
                      new TableId(
                          resultSet.getString("DATABASE_NAME"),
                          resultSet.getString("SCHEMA_NAME_VALUE"),
                          resultSet.getString("TABLE_NAME_VALUE"));
                  List<ColumnDefinition> columns = loadSchemaColumns(connection, columnTable, tableDisplayName);
                  List<String> primaryKeyColumns =
                      columns.stream().filter(ColumnDefinition::primaryKey).map(ColumnDefinition::name).toList();
                  List<String> ignoredColumns = loadIgnoredColumns(connection, columnTable, tableDisplayName);
                  return new TableSchema(
                      tableId,
                      columns,
                      primaryKeyColumns,
                      resultSet.getString("FINGERPRINT"),
                      Instant.parse(resultSet.getString("REFRESHED_AT")),
                      ignoredColumns);
                }));
  }

  private List<ColumnDefinition> loadSchemaColumns(
      java.sql.Connection connection, String columnTable, String tableDisplayName)
      throws java.sql.SQLException {
    return jdbc.queryList(
        connection,
        "SELECT COLUMN_NAME, SOURCE_TYPE, NEUTRAL_TYPE, PRIMARY_KEY_FLAG, PRIMARY_KEY_ORDINAL, NULLABLE_FLAG FROM "
            + columnTable
            + " WHERE TABLE_DISPLAY_NAME = ? ORDER BY ORDINAL_POSITION",
        resultSet ->
            new ColumnDefinition(
                resultSet.getString("COLUMN_NAME"),
                resultSet.getString("SOURCE_TYPE"),
                NeutralColumnType.valueOf(resultSet.getString("NEUTRAL_TYPE")),
                resultSet.getInt("PRIMARY_KEY_FLAG") != 0,
                resultSet.getInt("PRIMARY_KEY_ORDINAL"),
                resultSet.getInt("NULLABLE_FLAG") != 0),
        tableDisplayName);
  }

  private List<String> loadIgnoredColumns(
      java.sql.Connection connection, String columnTable, String tableDisplayName)
      throws java.sql.SQLException {
    return jdbc.queryList(
        connection,
        "SELECT COLUMN_NAME FROM "
            + columnTable
            + " WHERE TABLE_DISPLAY_NAME = ? AND IGNORED_FLAG <> 0 ORDER BY ORDINAL_POSITION",
        resultSet -> resultSet.getString("COLUMN_NAME"),
        tableDisplayName);
  }
}
