package io.github.aandreakis.dblog.sink.jdbc;

import io.github.aandreakis.dblog.core.model.TableId;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/** Supported JDBC target dialects for JDBC apply sinks. */
public enum JdbcApplyTargetDialect {
  POSTGRES("org.postgresql.Driver", '"') {
    @Override
    public String upsertSql(
        TableId tableId,
        List<String> columns,
        List<String> primaryKeyColumns,
        List<String> valueExpressions) {
      String qualifiedTable = qualifiedTableName(tableId);
      String columnList = quotedColumnList(columns);
      List<String> nonPrimaryKeyColumns =
          columns.stream().filter(column -> !primaryKeyColumns.contains(column)).toList();
      String conflictTarget = quotedColumnList(primaryKeyColumns);
      if (nonPrimaryKeyColumns.isEmpty()) {
        return "INSERT INTO "
            + qualifiedTable
            + " ("
            + columnList
            + ") VALUES ("
            + joinedValueExpressions(valueExpressions)
            + ") ON CONFLICT ("
            + conflictTarget
            + ") DO NOTHING";
      }
      String assignments =
          nonPrimaryKeyColumns.stream()
              .map(column -> quoteIdentifier(column) + " = EXCLUDED." + quoteIdentifier(column))
              .reduce((left, right) -> left + ", " + right)
              .orElseThrow();
      return "INSERT INTO "
          + qualifiedTable
          + " ("
          + columnList
          + ") VALUES ("
          + joinedValueExpressions(valueExpressions)
          + ") ON CONFLICT ("
          + conflictTarget
          + ") DO UPDATE SET "
          + assignments;
    }
  },
  MYSQL("com.mysql.cj.jdbc.Driver", '`') {
    @Override
    public String upsertSql(
        TableId tableId,
        List<String> columns,
        List<String> primaryKeyColumns,
        List<String> valueExpressions) {
      return mysqlAliasUpsertSql(tableId, columns, primaryKeyColumns, valueExpressions);
    }
  };

  private final String driverClassName;
  private final char identifierQuote;

  JdbcApplyTargetDialect(String driverClassName, char identifierQuote) {
    this.driverClassName = requireNonBlank(driverClassName, "driverClassName");
    this.identifierQuote = identifierQuote;
  }

  public String driverClassName() {
    return driverClassName;
  }

  public String deleteSql(TableId tableId, List<String> primaryKeyColumns) {
    return "DELETE FROM "
        + qualifiedTableName(tableId)
        + " WHERE "
        + primaryKeyColumns.stream()
            .map(this::quoteIdentifier)
            .map(column -> column + " = ?")
            .reduce((left, right) -> left + " AND " + right)
            .orElseThrow();
  }

  public abstract String upsertSql(
      TableId tableId,
      List<String> columns,
      List<String> primaryKeyColumns,
      List<String> valueExpressions);

  public String qualifiedTableName(TableId tableId) {
    Objects.requireNonNull(tableId, "tableId");
    return quoteIdentifier(tableId.schemaName()) + "." + quoteIdentifier(tableId.tableName());
  }

  public String quoteIdentifier(String identifier) {
    String requiredIdentifier = requireNonBlank(identifier, "identifier");
    String quote = Character.toString(identifierQuote);
    return quote + requiredIdentifier.replace(quote, quote + quote) + quote;
  }

  public static JdbcApplyTargetDialect from(String raw) {
    String normalized = requireNonBlank(raw, "raw").trim().toUpperCase(Locale.ROOT);
    return switch (normalized) {
      case "POSTGRES", "POSTGRESQL" -> POSTGRES;
      case "MYSQL" -> MYSQL;
      default -> throw new IllegalArgumentException("Unsupported JDBC target dialect: " + raw);
    };
  }

  final String mysqlAliasUpsertSql(
      TableId tableId,
      List<String> columns,
      List<String> primaryKeyColumns,
      List<String> valueExpressions) {
    String rowAlias = "new_row";
    String qualifiedTable = qualifiedTableName(tableId);
    String columnList = quotedColumnList(columns);
    List<String> assignmentColumns =
        columns.stream().filter(column -> !primaryKeyColumns.contains(column)).toList();
    if (assignmentColumns.isEmpty()) {
      assignmentColumns = primaryKeyColumns;
    }
    String assignments =
        assignmentColumns.stream()
            .map(
                column ->
                    quoteIdentifier(column) + " = " + rowAlias + "." + quoteIdentifier(column))
            .reduce((left, right) -> left + ", " + right)
            .orElseThrow();
    return "INSERT INTO "
        + qualifiedTable
        + " ("
        + columnList
        + ") VALUES ("
        + joinedValueExpressions(valueExpressions)
        + ") AS "
        + rowAlias
        + " ON DUPLICATE KEY UPDATE "
        + assignments;
  }

  final String quotedColumnList(List<String> columns) {
    return columns.stream()
        .map(column -> requireNonBlank(column, "column"))
        .map(this::quoteIdentifier)
        .reduce((left, right) -> left + ", " + right)
        .orElseThrow();
  }

  private static String joinedValueExpressions(List<String> valueExpressions) {
    Objects.requireNonNull(valueExpressions, "valueExpressions");
    if (valueExpressions.isEmpty()) {
      throw new IllegalArgumentException("valueExpressions must not be empty");
    }
    return valueExpressions.stream()
        .map(expression -> requireNonBlank(expression, "valueExpression"))
        .reduce((left, right) -> left + ", " + right)
        .orElseThrow();
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
