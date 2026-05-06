package io.github.aandreakis.dblog.verification.scenario;

import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralValueNormalizer;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyValue;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.runtime.sql.RuntimeSqlSupport;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Adapter-neutral JDBC helpers shared by scenario verification shells. */
public final class ScenarioJdbcSupport {
  private ScenarioJdbcSupport() {}

  public static String sinkJdbcUrl(Path path) {
    Objects.requireNonNull(path, "path");
    return "jdbc:h2:file:"
        + path.toAbsolutePath().normalize().toString().replace('\\', '/')
        + ";DB_CLOSE_ON_EXIT=FALSE";
  }

  public static void configureRuntimeSqlConnection(Connection connection) throws SQLException {
    RuntimeSqlSupport.configureRuntimeSqlConnection(connection);
  }

  public static void configureRuntimeSqlConnection(Connection connection, Duration networkTimeout)
      throws SQLException {
    RuntimeSqlSupport.configureRuntimeSqlConnection(connection, networkTimeout);
  }

  public static void sleepQuietly(Duration duration) {
    try {
      Thread.sleep(duration.toMillis());
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while sleeping in scenario harness", ex);
    }
  }

  public static void joinQuietly(Thread thread) {
    if (thread == null) {
      return;
    }
    try {
      thread.join();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for scenario helper thread", ex);
    }
  }

  public static Map<String, String> loadSinkSnapshot(ScenarioStore scenarioStore, String scenarioId) {
    return loadSinkSnapshot(scenarioStore, scenarioId, null);
  }

  public static Map<String, String> loadSinkSnapshot(
      ScenarioStore scenarioStore, String scenarioId, Set<String> capturedTables) {
    Map<String, String> snapshot = new LinkedHashMap<>();
    for (ScenarioRowState rowState : scenarioStore.loadCurrentRows(scenarioId)) {
      if (!rowState.present()) {
        continue;
      }
      if (capturedTables != null && !capturedTables.contains(rowState.tableDisplayName())) {
        continue;
      }
      snapshot.put(rowState.tableDisplayName() + "|" + rowState.primaryKeyLiteral(), rowState.payload());
    }
    return Map.copyOf(snapshot);
  }

  public static Set<String> loadTrackedSinkKeys(
      ScenarioStore scenarioStore, String scenarioId, Set<String> capturedTables) {
    Set<String> trackedKeys = new LinkedHashSet<>();
    for (ScenarioRowState rowState : scenarioStore.loadCurrentRows(scenarioId)) {
      if (capturedTables != null && !capturedTables.contains(rowState.tableDisplayName())) {
        continue;
      }
      trackedKeys.add(rowState.tableDisplayName() + "|" + rowState.primaryKeyLiteral());
    }
    return Set.copyOf(trackedKeys);
  }

  public static Map<String, String> loadSourceSnapshot(
      Connection connection, List<TableSchema> schemas, String idColumn) throws SQLException {
    Map<String, String> snapshot = new LinkedHashMap<>();
    for (TableSchema schema : schemas) {
      readSourceRows(connection, schema, idColumn, snapshot);
    }
    return Map.copyOf(snapshot);
  }

  public static void readSourceRows(
      Connection connection, TableSchema schema, String idColumn, Map<String, String> snapshot)
      throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "SELECT * FROM "
                    + schema.tableId().schemaName()
                    + "."
                    + schema.tableId().tableName()
                    + " ORDER BY "
                    + orderByColumns(schema, idColumn))) {
      while (resultSet.next()) {
        Map<String, Object> payload = new LinkedHashMap<>();
        for (int columnIndex = 0; columnIndex < schema.selectedColumns().size(); columnIndex++) {
          ColumnDefinition column = schema.selectedColumns().get(columnIndex);
          payload.put(
              column.name(),
              NeutralValueNormalizer.normalize(column, resultSet.getObject(columnIndex + 1)));
        }
        String primaryKey = scenarioPrimaryKeyLiteral(schema, payload);
        snapshot.put(
            schema.tableId().displayName() + "|" + primaryKey,
            ScenarioPayloadCodec.encodeRow(payload));
      }
    }
  }

  public static String loadSourceRowPayload(
      Connection connection,
      TableSchema schema,
      String primaryKeyLiteral,
      String identifierQuote)
      throws SQLException {
    Objects.requireNonNull(connection, "connection");
    Objects.requireNonNull(schema, "schema");
    Objects.requireNonNull(primaryKeyLiteral, "primaryKeyLiteral");
    Objects.requireNonNull(identifierQuote, "identifierQuote");

    StringBuilder select = new StringBuilder();
    for (int index = 0; index < schema.selectedColumns().size(); index++) {
      if (index > 0) {
        select.append(", ");
      }
      select.append(quoteIdentifier(schema.selectedColumns().get(index).name(), identifierQuote));
    }

    String sql =
        "SELECT "
            + select
            + " FROM "
            + quoteIdentifier(schema.tableId().schemaName(), identifierQuote)
            + "."
            + quoteIdentifier(schema.tableId().tableName(), identifierQuote)
            + " WHERE "
            + primaryKeyWhereClause(schema, identifierQuote);

    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      bindPrimaryKeyLiteral(statement, 1, schema, primaryKeyLiteral);
      try (ResultSet resultSet = statement.executeQuery()) {
        if (!resultSet.next()) {
          return null;
        }
        LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
        for (int index = 0; index < schema.selectedColumns().size(); index++) {
          ColumnDefinition column = schema.selectedColumns().get(index);
          payload.put(
              column.name(),
              NeutralValueNormalizer.normalize(column, resultSet.getObject(index + 1)));
        }
        if (resultSet.next()) {
          throw new IllegalStateException(
              "Scenario source query returned multiple rows for single primary key "
                  + schema.tableId().displayName()
                  + " pk="
                  + primaryKeyLiteral);
        }
        return ScenarioPayloadCodec.encodeRow(payload);
      }
    }
  }

  public static Object scenarioPrimaryKeyValue(TableSchema schema, String primaryKeyLiteral) {
    Objects.requireNonNull(schema, "schema");
    Objects.requireNonNull(primaryKeyLiteral, "primaryKeyLiteral");
    if (!schema.hasSinglePrimaryKey()) {
      return schema.primaryKeyRowFromLiteral(primaryKeyLiteral);
    }
    PrimaryKeyValue primaryKeyValue = schema.primaryKeyValueFromLiteral(primaryKeyLiteral);
    Object normalizedValue = primaryKeyValue.normalizedValue();
    if (normalizedValue instanceof BigInteger integerValue && integerValue.bitLength() <= 63) {
      return integerValue.longValueExact();
    }
    return normalizedValue;
  }

  public static String scenarioPrimaryKeyLiteral(
      TableSchema schema, Map<String, Object> rowPayload) {
    Objects.requireNonNull(schema, "schema");
    Objects.requireNonNull(rowPayload, "rowPayload");
    if (schema.hasSinglePrimaryKey()) {
      String primaryKeyLiteral = schema.primaryKeyLiteralFor(rowPayload);
      return ScenarioPayloadCodec.encodeValue(scenarioPrimaryKeyValue(schema, primaryKeyLiteral));
    }
    return ScenarioPayloadCodec.encodeRow(schema.primaryKeyRow(rowPayload));
  }

  public static String quoteIdentifier(String identifier, String identifierQuote) {
    Objects.requireNonNull(identifier, "identifier");
    Objects.requireNonNull(identifierQuote, "identifierQuote");
    return identifierQuote
        + identifier.replace(identifierQuote, identifierQuote + identifierQuote)
        + identifierQuote;
  }

  private static void bindPrimaryKeyLiteral(
      PreparedStatement statement, int index, TableSchema schema, String primaryKeyLiteral)
      throws SQLException {
    PrimaryKeyTuple primaryKeyTuple = schema.primaryKeyTupleFromLiteral(primaryKeyLiteral);
    for (int componentIndex = 0;
        componentIndex < schema.primaryKeyDefinitions().size();
        componentIndex++) {
      bindPrimaryKeyComponent(
          statement,
          index + componentIndex,
          schema.primaryKeyDefinitions().get(componentIndex),
          primaryKeyTuple.values().get(componentIndex).normalizedValue());
    }
  }

  private static void bindPrimaryKeyComponent(
      PreparedStatement statement,
      int index,
      ColumnDefinition primaryKeyColumn,
      Object normalizedValue)
      throws SQLException {
    if (normalizedValue instanceof BigInteger integerValue) {
      if (integerValue.bitLength() <= 63) {
        statement.setLong(index, integerValue.longValueExact());
      } else {
        statement.setBigDecimal(index, new BigDecimal(integerValue));
      }
      return;
    }
    if (normalizedValue instanceof BigDecimal decimalValue) {
      statement.setBigDecimal(index, decimalValue);
      return;
    }
    if (normalizedValue instanceof byte[] bytes) {
      statement.setBytes(index, bytes);
      return;
    }
    if (normalizedValue instanceof LocalDate localDate) {
      statement.setObject(index, localDate);
      return;
    }
    if (normalizedValue instanceof LocalTime localTime) {
      statement.setObject(index, localTime);
      return;
    }
    if (normalizedValue instanceof LocalDateTime localDateTime) {
      statement.setObject(index, localDateTime);
      return;
    }
    if (normalizedValue instanceof Instant instant) {
      statement.setObject(index, OffsetDateTime.ofInstant(instant, ZoneOffset.UTC));
      return;
    }
    statement.setObject(index, normalizedValue);
  }

  private static String orderByColumns(TableSchema schema, String fallbackColumn) {
    if (schema.hasSinglePrimaryKey()) {
      return fallbackColumn == null ? schema.primaryKeyColumn() : fallbackColumn;
    }
    return String.join(", ", schema.primaryKeyColumns());
  }

  private static String primaryKeyWhereClause(TableSchema schema, String identifierQuote) {
    return schema.primaryKeyColumns().stream()
        .map(column -> quoteIdentifier(column, identifierQuote) + " = ?")
        .reduce((left, right) -> left + " AND " + right)
        .orElseThrow();
  }
}
