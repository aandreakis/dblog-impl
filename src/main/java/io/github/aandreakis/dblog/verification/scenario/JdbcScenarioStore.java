package io.github.aandreakis.dblog.verification.scenario;

import io.github.aandreakis.dblog.core.model.ChangeEvent;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

/**
 * Small JDBC-backed scenario sink/telemetry store.
 *
 * <p>This intentionally avoids pooled dependencies so it can serve as a lightweight cutover-owned
 * verification store while the larger scenario shells are still being migrated.
 */
public final class JdbcScenarioStore implements ScenarioStore {
  private final String jdbcUrl;

  public JdbcScenarioStore(String driverClassName, String jdbcUrl) {
    this.jdbcUrl = Objects.requireNonNull(jdbcUrl, "jdbcUrl");
    loadDriver(Objects.requireNonNull(driverClassName, "driverClassName"));
    initializeSchema();
  }

  @Override
  public void appendEvents(String scenarioId, String stageLabel, List<ChangeEvent> events) {
    Objects.requireNonNull(events, "events");
    if (events.isEmpty()) {
      return;
    }
    String requiredScenarioId = requireNonBlank(scenarioId, "scenarioId");
    String requiredStageLabel = requireNonBlank(stageLabel, "stageLabel");
    inTransaction(
        connection -> {
          try (PreparedStatement insertEvent =
                  connection.prepareStatement(
                      "INSERT INTO SCENARIO_EVENT_LOG (SCENARIO_ID, STAGE_LABEL, TABLE_DISPLAY_NAME, OPERATION_TYPE, CAPTURE_ORIGIN, PRIMARY_KEY_LITERAL, SOURCE_POSITION, TRANSACTION_ID, DUMP_ID, BEFORE_PAYLOAD, AFTER_PAYLOAD, RECORDED_AT) "
                          + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                      Statement.RETURN_GENERATED_KEYS);
              PreparedStatement updateCurrentRow =
                  connection.prepareStatement(
                      "UPDATE SCENARIO_CURRENT_ROW SET PRESENT_FLAG = ?, PAYLOAD = ?, LAST_OPERATION_TYPE = ?, LAST_CAPTURE_ORIGIN = ?, LAST_EVENT_SEQ = ?, UPDATED_AT = ? "
                          + "WHERE SCENARIO_ID = ? AND TABLE_DISPLAY_NAME = ? AND PRIMARY_KEY_LITERAL = ?");
              PreparedStatement insertCurrentRow =
                  connection.prepareStatement(
                      "INSERT INTO SCENARIO_CURRENT_ROW (SCENARIO_ID, TABLE_DISPLAY_NAME, PRIMARY_KEY_LITERAL, PRESENT_FLAG, PAYLOAD, LAST_OPERATION_TYPE, LAST_CAPTURE_ORIGIN, LAST_EVENT_SEQ, UPDATED_AT) "
                          + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            LinkedHashMap<String, LatestRowStateUpdate> latestRowStates = new LinkedHashMap<>();
            for (ChangeEvent event : events) {
              EncodedScenarioEvent encoded = EncodedScenarioEvent.encode(event);
              long sequenceNumber =
                  insertEvent(insertEvent, requiredScenarioId, requiredStageLabel, encoded);
              latestRowStates.put(
                  encoded.tableDisplayName() + "\u0000" + encoded.primaryKeyLiteral(),
                  new LatestRowStateUpdate(sequenceNumber, encoded));
            }
            for (LatestRowStateUpdate rowStateUpdate : latestRowStates.values()) {
              upsertCurrentRow(
                  updateCurrentRow,
                  insertCurrentRow,
                  requiredScenarioId,
                  rowStateUpdate.sequenceNumber(),
                  rowStateUpdate.event());
            }
          }
        });
  }

  @Override
  public void recordTelemetry(String scenarioId, String category, String message, String detail) {
    inTransaction(
        connection ->
            insertTelemetry(
                connection,
                requireNonBlank(scenarioId, "scenarioId"),
                requireNonBlank(category, "category"),
                requireNonBlank(message, "message"),
                detail));
  }

  @Override
  public List<ScenarioEventRecord> loadEvents(String scenarioId) {
    return withConnection(
        connection -> {
          List<ScenarioEventRecord> records = new ArrayList<>();
          try (PreparedStatement statement =
              connection.prepareStatement(
                  "SELECT EVENT_SEQ, SCENARIO_ID, STAGE_LABEL, TABLE_DISPLAY_NAME, OPERATION_TYPE, CAPTURE_ORIGIN, PRIMARY_KEY_LITERAL, SOURCE_POSITION, TRANSACTION_ID, DUMP_ID, BEFORE_PAYLOAD, AFTER_PAYLOAD, RECORDED_AT "
                      + "FROM SCENARIO_EVENT_LOG WHERE SCENARIO_ID = ? ORDER BY EVENT_SEQ")) {
            statement.setString(1, requireNonBlank(scenarioId, "scenarioId"));
            try (ResultSet resultSet = statement.executeQuery()) {
              while (resultSet.next()) {
                records.add(
                    new ScenarioEventRecord(
                        resultSet.getLong("EVENT_SEQ"),
                        resultSet.getString("SCENARIO_ID"),
                        resultSet.getString("STAGE_LABEL"),
                        resultSet.getString("TABLE_DISPLAY_NAME"),
                        resultSet.getString("OPERATION_TYPE"),
                        resultSet.getString("CAPTURE_ORIGIN"),
                        resultSet.getString("PRIMARY_KEY_LITERAL"),
                        resultSet.getString("SOURCE_POSITION"),
                        resultSet.getString("TRANSACTION_ID"),
                        resultSet.getString("DUMP_ID"),
                        resultSet.getString("BEFORE_PAYLOAD"),
                        resultSet.getString("AFTER_PAYLOAD"),
                        Instant.parse(resultSet.getString("RECORDED_AT"))));
              }
            }
          }
          return List.copyOf(records);
        });
  }

  @Override
  public List<ScenarioRowState> loadCurrentRows(String scenarioId) {
    return withConnection(
        connection -> {
          List<ScenarioRowState> records = new ArrayList<>();
          try (PreparedStatement statement =
              connection.prepareStatement(
                  "SELECT SCENARIO_ID, TABLE_DISPLAY_NAME, PRIMARY_KEY_LITERAL, PRESENT_FLAG, PAYLOAD, LAST_OPERATION_TYPE, LAST_CAPTURE_ORIGIN, LAST_EVENT_SEQ, UPDATED_AT "
                      + "FROM SCENARIO_CURRENT_ROW WHERE SCENARIO_ID = ? ORDER BY TABLE_DISPLAY_NAME, PRIMARY_KEY_LITERAL")) {
            statement.setString(1, requireNonBlank(scenarioId, "scenarioId"));
            try (ResultSet resultSet = statement.executeQuery()) {
              while (resultSet.next()) {
                records.add(
                    new ScenarioRowState(
                        resultSet.getString("SCENARIO_ID"),
                        resultSet.getString("TABLE_DISPLAY_NAME"),
                        resultSet.getString("PRIMARY_KEY_LITERAL"),
                        resultSet.getInt("PRESENT_FLAG") != 0,
                        resultSet.getString("PAYLOAD"),
                        resultSet.getString("LAST_OPERATION_TYPE"),
                        resultSet.getString("LAST_CAPTURE_ORIGIN"),
                        resultSet.getLong("LAST_EVENT_SEQ"),
                        Instant.parse(resultSet.getString("UPDATED_AT"))));
              }
            }
          }
          return List.copyOf(records);
        });
  }

  @Override
  public List<ScenarioTelemetryRecord> loadTelemetry(String scenarioId) {
    return withConnection(
        connection -> {
          List<ScenarioTelemetryRecord> records = new ArrayList<>();
          try (PreparedStatement statement =
              connection.prepareStatement(
                  "SELECT TELEMETRY_SEQ, SCENARIO_ID, CATEGORY, MESSAGE, DETAIL, RECORDED_AT "
                      + "FROM SCENARIO_TELEMETRY WHERE SCENARIO_ID = ? ORDER BY TELEMETRY_SEQ")) {
            statement.setString(1, requireNonBlank(scenarioId, "scenarioId"));
            try (ResultSet resultSet = statement.executeQuery()) {
              while (resultSet.next()) {
                records.add(
                    new ScenarioTelemetryRecord(
                        resultSet.getLong("TELEMETRY_SEQ"),
                        resultSet.getString("SCENARIO_ID"),
                        resultSet.getString("CATEGORY"),
                        resultSet.getString("MESSAGE"),
                        resultSet.getString("DETAIL"),
                        Instant.parse(resultSet.getString("RECORDED_AT"))));
              }
            }
          }
          return List.copyOf(records);
        });
  }

  @Override
  public void close() {}

  private void initializeSchema() {
    inTransaction(
        connection -> {
          createTableIfMissing(
              connection,
              "SCENARIO_EVENT_LOG",
              "CREATE TABLE SCENARIO_EVENT_LOG ("
                  + "EVENT_SEQ BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, "
                  + "SCENARIO_ID VARCHAR(255) NOT NULL, "
                  + "STAGE_LABEL VARCHAR(255) NOT NULL, "
                  + "TABLE_DISPLAY_NAME VARCHAR(1024) NOT NULL, "
                  + "OPERATION_TYPE VARCHAR(64) NOT NULL, "
                  + "CAPTURE_ORIGIN VARCHAR(64) NOT NULL, "
                  + "PRIMARY_KEY_LITERAL VARCHAR(8192) NOT NULL, "
                  + "SOURCE_POSITION VARCHAR(8192), "
                  + "TRANSACTION_ID VARCHAR(255), "
                  + "DUMP_ID VARCHAR(255), "
                  + "BEFORE_PAYLOAD VARCHAR(32768), "
                  + "AFTER_PAYLOAD VARCHAR(32768), "
                  + "RECORDED_AT VARCHAR(128) NOT NULL)");
          createTableIfMissing(
              connection,
              "SCENARIO_CURRENT_ROW",
              "CREATE TABLE SCENARIO_CURRENT_ROW ("
                  + "SCENARIO_ID VARCHAR(255) NOT NULL, "
                  + "TABLE_DISPLAY_NAME VARCHAR(1024) NOT NULL, "
                  + "PRIMARY_KEY_LITERAL VARCHAR(8192) NOT NULL, "
                  + "PRESENT_FLAG INT NOT NULL, "
                  + "PAYLOAD VARCHAR(32768), "
                  + "LAST_OPERATION_TYPE VARCHAR(64) NOT NULL, "
                  + "LAST_CAPTURE_ORIGIN VARCHAR(64) NOT NULL, "
                  + "LAST_EVENT_SEQ BIGINT NOT NULL, "
                  + "UPDATED_AT VARCHAR(128) NOT NULL, "
                  + "PRIMARY KEY (SCENARIO_ID, TABLE_DISPLAY_NAME, PRIMARY_KEY_LITERAL))");
          createTableIfMissing(
              connection,
              "SCENARIO_TELEMETRY",
              "CREATE TABLE SCENARIO_TELEMETRY ("
                  + "TELEMETRY_SEQ BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, "
                  + "SCENARIO_ID VARCHAR(255) NOT NULL, "
                  + "CATEGORY VARCHAR(255) NOT NULL, "
                  + "MESSAGE VARCHAR(8192) NOT NULL, "
                  + "DETAIL VARCHAR(32768), "
                  + "RECORDED_AT VARCHAR(128) NOT NULL)");
        });
  }

  private long insertEvent(
      PreparedStatement statement,
      String scenarioId,
      String stageLabel,
      EncodedScenarioEvent event)
      throws SQLException {
    statement.setString(1, scenarioId);
    statement.setString(2, stageLabel);
    statement.setString(3, event.tableDisplayName());
    statement.setString(4, event.operationType());
    statement.setString(5, event.captureOrigin());
    statement.setString(6, event.primaryKeyLiteral());
    statement.setString(7, event.sourcePosition());
    statement.setString(8, event.transactionId());
    statement.setString(9, event.dumpId());
    statement.setString(10, event.beforePayload());
    statement.setString(11, event.afterPayload());
    statement.setString(12, event.recordedAt());
    statement.executeUpdate();
    try (ResultSet keys = statement.getGeneratedKeys()) {
      if (!keys.next()) {
        throw new IllegalStateException("Scenario event log insert did not return an identity value");
      }
      return keys.getLong(1);
    }
  }

  private void upsertCurrentRow(
      PreparedStatement updateCurrentRow,
      PreparedStatement insertCurrentRow,
      String scenarioId,
      long sequenceNumber,
      EncodedScenarioEvent event)
      throws SQLException {
    updateCurrentRow.setInt(1, event.presentFlag());
    updateCurrentRow.setString(2, event.afterPayload());
    updateCurrentRow.setString(3, event.operationType());
    updateCurrentRow.setString(4, event.captureOrigin());
    updateCurrentRow.setLong(5, sequenceNumber);
    updateCurrentRow.setString(6, event.recordedAt());
    updateCurrentRow.setString(7, scenarioId);
    updateCurrentRow.setString(8, event.tableDisplayName());
    updateCurrentRow.setString(9, event.primaryKeyLiteral());
    int updated = updateCurrentRow.executeUpdate();
    if (updated == 0) {
      insertCurrentRow.setString(1, scenarioId);
      insertCurrentRow.setString(2, event.tableDisplayName());
      insertCurrentRow.setString(3, event.primaryKeyLiteral());
      insertCurrentRow.setInt(4, event.presentFlag());
      insertCurrentRow.setString(5, event.afterPayload());
      insertCurrentRow.setString(6, event.operationType());
      insertCurrentRow.setString(7, event.captureOrigin());
      insertCurrentRow.setLong(8, sequenceNumber);
      insertCurrentRow.setString(9, event.recordedAt());
      insertCurrentRow.executeUpdate();
    }
  }

  private void insertTelemetry(
      Connection connection, String scenarioId, String category, String message, String detail)
      throws SQLException {
    insert(
        connection,
        "INSERT INTO SCENARIO_TELEMETRY (SCENARIO_ID, CATEGORY, MESSAGE, DETAIL, RECORDED_AT) VALUES (?, ?, ?, ?, ?)",
        scenarioId,
        category,
        message,
        detail,
        Instant.now().toString());
  }

  private static String primaryKeyLiteral(ChangeEvent event) {
    if (event.primaryKey().size() == 1) {
      return ScenarioPayloadCodec.encodeValue(event.primaryKey().valueAt(0));
    }
    return ScenarioPayloadCodec.encodeRow(event.primaryKey().asMap());
  }

  private record EncodedScenarioEvent(
      String tableDisplayName,
      String operationType,
      String captureOrigin,
      String primaryKeyLiteral,
      String sourcePosition,
      String transactionId,
      String dumpId,
      String beforePayload,
      String afterPayload,
      int presentFlag,
      String recordedAt) {
    static EncodedScenarioEvent encode(ChangeEvent event) {
      String afterPayload =
          event.afterRow() == null
              ? null
              : ScenarioPayloadCodec.encodeRow(event.afterRow().asMap());
      return new EncodedScenarioEvent(
          event.tableId().displayName(),
          event.operationType().name(),
          event.captureOrigin().name(),
          JdbcScenarioStore.primaryKeyLiteral(event),
          event.sourcePosition() == null ? null : event.sourcePosition().displayValue(),
          event.transactionId(),
          event.dumpId(),
          event.beforeRow() == null
              ? null
              : ScenarioPayloadCodec.encodeRow(event.beforeRow().asMap()),
          afterPayload,
          afterPayload == null ? 0 : 1,
          Instant.now().toString());
    }
  }

  private record LatestRowStateUpdate(long sequenceNumber, EncodedScenarioEvent event) {}

  private void createTableIfMissing(Connection connection, String tableName, String ddl)
      throws SQLException {
    try (ResultSet tables =
            connection.getMetaData().getTables(null, null, tableName, new String[] {"TABLE"})) {
      if (tables.next()) {
        return;
      }
    }
    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate(ddl);
    }
  }

  private void insert(Connection connection, String sql, Object... values) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      bind(statement, values);
      statement.executeUpdate();
    }
  }

  private static void bind(PreparedStatement statement, Object... values) throws SQLException {
    for (int index = 0; index < values.length; index++) {
      statement.setObject(index + 1, values[index]);
    }
  }

  private void inTransaction(SqlConsumer consumer) {
    withConnection(
        connection -> {
          boolean originalAutoCommit = connection.getAutoCommit();
          connection.setAutoCommit(false);
          try {
            consumer.accept(connection);
            connection.commit();
          } catch (SQLException | RuntimeException ex) {
            rollbackQuietly(connection);
            throw ex;
          } finally {
            connection.setAutoCommit(originalAutoCommit);
          }
          return null;
        });
  }

  private <T> T withConnection(SqlFunction<T> function) {
    try (Connection connection = DriverManager.getConnection(jdbcUrl)) {
      return function.apply(connection);
    } catch (SQLException ex) {
      throw new IllegalStateException("Scenario store JDBC operation failed", ex);
    }
  }

  private static void rollbackQuietly(Connection connection) {
    try {
      connection.rollback();
    } catch (SQLException ignored) {
      // best effort rollback only
    }
  }

  private static void loadDriver(String driverClassName) {
    try {
      Class.forName(driverClassName);
    } catch (ClassNotFoundException ex) {
      throw new IllegalStateException(
          "required JDBC driver is not on the classpath: " + driverClassName, ex);
    }
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  @FunctionalInterface
  private interface SqlFunction<T> {
    T apply(Connection connection) throws SQLException;
  }

  @FunctionalInterface
  private interface SqlConsumer {
    void accept(Connection connection) throws SQLException;
  }
}
