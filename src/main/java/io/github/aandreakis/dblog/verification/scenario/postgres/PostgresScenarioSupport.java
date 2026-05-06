package io.github.aandreakis.dblog.verification.scenario.postgres;

import io.github.aandreakis.dblog.config.ScenarioRequestMode;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.runtime.sql.RuntimeSqlSupport;
import io.github.aandreakis.dblog.verification.scenario.ScenarioExecutionException;
import io.github.aandreakis.dblog.verification.scenario.ScenarioJdbcSupport;
import io.github.aandreakis.dblog.verification.scenario.ScenarioLedgerVerifier;
import io.github.aandreakis.dblog.verification.scenario.ScenarioPayloadCodec;
import io.github.aandreakis.dblog.verification.scenario.ScenarioRowState;
import io.github.aandreakis.dblog.verification.scenario.ScenarioStore;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;

/** PostgreSQL scenario helpers extracted from the runner. */
public final class PostgresScenarioSupport {
  private PostgresScenarioSupport() {}

  public static void configureRuntimeSqlConnection(Connection connection) throws SQLException {
    RuntimeSqlSupport.configureRuntimeSqlConnection(connection);
  }

  public static String sinkJdbcUrl(Path path) {
    return ScenarioJdbcSupport.sinkJdbcUrl(path);
  }

  public static void cleanupStaleScenarioArtifacts(
      Connection connection, ScenarioStore scenarioStore, String scenarioId) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      List<String> publications = new ArrayList<>();
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT pubname FROM pg_publication WHERE pubname LIKE 'pub_%' AND pubname <> 'dblog_runtime_pub' ORDER BY pubname")) {
        while (resultSet.next()) {
          publications.add(resultSet.getString(1));
        }
      }
      for (String publication : publications) {
        statement.execute("DROP PUBLICATION IF EXISTS \"" + publication + "\"");
      }

      List<String> slots = new ArrayList<>();
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'slot_%' AND slot_name <> 'dblog_runtime_slot' AND NOT active ORDER BY slot_name")) {
        while (resultSet.next()) {
          slots.add(resultSet.getString(1));
        }
      }
      for (String slot : slots) {
        statement.execute("SELECT pg_drop_replication_slot('" + slot + "')");
      }

      scenarioStore.recordTelemetry(
          scenarioId,
          "runner",
          "cleanup-stale-scenario-artifacts",
          "droppedPublications=" + publications.size() + " droppedSlots=" + slots.size());
    }
  }

  public static void resetScenarioSource(Connection connection, PostgresScenarioSchema scenarioSchema)
      throws SQLException {
    String widgetNameType = "TEXT";
    try (Statement statement = connection.createStatement()) {
      statement.execute("DROP SCHEMA IF EXISTS \"" + scenarioSchema.schemaName() + "\" CASCADE");
      statement.execute("DROP SCHEMA IF EXISTS dblog_meta CASCADE");
      statement.execute(
          "DROP PUBLICATION IF EXISTS \"" + scenarioSchema.publicationName() + "\"");
      statement.execute(
          "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '"
              + scenarioSchema.slotName()
              + "' AND NOT active");
      statement.execute("CREATE SCHEMA \"" + scenarioSchema.schemaName() + "\"");
      statement.execute(
          "CREATE TABLE \""
              + scenarioSchema.schemaName()
              + "\".\"widgets\" (id BIGINT PRIMARY KEY, name "
              + widgetNameType
              + ", enabled BOOLEAN, payload BYTEA)");
      statement.execute(
          "CREATE TABLE \""
              + scenarioSchema.schemaName()
              + "\".\"gadgets\" (id BIGINT PRIMARY KEY, name TEXT)");
      statement.execute(
          "ALTER TABLE \"" + scenarioSchema.schemaName() + "\".\"widgets\" REPLICA IDENTITY FULL");
      statement.execute(
          "ALTER TABLE \"" + scenarioSchema.schemaName() + "\".\"gadgets\" REPLICA IDENTITY FULL");
    }
  }

  public static void seedInitialRows(
      Connection connection,
      ScenarioStore scenarioStore,
      String scenarioId,
      PostgresScenarioSchema scenarioSchema)
      throws SQLException {
    for (long id = 1; id <= 5; id++) {
      insertWidget(
          connection,
          scenarioSchema,
          id,
          "widget-" + id,
          id % 2 == 0,
          hexBytes(String.format("%04X", id)));
      scenarioStore.recordTelemetry(
          scenarioId,
          "source-seed",
          "seed-widget",
          "table=" + scenarioSchema.widgets().tableId().displayName() + " pk=" + id);
      insertGadget(connection, scenarioSchema, id, "gadget-" + id);
      scenarioStore.recordTelemetry(
          scenarioId,
          "source-seed",
          "seed-gadget",
          "table=" + scenarioSchema.gadgets().tableId().displayName() + " pk=" + id);
    }
  }

  public static void runBaselineMutations(
      Connection connection,
      ScenarioStore scenarioStore,
      String scenarioId,
      PostgresScenarioSchema scenarioSchema)
      throws Exception {
    executeMutation(
        connection,
        scenarioStore,
        scenarioId,
        "baseline-update-widget-1",
        "UPDATE",
        scenarioSchema.widgets(),
        "1",
        () ->
            updateWidget(
                connection,
                scenarioSchema,
                1L,
                "widget-one-live",
                true,
                hexBytes("AA01")));
    executeMutation(
        connection,
        scenarioStore,
        scenarioId,
        "baseline-insert-gadget-11",
        "INSERT",
        scenarioSchema.gadgets(),
        "11",
        () -> insertGadget(connection, scenarioSchema, 11L, "gadget-11"));
    executeMutation(
        connection,
        scenarioStore,
        scenarioId,
        "baseline-delete-gadget-2",
        "DELETE",
        scenarioSchema.gadgets(),
        "2",
        () -> deleteGadget(connection, scenarioSchema, 2L));
  }

  public static void runDeterministicMutationSequence(
      Connection connection,
      ScenarioStore scenarioStore,
      String scenarioId,
      PostgresScenarioSchema scenarioSchema,
      int mutationCount,
      Duration mutationPause)
      throws Exception {
    runDeterministicMutationSequence(
        connection,
        scenarioStore,
        scenarioId,
        scenarioSchema,
        mutationCount,
        mutationPause,
        1);
  }

  public static void runDeterministicMutationSequence(
      Connection connection,
      ScenarioStore scenarioStore,
      String scenarioId,
      PostgresScenarioSchema scenarioSchema,
      int mutationCount,
      Duration mutationPause,
      int mutationBatchSize)
      throws Exception {
    ScenarioTableState widgetState = ScenarioTableState.seededWidgets();
    ScenarioTableState gadgetState = ScenarioTableState.seededGadgets();
    long nextWidgetId = 100L;
    long nextGadgetId = 200L;
    try (PreparedMutationStatements statements =
        PreparedMutationStatements.open(connection, scenarioSchema)) {
      for (int batchStart = 1; batchStart <= mutationCount; batchStart += mutationBatchSize) {
        final int currentBatchStart = batchStart;
        final int currentBatchEnd = Math.min(mutationCount, batchStart + mutationBatchSize - 1);
        MutationCursor startCursor = new MutationCursor(nextWidgetId, nextGadgetId);
        MutationCursor endCursor =
            runMutationBatch(
                connection,
                () -> {
                  MutationCursor cursor = startCursor;
                  for (int index = currentBatchStart; index <= currentBatchEnd; index++) {
                    cursor =
                        applyDeterministicMutation(
                            connection,
                            scenarioStore,
                            scenarioId,
                            scenarioSchema,
                            statements,
                            widgetState,
                            gadgetState,
                            cursor.nextWidgetId(),
                            cursor.nextGadgetId(),
                            index);
                  }
                  return cursor;
                });
        nextWidgetId = endCursor.nextWidgetId();
        nextGadgetId = endCursor.nextGadgetId();
        PostgresScenarioFaultThreads.sleepQuietly(mutationPause);
      }
    }
  }

  private static MutationCursor applyDeterministicMutation(
      Connection connection,
      ScenarioStore scenarioStore,
      String scenarioId,
      PostgresScenarioSchema scenarioSchema,
      PreparedMutationStatements statements,
      ScenarioTableState widgetState,
      ScenarioTableState gadgetState,
      long nextWidgetId,
      long nextGadgetId,
      int mutationIndex)
      throws Exception {
    int mode = mutationIndex % 6;
    if (mode == 0 || widgetState.ids().isEmpty()) {
      long widgetId = nextWidgetId++;
      widgetState.add(widgetId);
      executeMutation(
          connection,
          scenarioStore,
          scenarioId,
          "mutation-" + mutationIndex + "-insert-widget",
          "INSERT",
          scenarioSchema.widgets(),
          Long.toString(widgetId),
          () ->
              statements.insertWidget(
                  widgetId,
                  "widget-" + widgetId,
                  mutationIndex % 2 == 0,
                  payloadBytes(mutationIndex)));
      return new MutationCursor(nextWidgetId, nextGadgetId);
    }
    if (mode == 1) {
      long widgetId = widgetState.hotId(mutationIndex);
      executeMutation(
          connection,
          scenarioStore,
          scenarioId,
          "mutation-" + mutationIndex + "-update-widget",
          "UPDATE",
          scenarioSchema.widgets(),
          Long.toString(widgetId),
          () ->
              statements.updateWidget(
                  widgetId,
                  "widget-live-" + mutationIndex,
                  mutationIndex % 2 != 0,
                  payloadBytes(mutationIndex + 4096)));
      return new MutationCursor(nextWidgetId, nextGadgetId);
    }
    if (mode == 2 || gadgetState.ids().isEmpty()) {
      long gadgetId = nextGadgetId++;
      gadgetState.add(gadgetId);
      executeMutation(
          connection,
          scenarioStore,
          scenarioId,
          "mutation-" + mutationIndex + "-insert-gadget",
          "INSERT",
          scenarioSchema.gadgets(),
          Long.toString(gadgetId),
          () -> statements.insertGadget(gadgetId, "gadget-" + mutationIndex));
      return new MutationCursor(nextWidgetId, nextGadgetId);
    }
    if (mode == 3) {
      long gadgetId = gadgetState.hotId(mutationIndex);
      executeMutation(
          connection,
          scenarioStore,
          scenarioId,
          "mutation-" + mutationIndex + "-update-gadget",
          "UPDATE",
          scenarioSchema.gadgets(),
          Long.toString(gadgetId),
          () -> statements.updateGadget(gadgetId, "gadget-live-" + mutationIndex));
      return new MutationCursor(nextWidgetId, nextGadgetId);
    }
    if (mode == 4 && widgetState.ids().size() > 2) {
      long widgetId = widgetState.removeHotId(mutationIndex);
      executeMutation(
          connection,
          scenarioStore,
          scenarioId,
          "mutation-" + mutationIndex + "-delete-widget",
          "DELETE",
          scenarioSchema.widgets(),
          Long.toString(widgetId),
          () -> statements.deleteWidget(widgetId));
      return new MutationCursor(nextWidgetId, nextGadgetId);
    }
    long gadgetId = gadgetState.removeHotId(mutationIndex);
    executeMutation(
        connection,
        scenarioStore,
        scenarioId,
        "mutation-" + mutationIndex + "-delete-gadget",
        "DELETE",
        scenarioSchema.gadgets(),
        Long.toString(gadgetId),
        () -> statements.deleteGadget(gadgetId));
    return new MutationCursor(nextWidgetId, nextGadgetId);
  }

  private static <T> T runMutationBatch(Connection connection, SqlSupplier<T> supplier)
      throws Exception {
    boolean originalAutoCommit = connection.getAutoCommit();
    connection.setAutoCommit(false);
    try {
      T result = supplier.get();
      connection.commit();
      return result;
    } catch (Throwable failure) {
      try {
        connection.rollback();
      } catch (SQLException rollbackFailure) {
        failure.addSuppressed(rollbackFailure);
      }
      throw failure;
    } finally {
      connection.setAutoCommit(originalAutoCommit);
    }
  }

  public static Map<String, String> loadSourceSnapshot(
      Connection connection, PostgresScenarioSchema scenarioSchema) throws SQLException {
    return ScenarioJdbcSupport.loadSourceSnapshot(
        connection,
        scenarioSchema.capturedSchemas(),
        scenarioSchema.widgets().hasSinglePrimaryKey()
            ? scenarioSchema.widgets().primaryKeyColumn()
            : null);
  }

  public static Map<String, String> loadSinkSnapshot(
      ScenarioStore scenarioStore, PostgresScenarioSchema scenarioSchema) {
    return ScenarioJdbcSupport.loadSinkSnapshot(
        scenarioStore, scenarioSchema.widgets().tableId().databaseName(), scenarioSchema.capturedTableNames());
  }

  public static void verifyFinalStateMatchesSource(
      Connection verificationConnection,
      ScenarioStore scenarioStore,
      String scenarioId,
      ScenarioRequestMode requestMode,
      PostgresScenarioSchema scenarioSchema)
      throws SQLException {
    Set<String> capturedTableNames = scenarioSchema.capturedTableNames();
    Map<String, String> expected = loadSourceSnapshot(verificationConnection, scenarioSchema);
    if (requestMode != ScenarioRequestMode.ALL_TABLES) {
      Set<String> trackedKeys =
          ScenarioJdbcSupport.loadTrackedSinkKeys(scenarioStore, scenarioId, capturedTableNames);
      Map<String, String> filtered = new LinkedHashMap<>();
      for (Map.Entry<String, String> entry : expected.entrySet()) {
        if (trackedKeys.contains(entry.getKey())) {
          filtered.put(entry.getKey(), entry.getValue());
        }
      }
      expected = Map.copyOf(filtered);
    }
    Map<String, String> actual =
        ScenarioJdbcSupport.loadSinkSnapshot(scenarioStore, scenarioId, capturedTableNames);
    if (!expected.equals(actual)) {
      throw new ScenarioExecutionException(
          "Scenario sink final state did not match PostgreSQL source state. expected="
              + expected
              + " actual="
              + actual);
    }
  }

  public static void logScenarioSummary(
      Logger log, ScenarioStore scenarioStore, String scenarioId) {
    long eventCount = scenarioStore.loadEvents(scenarioId).size();
    long currentRows =
        scenarioStore.loadCurrentRows(scenarioId).stream().filter(ScenarioRowState::present).count();
    long telemetryCount = scenarioStore.loadTelemetry(scenarioId).size();
    log.info(
        "Scenario {} summary: events={}, currentRows={}, telemetryRecords={}",
        scenarioId,
        eventCount,
        currentRows,
        telemetryCount);
  }

  private static void executeMutation(
      Connection connection,
      ScenarioStore scenarioStore,
      String scenarioId,
      String label,
      String operation,
      TableSchema schema,
      String primaryKeyLiteral,
      SqlRunnable runnable)
      throws Exception {
    String beforePayload =
        ScenarioJdbcSupport.loadSourceRowPayload(connection, schema, primaryKeyLiteral, "\"");
    runnable.run();
    String afterPayload =
        ScenarioJdbcSupport.loadSourceRowPayload(connection, schema, primaryKeyLiteral, "\"");
    scenarioStore.recordTelemetry(
        scenarioId,
        "source-mutation",
        label,
        ScenarioLedgerVerifier.encodeSourceMutationDetail(
            operation,
            schema.tableId().displayName(),
            encodeScenarioPrimaryKey(schema, primaryKeyLiteral),
            beforePayload,
            afterPayload));
  }

  private static String encodeScenarioPrimaryKey(TableSchema schema, String primaryKeyLiteral) {
    if (schema.hasSinglePrimaryKey()) {
      return ScenarioPayloadCodec.encodeValue(
          ScenarioJdbcSupport.scenarioPrimaryKeyValue(schema, primaryKeyLiteral));
    }
    return ScenarioPayloadCodec.encodeRow(schema.primaryKeyRowFromLiteral(primaryKeyLiteral));
  }

  private static void insertWidget(
      Connection connection,
      PostgresScenarioSchema scenarioSchema,
      long id,
      String name,
      boolean enabled,
      byte[] payload)
      throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(
            "INSERT INTO \""
                + scenarioSchema.schemaName()
                + "\".\"widgets\" (id, name, enabled, payload) VALUES (?, ?, ?, ?)")) {
      statement.setLong(1, id);
      statement.setString(2, name);
      statement.setBoolean(3, enabled);
      statement.setBytes(4, payload);
      statement.executeUpdate();
    }
  }

  private static void updateWidget(
      Connection connection,
      PostgresScenarioSchema scenarioSchema,
      long id,
      String name,
      boolean enabled,
      byte[] payload)
      throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(
            "UPDATE \""
                + scenarioSchema.schemaName()
                + "\".\"widgets\" SET name = ?, enabled = ?, payload = ? WHERE id = ?")) {
      statement.setString(1, name);
      statement.setBoolean(2, enabled);
      statement.setBytes(3, payload);
      statement.setLong(4, id);
      statement.executeUpdate();
    }
  }

  private static void deleteWidget(
      Connection connection, PostgresScenarioSchema scenarioSchema, long id) throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(
            "DELETE FROM \"" + scenarioSchema.schemaName() + "\".\"widgets\" WHERE id = ?")) {
      statement.setLong(1, id);
      statement.executeUpdate();
    }
  }

  private static void insertGadget(
      Connection connection, PostgresScenarioSchema scenarioSchema, long id, String name)
      throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(
            "INSERT INTO \""
                + scenarioSchema.schemaName()
                + "\".\"gadgets\" (id, name) VALUES (?, ?)")) {
      statement.setLong(1, id);
      statement.setString(2, name);
      statement.executeUpdate();
    }
  }

  private static void updateGadget(
      Connection connection, PostgresScenarioSchema scenarioSchema, long id, String name)
      throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(
            "UPDATE \"" + scenarioSchema.schemaName() + "\".\"gadgets\" SET name = ? WHERE id = ?")) {
      statement.setString(1, name);
      statement.setLong(2, id);
      statement.executeUpdate();
    }
  }

  private static void deleteGadget(
      Connection connection, PostgresScenarioSchema scenarioSchema, long id) throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(
            "DELETE FROM \"" + scenarioSchema.schemaName() + "\".\"gadgets\" WHERE id = ?")) {
      statement.setLong(1, id);
      statement.executeUpdate();
    }
  }

  private static byte[] hexBytes(String value) {
    return java.util.HexFormat.of().parseHex(value);
  }

  private static byte[] payloadBytes(int value) {
    return new byte[] {(byte) ((value >>> 8) & 0xFF), (byte) (value & 0xFF)};
  }

  private static final class ScenarioTableState {
    private final List<Long> ids;

    private ScenarioTableState(List<Long> ids) {
      this.ids = ids;
    }

    private static ScenarioTableState seededWidgets() {
      return new ScenarioTableState(new ArrayList<>(List.of(1L, 2L, 3L, 4L, 5L)));
    }

    private static ScenarioTableState seededGadgets() {
      return new ScenarioTableState(new ArrayList<>(List.of(1L, 2L, 3L, 4L, 5L)));
    }

    private List<Long> ids() {
      return ids;
    }

    private void add(long id) {
      ids.add(id);
    }

    private long hotId(int index) {
      return ids.get(index % ids.size());
    }

    private long removeHotId(int index) {
      return ids.remove(index % ids.size());
    }
  }

  private record MutationCursor(long nextWidgetId, long nextGadgetId) {}

  private static final class PreparedMutationStatements implements AutoCloseable {
    private final PreparedStatement insertWidget;
    private final PreparedStatement updateWidget;
    private final PreparedStatement deleteWidget;
    private final PreparedStatement insertGadget;
    private final PreparedStatement updateGadget;
    private final PreparedStatement deleteGadget;

    private PreparedMutationStatements(
        PreparedStatement insertWidget,
        PreparedStatement updateWidget,
        PreparedStatement deleteWidget,
        PreparedStatement insertGadget,
        PreparedStatement updateGadget,
        PreparedStatement deleteGadget) {
      this.insertWidget = insertWidget;
      this.updateWidget = updateWidget;
      this.deleteWidget = deleteWidget;
      this.insertGadget = insertGadget;
      this.updateGadget = updateGadget;
      this.deleteGadget = deleteGadget;
    }

    static PreparedMutationStatements open(Connection connection, PostgresScenarioSchema scenarioSchema)
        throws SQLException {
      String schemaName = scenarioSchema.schemaName();
      PreparedStatement insertWidget = null;
      PreparedStatement updateWidget = null;
      PreparedStatement deleteWidget = null;
      PreparedStatement insertGadget = null;
      PreparedStatement updateGadget = null;
      PreparedStatement deleteGadget = null;
      try {
        insertWidget =
            connection.prepareStatement(
                "INSERT INTO \""
                    + schemaName
                    + "\".\"widgets\" (id, name, enabled, payload) VALUES (?, ?, ?, ?)");
        updateWidget =
            connection.prepareStatement(
                "UPDATE \""
                    + schemaName
                    + "\".\"widgets\" SET name = ?, enabled = ?, payload = ? WHERE id = ?");
        deleteWidget =
            connection.prepareStatement(
                "DELETE FROM \"" + schemaName + "\".\"widgets\" WHERE id = ?");
        insertGadget =
            connection.prepareStatement(
                "INSERT INTO \"" + schemaName + "\".\"gadgets\" (id, name) VALUES (?, ?)");
        updateGadget =
            connection.prepareStatement(
                "UPDATE \"" + schemaName + "\".\"gadgets\" SET name = ? WHERE id = ?");
        deleteGadget =
            connection.prepareStatement(
                "DELETE FROM \"" + schemaName + "\".\"gadgets\" WHERE id = ?");
        return new PreparedMutationStatements(
            insertWidget, updateWidget, deleteWidget, insertGadget, updateGadget, deleteGadget);
      } catch (SQLException failure) {
        failure = closeStatement(deleteGadget, failure);
        failure = closeStatement(updateGadget, failure);
        failure = closeStatement(insertGadget, failure);
        failure = closeStatement(deleteWidget, failure);
        failure = closeStatement(updateWidget, failure);
        failure = closeStatement(insertWidget, failure);
        throw failure;
      }
    }

    void insertWidget(long id, String name, boolean enabled, byte[] payload) throws SQLException {
      insertWidget.setLong(1, id);
      insertWidget.setString(2, name);
      insertWidget.setBoolean(3, enabled);
      insertWidget.setBytes(4, payload);
      insertWidget.executeUpdate();
    }

    void updateWidget(long id, String name, boolean enabled, byte[] payload) throws SQLException {
      updateWidget.setString(1, name);
      updateWidget.setBoolean(2, enabled);
      updateWidget.setBytes(3, payload);
      updateWidget.setLong(4, id);
      updateWidget.executeUpdate();
    }

    void deleteWidget(long id) throws SQLException {
      deleteWidget.setLong(1, id);
      deleteWidget.executeUpdate();
    }

    void insertGadget(long id, String name) throws SQLException {
      insertGadget.setLong(1, id);
      insertGadget.setString(2, name);
      insertGadget.executeUpdate();
    }

    void updateGadget(long id, String name) throws SQLException {
      updateGadget.setString(1, name);
      updateGadget.setLong(2, id);
      updateGadget.executeUpdate();
    }

    void deleteGadget(long id) throws SQLException {
      deleteGadget.setLong(1, id);
      deleteGadget.executeUpdate();
    }

    @Override
    public void close() throws SQLException {
      SQLException failure = null;
      failure = closeStatement(insertWidget, failure);
      failure = closeStatement(updateWidget, failure);
      failure = closeStatement(deleteWidget, failure);
      failure = closeStatement(insertGadget, failure);
      failure = closeStatement(updateGadget, failure);
      failure = closeStatement(deleteGadget, failure);
      if (failure != null) {
        throw failure;
      }
    }

    private static SQLException closeStatement(PreparedStatement statement, SQLException failure) {
      if (statement == null) {
        return failure;
      }
      try {
        statement.close();
      } catch (SQLException ex) {
        if (failure == null) {
          return ex;
        }
        failure.addSuppressed(ex);
      }
      return failure;
    }
  }

  @FunctionalInterface
  private interface SqlRunnable {
    void run() throws Exception;
  }

  @FunctionalInterface
  private interface SqlSupplier<T> {
    T get() throws Exception;
  }
}
