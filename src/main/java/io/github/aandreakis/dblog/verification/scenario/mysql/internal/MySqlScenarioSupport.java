package io.github.aandreakis.dblog.verification.scenario.mysql.internal;

import io.github.aandreakis.dblog.config.ScenarioRequestMode;
import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.core.request.DumpRequestState;
import io.github.aandreakis.dblog.core.request.DumpRequestStatus;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import io.github.aandreakis.dblog.verification.scenario.ScenarioExecutionException;
import io.github.aandreakis.dblog.verification.scenario.ScenarioJdbcSupport;
import io.github.aandreakis.dblog.verification.scenario.ScenarioLedgerVerifier;
import io.github.aandreakis.dblog.verification.scenario.ScenarioPayloadCodec;
import io.github.aandreakis.dblog.verification.scenario.ScenarioRowState;
import io.github.aandreakis.dblog.verification.scenario.ScenarioStore;
import io.github.aandreakis.dblog.verification.scenario.ScenarioTelemetry;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;

/** Shared scenario-harness helpers for MySQL. */
public final class MySqlScenarioSupport {
  private MySqlScenarioSupport() {}

  public static boolean shouldSkipStartupDrain(
      boolean resetSource, RuntimeStateStore stateStore, List<TableSchema> capturedSchemas) {
    if (resetSource) {
      return false;
    }
    List<DumpRequest> pendingRequests = stateStore.dumpRequests().loadPending();
    if (pendingRequests.isEmpty()) {
      return false;
    }
    for (DumpRequest request : pendingRequests) {
      if (hasActiveRecoveryState(stateStore, request, capturedSchemas)) {
        return true;
      }
    }
    return false;
  }

  public static void verifyMutationCountCoveredByLogEvents(
      ScenarioStore scenarioStore, String scenarioId, Set<String> capturedTables) {
    long sourceMutations =
        scenarioStore.loadTelemetry(scenarioId).stream()
            .filter(record -> record.category().equals("source-mutation"))
            .count();
    long logEvents =
        scenarioStore.loadEvents(scenarioId).stream()
            .filter(record -> capturedTables.contains(record.tableDisplayName()))
            .filter(record -> record.captureOrigin().equals(CaptureOrigin.LOG.name()))
            .filter(
                record ->
                    !record.operationType().equals(OperationType.WATERMARK.name())
                        && !record.operationType().equals(OperationType.HEARTBEAT.name()))
            .count();
    if (logEvents < sourceMutations) {
      throw new ScenarioExecutionException(
          "Scenario emitted fewer captured LOG events than committed source mutations. mutations="
              + sourceMutations
              + " logEvents="
              + logEvents);
    }
  }

  public static void resetScenarioSource(
      Connection connection, MySqlScenarioSchema scenarioSchema) throws SQLException {
    resetScenarioSource(connection, scenarioSchema.databaseName());
  }

  public static void resetScenarioSource(Connection connection, String databaseName)
      throws SQLException {
    String widgetNameType = "VARCHAR(255)";
    String widgetsTable = qualifiedTableName(databaseName, "widgets");
    String gadgetsTable = qualifiedTableName(databaseName, "gadgets");
    try (Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE IF NOT EXISTS dblog_meta");
      statement.execute("DROP TABLE IF EXISTS " + widgetsTable);
      statement.execute("DROP TABLE IF EXISTS " + gadgetsTable);
      statement.execute("DROP TABLE IF EXISTS `dblog_meta`.`watermarks`");
      statement.execute("DROP TABLE IF EXISTS `dblog_meta`.`heartbeats`");
      statement.execute(
          "CREATE TABLE "
              + widgetsTable
              + " (id BIGINT PRIMARY KEY, name "
              + widgetNameType
              + ", enabled BOOLEAN, payload VARBINARY(255))");
      statement.execute(
          "CREATE TABLE " + gadgetsTable + " (id BIGINT PRIMARY KEY, name VARCHAR(255))");
      statement.execute(
          "CREATE TABLE `dblog_meta`.`watermarks` (id BIGINT PRIMARY KEY, run_id VARCHAR(255) NULL, token VARCHAR(512) NULL, CHECK (id = 1), CHECK (run_id IS NULL OR TRIM(run_id) <> ''), CHECK (token IS NULL OR TRIM(token) <> ''))");
      statement.execute(
          "INSERT INTO `dblog_meta`.`watermarks` (id, run_id, token) VALUES (1, NULL, NULL)");
      statement.execute(
          "CREATE TABLE `dblog_meta`.`heartbeats` (id BIGINT PRIMARY KEY, run_id VARCHAR(255) NULL, source_stream_id VARCHAR(255) NULL, last_beat_at TIMESTAMP NULL, CHECK (id = 1), CHECK (run_id IS NULL OR TRIM(run_id) <> ''), CHECK (source_stream_id IS NULL OR TRIM(source_stream_id) <> ''))");
      statement.execute(
          "INSERT INTO `dblog_meta`.`heartbeats` (id, run_id, source_stream_id, last_beat_at) VALUES (1, NULL, NULL, NULL)");
    }
  }

  public static void seedInitialRows(
      Connection connection,
      ScenarioStore scenarioStore,
      String scenarioId,
      MySqlScenarioSchema scenarioSchema)
      throws SQLException {
    seedInitialRows(
        connection,
        scenarioStore,
        scenarioId,
        scenarioSchema.databaseName(),
        scenarioSchema.widgets(),
        scenarioSchema.gadgets());
  }

  public static void seedInitialRows(
      Connection connection,
      ScenarioStore scenarioStore,
      String scenarioId,
      String databaseName,
      TableSchema widgets,
      TableSchema gadgets)
      throws SQLException {
    for (long id = 1; id <= 5; id++) {
      insertWidget(
          connection, databaseName, id, "widget-" + id, id % 2 == 0, hexBytes(String.format("%04X", id)));
      ScenarioTelemetry.record(
          scenarioStore, scenarioId, "source-seed", "seed-widget", "table=" + widgets.tableId().displayName() + " pk=" + id);
      insertGadget(connection, databaseName, id, "gadget-" + id);
      ScenarioTelemetry.record(
          scenarioStore, scenarioId, "source-seed", "seed-gadget", "table=" + gadgets.tableId().displayName() + " pk=" + id);
    }
  }

  public static void runBaselineMutations(
      Connection connection,
      ScenarioStore scenarioStore,
      String scenarioId,
      MySqlScenarioSchema scenarioSchema)
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
      MySqlScenarioSchema scenarioSchema,
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
      MySqlScenarioSchema scenarioSchema,
      int mutationCount,
      Duration mutationPause,
      int mutationBatchSize)
      throws Exception {
    ScenarioTableState widgetState = ScenarioTableState.seededWidgets();
    ScenarioTableState gadgetState = ScenarioTableState.seededGadgets();
    long nextWidgetId = 100L;
    long nextGadgetId = 200L;
    try (PreparedMutationStatements statements =
        PreparedMutationStatements.open(connection, scenarioSchema.databaseName())) {
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
        ScenarioJdbcSupport.sleepQuietly(mutationPause);
      }
    }
  }

  private static MutationCursor applyDeterministicMutation(
      Connection connection,
      ScenarioStore scenarioStore,
      String scenarioId,
      MySqlScenarioSchema scenarioSchema,
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

  public static void verifyFinalStateMatchesSource(
      Connection verificationConnection,
      ScenarioStore scenarioStore,
      String scenarioId,
      ScenarioRequestMode requestMode,
      MySqlScenarioSchema scenarioSchema)
      throws SQLException {
    Set<String> capturedTableNames = scenarioSchema.capturedTableNames();
    Map<String, String> expected =
        ScenarioJdbcSupport.loadSourceSnapshot(
            verificationConnection,
            scenarioSchema.capturedSchemas(),
            scenarioSchema.widgets().hasSinglePrimaryKey()
                ? scenarioSchema.widgets().primaryKeyColumn()
                : null);
    if (requestMode != ScenarioRequestMode.ALL_TABLES) {
      Set<String> trackedKeys =
          ScenarioJdbcSupport.loadTrackedSinkKeys(
              scenarioStore, scenarioId, capturedTableNames);
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
          "Scenario sink final state did not match source state. expected="
              + expected
              + " actual="
              + actual);
    }
  }

  public static void logScenarioSummary(
      Logger log, String adapterLabel, ScenarioStore scenarioStore, String scenarioId) {
    long eventCount = scenarioStore.loadEvents(scenarioId).size();
    long currentRows =
        scenarioStore.loadCurrentRows(scenarioId).stream().filter(ScenarioRowState::present).count();
    long telemetryCount = scenarioStore.loadTelemetry(scenarioId).size();
    log.info(
        "{} scenario {} summary: events={}, currentRows={}, telemetryRecords={}",
        adapterLabel,
        scenarioId,
        eventCount,
        currentRows,
        telemetryCount);
  }

  public static void executeMutation(
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
        ScenarioJdbcSupport.loadSourceRowPayload(connection, schema, primaryKeyLiteral, "`");
    runnable.run();
    String afterPayload =
        ScenarioJdbcSupport.loadSourceRowPayload(connection, schema, primaryKeyLiteral, "`");
    ScenarioTelemetry.record(
        scenarioStore,
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

  public static void insertWidget(
      Connection connection,
      MySqlScenarioSchema scenarioSchema,
      long id,
      String name,
      boolean enabled,
      byte[] payload)
      throws SQLException {
    insertWidget(connection, scenarioSchema.databaseName(), id, name, enabled, payload);
  }

  public static void insertWidget(
      Connection connection,
      String databaseName,
      long id,
      String name,
      boolean enabled,
      byte[] payload)
      throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(
            "INSERT INTO "
                + qualifiedTableName(databaseName, "widgets")
                + " (id, name, enabled, payload) VALUES (?, ?, ?, ?)")) {
      statement.setLong(1, id);
      statement.setString(2, name);
      statement.setBoolean(3, enabled);
      statement.setBytes(4, payload);
      statement.executeUpdate();
    }
  }

  public static void updateWidget(
      Connection connection,
      MySqlScenarioSchema scenarioSchema,
      long id,
      String name,
      boolean enabled,
      byte[] payload)
      throws SQLException {
    updateWidget(connection, scenarioSchema.databaseName(), id, name, enabled, payload);
  }

  public static void updateWidget(
      Connection connection,
      String databaseName,
      long id,
      String name,
      boolean enabled,
      byte[] payload)
      throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(
            "UPDATE "
                + qualifiedTableName(databaseName, "widgets")
                + " SET name = ?, enabled = ?, payload = ? WHERE id = ?")) {
      statement.setString(1, name);
      statement.setBoolean(2, enabled);
      statement.setBytes(3, payload);
      statement.setLong(4, id);
      statement.executeUpdate();
    }
  }

  public static void deleteWidget(
      Connection connection, MySqlScenarioSchema scenarioSchema, long id)
      throws SQLException {
    deleteWidget(connection, scenarioSchema.databaseName(), id);
  }

  public static void deleteWidget(Connection connection, String databaseName, long id)
      throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(
            "DELETE FROM " + qualifiedTableName(databaseName, "widgets") + " WHERE id = ?")) {
      statement.setLong(1, id);
      statement.executeUpdate();
    }
  }

  public static void insertGadget(
      Connection connection, MySqlScenarioSchema scenarioSchema, long id, String name)
      throws SQLException {
    insertGadget(connection, scenarioSchema.databaseName(), id, name);
  }

  public static void insertGadget(
      Connection connection, String databaseName, long id, String name) throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(
            "INSERT INTO "
                + qualifiedTableName(databaseName, "gadgets")
                + " (id, name) VALUES (?, ?)")) {
      statement.setLong(1, id);
      statement.setString(2, name);
      statement.executeUpdate();
    }
  }

  public static void updateGadget(
      Connection connection, MySqlScenarioSchema scenarioSchema, long id, String name)
      throws SQLException {
    updateGadget(connection, scenarioSchema.databaseName(), id, name);
  }

  public static void updateGadget(
      Connection connection, String databaseName, long id, String name) throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(
            "UPDATE "
                + qualifiedTableName(databaseName, "gadgets")
                + " SET name = ? WHERE id = ?")) {
      statement.setString(1, name);
      statement.setLong(2, id);
      statement.executeUpdate();
    }
  }

  public static void deleteGadget(
      Connection connection, MySqlScenarioSchema scenarioSchema, long id)
      throws SQLException {
    deleteGadget(connection, scenarioSchema.databaseName(), id);
  }

  public static void deleteGadget(Connection connection, String databaseName, long id)
      throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(
            "DELETE FROM " + qualifiedTableName(databaseName, "gadgets") + " WHERE id = ?")) {
      statement.setLong(1, id);
      statement.executeUpdate();
    }
  }

  public static byte[] hexBytes(String value) {
    return java.util.HexFormat.of().parseHex(value);
  }

  public static byte[] payloadBytes(int value) {
    return new byte[] {(byte) ((value >>> 8) & 0xFF), (byte) (value & 0xFF)};
  }

  private static boolean hasActiveRecoveryState(
      RuntimeStateStore stateStore, DumpRequest request, List<TableSchema> capturedSchemas) {
    Optional<DumpRequestStatus> status = stateStore.dumpRequests().loadStatus(request.requestId());
    if (request.scope() == DumpScope.PRIMARY_KEYS) {
      return false;
    }
    for (TableSchema schema : schemasForRequest(request, capturedSchemas)) {
      if (stateStore
          .dumpProgress()
          .load(request.requestId(), schema.tableId().displayName())
          .filter(progress -> progress.hasActiveChunk())
          .isPresent()) {
        return true;
      }
    }
    return false;
  }

  private static List<TableSchema> schemasForRequest(
      DumpRequest request, List<TableSchema> capturedSchemas) {
    if (request.scope() == DumpScope.TABLE) {
      for (TableSchema schema : capturedSchemas) {
        if (schema.tableId().displayName().equals(request.tableId().displayName())) {
          return List.of(schema);
        }
      }
      return List.of();
    }
    return capturedSchemas;
  }

  public static final class ScenarioTableState {
    private final List<Long> ids;

    private ScenarioTableState(List<Long> ids) {
      this.ids = ids;
    }

    public static ScenarioTableState seededWidgets() {
      return new ScenarioTableState(new ArrayList<>(List.of(1L, 2L, 3L, 4L, 5L)));
    }

    public static ScenarioTableState seededGadgets() {
      return new ScenarioTableState(new ArrayList<>(List.of(1L, 2L, 3L, 4L, 5L)));
    }

    public List<Long> ids() {
      return ids;
    }

    public void add(long id) {
      ids.add(id);
    }

    public long hotId(int index) {
      return ids.get(index % ids.size());
    }

    public long removeHotId(int index) {
      return ids.remove(index % ids.size());
    }
  }

  @FunctionalInterface
  public interface SqlRunnable {
    void run() throws Exception;
  }

  @FunctionalInterface
  private interface SqlSupplier<T> {
    T get() throws Exception;
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

    static PreparedMutationStatements open(Connection connection, String databaseName)
        throws SQLException {
      String widgetsTable = qualifiedTableName(databaseName, "widgets");
      String gadgetsTable = qualifiedTableName(databaseName, "gadgets");
      PreparedStatement insertWidget = null;
      PreparedStatement updateWidget = null;
      PreparedStatement deleteWidget = null;
      PreparedStatement insertGadget = null;
      PreparedStatement updateGadget = null;
      PreparedStatement deleteGadget = null;
      try {
        insertWidget =
            connection.prepareStatement(
                "INSERT INTO " + widgetsTable + " (id, name, enabled, payload) VALUES (?, ?, ?, ?)");
        updateWidget =
            connection.prepareStatement(
                "UPDATE " + widgetsTable + " SET name = ?, enabled = ?, payload = ? WHERE id = ?");
        deleteWidget =
            connection.prepareStatement("DELETE FROM " + widgetsTable + " WHERE id = ?");
        insertGadget =
            connection.prepareStatement(
                "INSERT INTO " + gadgetsTable + " (id, name) VALUES (?, ?)");
        updateGadget =
            connection.prepareStatement("UPDATE " + gadgetsTable + " SET name = ? WHERE id = ?");
        deleteGadget =
            connection.prepareStatement("DELETE FROM " + gadgetsTable + " WHERE id = ?");
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

  private static String qualifiedTableName(String databaseName, String tableName) {
    return quoteIdentifier(databaseName) + "." + quoteIdentifier(tableName);
  }

  private static String quoteIdentifier(String identifier) {
    if (identifier == null) {
      throw new NullPointerException("identifier");
    }
    if (identifier.isBlank()) {
      throw new IllegalArgumentException("identifier must not be blank");
    }
    return "`" + identifier.replace("`", "``") + "`";
  }
}
