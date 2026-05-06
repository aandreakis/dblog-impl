package io.github.aandreakis.dblog.state.jdbc;

import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.core.request.DumpRequestState;
import io.github.aandreakis.dblog.core.request.DumpRequestStatus;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTupleCodec;
import io.github.aandreakis.dblog.state.api.DumpRequestNotFoundException;
import io.github.aandreakis.dblog.state.api.DumpRequestRepository;
import io.github.aandreakis.dblog.state.api.StoredDumpRequest;
import io.github.aandreakis.dblog.state.api.StoredDumpRequestDetail;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class JdbcDumpRequestRepository implements DumpRequestRepository {
  private final JdbcStateStoreSupport jdbc;

  public JdbcDumpRequestRepository(JdbcStateStoreSupport jdbc) {
    this.jdbc = Objects.requireNonNull(jdbc, "jdbc");
  }

  @Override
  public void upsert(DumpRequest request) {
    Objects.requireNonNull(request, "request");
    jdbc.withTransaction(
        connection -> {
          String now = Instant.now().toString();
          int updated =
              jdbc.update(
                  connection,
                  "UPDATE DUMP_REQUEST SET SCOPE_VALUE = ?, DATABASE_NAME = ?, SCHEMA_NAME_VALUE = ?, TABLE_NAME_VALUE = ?, UPDATED_AT = ? WHERE REQUEST_ID = ?",
                  request.scope().name(),
                  request.tableId() == null ? null : request.tableId().databaseName(),
                  request.tableId() == null ? null : request.tableId().schemaName(),
                  request.tableId() == null ? null : request.tableId().tableName(),
                  now,
                  request.requestId());
          if (updated == 0) {
            insertDumpRequestRow(connection, request, now, now);
          }
          jdbc.update(connection, "DELETE FROM DUMP_REQUEST_STATUS WHERE REQUEST_ID = ?", request.requestId());
          jdbc.update(connection, "DELETE FROM DUMP_REQUEST_MISSING_KEY WHERE REQUEST_ID = ?", request.requestId());
          jdbc.update(connection, "DELETE FROM DUMP_REQUEST_KEY WHERE REQUEST_ID = ?", request.requestId());
          insertDumpRequestKeys(connection, request);
          return null;
        });
  }

  @Override
  public boolean createIfAbsent(DumpRequest request) {
    Objects.requireNonNull(request, "request");
    try {
      jdbc.withTransaction(
          connection -> {
            String now = Instant.now().toString();
            insertDumpRequestRow(connection, request, now, now);
            insertDumpRequestKeys(connection, request);
            return null;
          });
      return true;
    } catch (IllegalStateException e) {
      if (isDuplicateRequestIdViolation(e)) {
        return false;
      }
      throw e;
    }
  }

  @Override
  public DumpRequest createGenerated(
      DumpScope scope, TableId tableId, List<PrimaryKeyTuple> primaryKeyTuples) {
    Objects.requireNonNull(scope, "scope");
    List<PrimaryKeyTuple> normalizedPrimaryKeyTuples =
        primaryKeyTuples == null ? List.of() : List.copyOf(primaryKeyTuples);
    new DumpRequest("1", scope, tableId, normalizedPrimaryKeyTuples);
    return jdbc.withTransaction(
        connection -> {
          String now = Instant.now().toString();
          String requestId = insertGeneratedDumpRequestRow(connection, scope, tableId, now, now);
          DumpRequest request = new DumpRequest(requestId, scope, tableId, normalizedPrimaryKeyTuples);
          insertDumpRequestKeys(connection, request);
          return request;
        });
  }

  @Override
  public List<DumpRequest> loadPending() {
    return jdbc.withTransaction(
        connection ->
            assembleDumpRequests(
                connection,
                jdbc.queryList(
                    connection,
                    "SELECT r.REQUEST_ID, r.SCOPE_VALUE, r.DATABASE_NAME, r.SCHEMA_NAME_VALUE, r.TABLE_NAME_VALUE "
                        + "FROM DUMP_REQUEST r "
                        + "LEFT JOIN DUMP_REQUEST_STATUS s ON s.REQUEST_ID = r.REQUEST_ID "
                        + "WHERE s.STATE_VALUE IS NULL OR s.STATE_VALUE = 'ACTIVE' "
                        + "ORDER BY r.REQUEST_SEQUENCE",
                    this::requestRow)));
  }

  @Override
  public List<DumpRequest> loadAll() {
    return jdbc.withTransaction(
        connection ->
            assembleDumpRequests(
                connection,
                jdbc.queryList(
                    connection,
                    "SELECT REQUEST_ID, SCOPE_VALUE, DATABASE_NAME, SCHEMA_NAME_VALUE, TABLE_NAME_VALUE "
                        + "FROM DUMP_REQUEST ORDER BY REQUEST_SEQUENCE",
                    this::requestRow)));
  }

  @Override
  public List<StoredDumpRequest> loadAllDetailed() {
    return jdbc.withTransaction(
        connection ->
            jdbc.queryList(
                connection,
                "SELECT r.REQUEST_ID, r.SCOPE_VALUE, r.DATABASE_NAME, r.SCHEMA_NAME_VALUE, r.TABLE_NAME_VALUE, r.CREATED_AT, COALESCE(s.UPDATED_AT, r.UPDATED_AT) AS EFFECTIVE_UPDATED_AT "
                    + "FROM DUMP_REQUEST r "
                    + "LEFT JOIN DUMP_REQUEST_STATUS s ON s.REQUEST_ID = r.REQUEST_ID "
                    + "ORDER BY r.REQUEST_SEQUENCE",
                resultSet -> storedDumpRequest(connection, resultSet)));
  }

  @Override
  public List<StoredDumpRequestDetail> loadAllDetailedWithStatus() {
    return jdbc.withTransaction(
        connection ->
            materializeStoredDumpRequestDetails(
                connection,
                jdbc.queryList(
                    connection,
                    "SELECT r.REQUEST_ID, r.SCOPE_VALUE, r.DATABASE_NAME, r.SCHEMA_NAME_VALUE, r.TABLE_NAME_VALUE, r.CREATED_AT, COALESCE(s.UPDATED_AT, r.UPDATED_AT) AS EFFECTIVE_UPDATED_AT, s.STATE_VALUE, s.ACTIVE_LOW_WATERMARK, s.ACTIVE_HIGH_WATERMARK, s.FAILURE_REASON "
                        + "FROM DUMP_REQUEST r "
                        + "LEFT JOIN DUMP_REQUEST_STATUS s ON s.REQUEST_ID = r.REQUEST_ID "
                        + "ORDER BY r.REQUEST_SEQUENCE",
                    this::detailedRequestRow)));
  }

  @Override
  public int countPending() {
    return jdbc.withTransaction(
        connection ->
            jdbc.queryOptional(
                    connection,
                    "SELECT COUNT(*) "
                        + "FROM DUMP_REQUEST r "
                        + "LEFT JOIN DUMP_REQUEST_STATUS s ON s.REQUEST_ID = r.REQUEST_ID "
                        + "WHERE s.STATE_VALUE IS NULL OR s.STATE_VALUE = 'ACTIVE'",
                    resultSet -> resultSet.getInt(1))
                .orElse(0));
  }

  @Override
  public Optional<DumpRequest> loadRequest(String requestId) {
    Objects.requireNonNull(requestId, "requestId");
    return jdbc.withTransaction(connection -> loadDumpRequest(connection, requestId));
  }

  @Override
  public Optional<StoredDumpRequest> loadRequestDetailed(String requestId) {
    Objects.requireNonNull(requestId, "requestId");
    return jdbc.withTransaction(
        connection ->
            jdbc.queryOptional(
                connection,
                "SELECT r.REQUEST_ID, r.SCOPE_VALUE, r.DATABASE_NAME, r.SCHEMA_NAME_VALUE, r.TABLE_NAME_VALUE, r.CREATED_AT, COALESCE(s.UPDATED_AT, r.UPDATED_AT) AS EFFECTIVE_UPDATED_AT "
                    + "FROM DUMP_REQUEST r "
                    + "LEFT JOIN DUMP_REQUEST_STATUS s ON s.REQUEST_ID = r.REQUEST_ID "
                    + "WHERE r.REQUEST_ID = ?",
                resultSet -> storedDumpRequest(connection, resultSet),
                requestId));
  }

  @Override
  public Optional<StoredDumpRequestDetail> loadRequestDetailedWithStatus(String requestId) {
    Objects.requireNonNull(requestId, "requestId");
    return jdbc.withTransaction(
        connection -> {
          Optional<DetailedRequestRow> maybeRow =
              jdbc.queryOptional(
                  connection,
                  "SELECT r.REQUEST_ID, r.SCOPE_VALUE, r.DATABASE_NAME, r.SCHEMA_NAME_VALUE, r.TABLE_NAME_VALUE, r.CREATED_AT, COALESCE(s.UPDATED_AT, r.UPDATED_AT) AS EFFECTIVE_UPDATED_AT, s.STATE_VALUE, s.ACTIVE_LOW_WATERMARK, s.ACTIVE_HIGH_WATERMARK, s.FAILURE_REASON "
                      + "FROM DUMP_REQUEST r "
                      + "LEFT JOIN DUMP_REQUEST_STATUS s ON s.REQUEST_ID = r.REQUEST_ID "
                      + "WHERE r.REQUEST_ID = ?",
                  this::detailedRequestRow,
                  requestId);
          if (maybeRow.isEmpty()) {
            return Optional.empty();
          }
          return Optional.of(
              materializeStoredDumpRequestDetails(connection, List.of(maybeRow.orElseThrow())).get(0));
        });
  }

  @Override
  public void saveStatus(DumpRequestStatus status) {
    Objects.requireNonNull(status, "status");
    jdbc.withTransaction(
        connection -> {
          saveDumpRequestStatusInternal(connection, status);
          return null;
        });
  }

  @Override
  public boolean saveStatusIfAbsentOrActive(DumpRequestStatus status) {
    return saveStatusIfCurrentStateIn(status, true, DumpRequestState.ACTIVE);
  }

  @Override
  public boolean saveStatusIfCurrentStateIn(
      DumpRequestStatus status, boolean allowMissing, DumpRequestState... allowedCurrentStates) {
    Objects.requireNonNull(status, "status");
    Objects.requireNonNull(allowedCurrentStates, "allowedCurrentStates");
    return jdbc.withTransaction(
        connection ->
            saveDumpRequestStatusIfCurrentStateInInternal(
                connection, status, allowMissing, allowedCurrentStates));
  }

  @Override
  public Optional<DumpRequestStatus> loadStatus(String requestId) {
    Objects.requireNonNull(requestId, "requestId");
    return jdbc.withTransaction(connection -> loadDumpRequestStatusInternal(connection, requestId));
  }

  @Override
  public void failNonTerminalRequests(String reason) {
    Objects.requireNonNull(reason, "reason");
    jdbc.withTransaction(
        connection -> {
          failNonTerminalRequestsInTxn(connection, reason);
          return null;
        });
  }

  /**
   * In-transaction variant for composite atomic invalidations.
   */
  void failNonTerminalRequestsInTxn(Connection connection, String reason) throws SQLException {
    Objects.requireNonNull(connection, "connection");
    Objects.requireNonNull(reason, "reason");
    List<DumpRequest> requests =
        assembleDumpRequests(
            connection,
            jdbc.queryList(
                connection,
                "SELECT r.REQUEST_ID, r.SCOPE_VALUE, r.DATABASE_NAME, r.SCHEMA_NAME_VALUE, r.TABLE_NAME_VALUE "
                    + "FROM DUMP_REQUEST r "
                    + "LEFT JOIN DUMP_REQUEST_STATUS s ON s.REQUEST_ID = r.REQUEST_ID "
                    + "WHERE s.STATE_VALUE IS NULL OR s.STATE_VALUE NOT IN ('COMPLETED', 'FAILED')",
                this::requestRow));
    for (DumpRequest request : requests) {
      saveDumpRequestStatusInternal(connection, DumpRequestStatus.failed(request, reason));
    }
  }

  @Override
  public void pruneSupersededTerminalRequests(DumpRequest justCompletedRequest) {
    Objects.requireNonNull(justCompletedRequest, "justCompletedRequest");
    jdbc.withTransaction(
        connection -> {
          Long sequence = loadRequestSequence(connection, justCompletedRequest.requestId());
          if (sequence == null) {
            // Just-completed request was already pruned (e.g., concurrent operator delete).
            // Nothing to supersede.
            return null;
          }
          List<String> supersededIds =
              switch (justCompletedRequest.scope()) {
                case ALL_TABLES -> jdbc.queryList(
                    connection,
                    "SELECT r.REQUEST_ID FROM DUMP_REQUEST r "
                        + "JOIN DUMP_REQUEST_STATUS s ON s.REQUEST_ID = r.REQUEST_ID "
                        + "WHERE r.SCOPE_VALUE = 'ALL_TABLES' "
                        + "AND s.STATE_VALUE IN ('COMPLETED', 'FAILED') "
                        + "AND r.REQUEST_SEQUENCE < ?",
                    rs -> rs.getString(1),
                    sequence);
                case TABLE, PRIMARY_KEYS -> {
                  TableId tableId = justCompletedRequest.tableId();
                  if (tableId == null) {
                    // TABLE/PRIMARY_KEYS without a tableId is not a valid coordinator input;
                    // fail closed by skipping rather than pruning anything.
                    yield List.of();
                  }
                  yield jdbc.queryList(
                      connection,
                      "SELECT r.REQUEST_ID FROM DUMP_REQUEST r "
                          + "JOIN DUMP_REQUEST_STATUS s ON s.REQUEST_ID = r.REQUEST_ID "
                          + "WHERE r.SCOPE_VALUE IN ('TABLE', 'PRIMARY_KEYS') "
                          + "AND r.DATABASE_NAME = ? "
                          + "AND r.SCHEMA_NAME_VALUE = ? "
                          + "AND r.TABLE_NAME_VALUE = ? "
                          + "AND s.STATE_VALUE IN ('COMPLETED', 'FAILED') "
                          + "AND r.REQUEST_SEQUENCE < ?",
                      rs -> rs.getString(1),
                      tableId.databaseName(),
                      tableId.schemaName(),
                      tableId.tableName(),
                      sequence);
                }
              };
          for (String supersededId : supersededIds) {
            deleteRequestLineageInTxn(connection, supersededId);
          }
          return null;
        });
  }

  private Long loadRequestSequence(Connection connection, String requestId) throws SQLException {
    return jdbc.queryOptional(
            connection,
            "SELECT REQUEST_SEQUENCE FROM DUMP_REQUEST WHERE REQUEST_ID = ?",
            rs -> rs.getLong(1),
            requestId)
        .orElse(null);
  }

  /**
   * Delete every row tied to {@code requestId} across the request-lineage tables.
   *
   * <p>Order matters: delete child tables first, then {@code DUMP_REQUEST_STATUS}, then the parent
   * {@code DUMP_REQUEST} row. The {@code loadPending} {@code LEFT JOIN} on {@code DUMP_REQUEST}
   * means any half-deleted state where the request row outlives its status row would resurrect the
   * request as pending.
   */
  private void deleteRequestLineageInTxn(Connection connection, String requestId)
      throws SQLException {
    jdbc.update(
        connection, "DELETE FROM DUMP_TABLE_PROGRESS WHERE JOB_ID = ?", requestId);
    jdbc.update(
        connection, "DELETE FROM DUMP_REQUEST_KEY WHERE REQUEST_ID = ?", requestId);
    jdbc.update(
        connection, "DELETE FROM DUMP_REQUEST_MISSING_KEY WHERE REQUEST_ID = ?", requestId);
    jdbc.update(
        connection, "DELETE FROM DUMP_REQUEST_STATUS WHERE REQUEST_ID = ?", requestId);
    jdbc.update(connection, "DELETE FROM DUMP_REQUEST WHERE REQUEST_ID = ?", requestId);
  }

  private void insertDumpRequestRow(
      Connection connection, DumpRequest request, String createdAt, String updatedAt)
      throws SQLException {
    jdbc.insert(
        connection,
        "INSERT INTO DUMP_REQUEST (REQUEST_ID, SCOPE_VALUE, DATABASE_NAME, SCHEMA_NAME_VALUE, TABLE_NAME_VALUE, CREATED_AT, UPDATED_AT) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?)",
        request.requestId(),
        request.scope().name(),
        request.tableId() == null ? null : request.tableId().databaseName(),
        request.tableId() == null ? null : request.tableId().schemaName(),
        request.tableId() == null ? null : request.tableId().tableName(),
        createdAt,
        updatedAt);
  }

  private String insertGeneratedDumpRequestRow(
      Connection connection, DumpScope scope, TableId tableId, String createdAt, String updatedAt)
      throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(
            "INSERT INTO DUMP_REQUEST (REQUEST_ID, SCOPE_VALUE, DATABASE_NAME, SCHEMA_NAME_VALUE, TABLE_NAME_VALUE, CREATED_AT, UPDATED_AT) "
                + "VALUES (NULL, ?, ?, ?, ?, ?, ?)",
            Statement.RETURN_GENERATED_KEYS)) {
      statement.setString(1, scope.name());
      statement.setObject(2, tableId == null ? null : tableId.databaseName());
      statement.setObject(3, tableId == null ? null : tableId.schemaName());
      statement.setObject(4, tableId == null ? null : tableId.tableName());
      statement.setString(5, createdAt);
      statement.setString(6, updatedAt);
      statement.executeUpdate();
      try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
        if (!generatedKeys.next()) {
          throw new IllegalStateException(
              "dump request insert did not return a generated requestId");
        }
        long requestSequence = generatedKeys.getLong(1);
        String requestId = Long.toString(requestSequence);
        jdbc.update(
            connection,
            "UPDATE DUMP_REQUEST SET REQUEST_ID = ? WHERE REQUEST_SEQUENCE = ?",
            requestId,
            requestSequence);
        return requestId;
      }
    }
  }

  private void insertDumpRequestKeys(Connection connection, DumpRequest request) throws SQLException {
    for (int index = 0; index < request.primaryKeyTuples().size(); index++) {
      jdbc.insert(
          connection,
          "INSERT INTO DUMP_REQUEST_KEY (REQUEST_ID, ORDINAL_POSITION, PRIMARY_KEY_LITERAL) VALUES (?, ?, ?)",
          request.requestId(),
          index,
          PrimaryKeyTupleCodec.encode(request.primaryKeyTuples().get(index)));
    }
  }

  private List<PrimaryKeyTuple> loadDumpRequestKeys(Connection connection, String requestId)
      throws SQLException {
    return jdbc.queryList(
        connection,
        "SELECT PRIMARY_KEY_LITERAL FROM DUMP_REQUEST_KEY WHERE REQUEST_ID = ? ORDER BY ORDINAL_POSITION",
        resultSet -> PrimaryKeyTupleCodec.decode(resultSet.getString(1)),
        requestId);
  }

  private List<PrimaryKeyTuple> loadDumpRequestMissingKeys(Connection connection, String requestId)
      throws SQLException {
    return jdbc.queryList(
        connection,
        "SELECT PRIMARY_KEY_LITERAL FROM DUMP_REQUEST_MISSING_KEY WHERE REQUEST_ID = ? ORDER BY ORDINAL_POSITION",
        resultSet -> PrimaryKeyTupleCodec.decode(resultSet.getString(1)),
        requestId);
  }

  private List<DumpRequest> assembleDumpRequests(
      Connection connection, List<RequestRow> rows) throws SQLException {
    Objects.requireNonNull(connection, "connection");
    List<RequestRow> requiredRows = List.copyOf(Objects.requireNonNull(rows, "rows"));
    if (requiredRows.isEmpty()) {
      return List.of();
    }
    Map<String, List<PrimaryKeyTuple>> requestKeysByRequestId =
        loadPrimaryKeyLiteralsByRequestId(
            connection, "DUMP_REQUEST_KEY", primaryKeyScopedRequestIds(requiredRows));
    ArrayList<DumpRequest> requests = new ArrayList<>(requiredRows.size());
    for (RequestRow row : requiredRows) {
      requests.add(
          new DumpRequest(
              row.requestId(),
              row.scope(),
              row.tableId(),
              requestKeysByRequestId.getOrDefault(row.requestId(), List.of())));
    }
    return List.copyOf(requests);
  }

  private Optional<DumpRequest> loadDumpRequest(Connection connection, String requestId)
      throws SQLException {
    return jdbc.queryOptional(
        connection,
        "SELECT SCOPE_VALUE, DATABASE_NAME, SCHEMA_NAME_VALUE, TABLE_NAME_VALUE "
            + "FROM DUMP_REQUEST WHERE REQUEST_ID = ?",
        resultSet -> {
          DumpScope scope = DumpScope.valueOf(resultSet.getString("SCOPE_VALUE"));
          String databaseName = resultSet.getString("DATABASE_NAME");
          TableId tableId =
              databaseName == null
                  ? null
                  : new TableId(
                      databaseName,
                      resultSet.getString("SCHEMA_NAME_VALUE"),
                      resultSet.getString("TABLE_NAME_VALUE"));
          return new DumpRequest(requestId, scope, tableId, loadDumpRequestKeys(connection, requestId));
        },
        requestId);
  }

  private StoredDumpRequest storedDumpRequest(Connection connection, ResultSet resultSet)
      throws SQLException {
    String requestId = resultSet.getString("REQUEST_ID");
    DumpScope scope = DumpScope.valueOf(resultSet.getString("SCOPE_VALUE"));
    String databaseName = resultSet.getString("DATABASE_NAME");
    TableId tableId =
        databaseName == null
            ? null
            : new TableId(
                databaseName,
                resultSet.getString("SCHEMA_NAME_VALUE"),
                resultSet.getString("TABLE_NAME_VALUE"));
    return new StoredDumpRequest(
        new DumpRequest(requestId, scope, tableId, loadDumpRequestKeys(connection, requestId)),
        Instant.parse(resultSet.getString("CREATED_AT")),
        Instant.parse(resultSet.getString("EFFECTIVE_UPDATED_AT")));
  }

  private DetailedRequestRow detailedRequestRow(ResultSet resultSet) throws SQLException {
    String requestId = resultSet.getString("REQUEST_ID");
    DumpScope scope = DumpScope.valueOf(resultSet.getString("SCOPE_VALUE"));
    String databaseName = resultSet.getString("DATABASE_NAME");
    TableId tableId =
        databaseName == null
            ? null
            : new TableId(
                databaseName,
                resultSet.getString("SCHEMA_NAME_VALUE"),
                resultSet.getString("TABLE_NAME_VALUE"));
    return new DetailedRequestRow(
        requestId,
        scope,
        tableId,
        Instant.parse(resultSet.getString("CREATED_AT")),
        Instant.parse(resultSet.getString("EFFECTIVE_UPDATED_AT")),
        resultSet.getString("STATE_VALUE"),
        resultSet.getString("FAILURE_REASON"));
  }

  private RequestRow requestRow(ResultSet resultSet) throws SQLException {
    String requestId = resultSet.getString("REQUEST_ID");
    DumpScope scope = DumpScope.valueOf(resultSet.getString("SCOPE_VALUE"));
    String databaseName = resultSet.getString("DATABASE_NAME");
    TableId tableId =
        databaseName == null
            ? null
            : new TableId(
                databaseName,
                resultSet.getString("SCHEMA_NAME_VALUE"),
                resultSet.getString("TABLE_NAME_VALUE"));
    return new RequestRow(requestId, scope, tableId);
  }

  private List<StoredDumpRequestDetail> materializeStoredDumpRequestDetails(
      Connection connection, List<DetailedRequestRow> rows)
      throws SQLException {
    if (rows.isEmpty()) {
      return List.of();
    }
    List<String> requestIds = primaryKeyScopedRequestIds(rows);
    Map<String, List<PrimaryKeyTuple>> requestKeysByRequestId =
        loadPrimaryKeyLiteralsByRequestId(connection, "DUMP_REQUEST_KEY", requestIds);
    Map<String, List<PrimaryKeyTuple>> missingKeysByRequestId =
        loadPrimaryKeyLiteralsByRequestId(connection, "DUMP_REQUEST_MISSING_KEY", requestIds);

    ArrayList<StoredDumpRequestDetail> details = new ArrayList<>(rows.size());
    for (DetailedRequestRow row : rows) {
      DumpRequest request =
          new DumpRequest(
              row.requestId(),
              row.scope(),
              row.tableId(),
              requestKeysByRequestId.getOrDefault(row.requestId(), List.of()));
      StoredDumpRequest storedRequest =
          new StoredDumpRequest(request, row.createdAt(), row.updatedAt());
      DumpRequestStatus status =
          row.stateValue() == null
              ? null
              : new DumpRequestStatus(
                  row.requestId(),
                  row.scope(),
                  row.tableId(),
                  DumpRequestState.valueOf(row.stateValue()),
                  missingKeysByRequestId.getOrDefault(row.requestId(), List.of()),
                  row.failureReason());
      details.add(new StoredDumpRequestDetail(storedRequest, status));
    }
    return List.copyOf(details);
  }

  private Map<String, List<PrimaryKeyTuple>> loadPrimaryKeyLiteralsByRequestId(
      Connection connection, String tableName, List<String> requestIds)
      throws SQLException {
    Objects.requireNonNull(connection, "connection");
    Objects.requireNonNull(tableName, "tableName");
    List<String> requiredRequestIds = List.copyOf(Objects.requireNonNull(requestIds, "requestIds"));
    if (requiredRequestIds.isEmpty()) {
      return Map.of();
    }
    String placeholders = String.join(", ", Collections.nCopies(requiredRequestIds.size(), "?"));
    List<RequestKeyRow> rows =
        jdbc.queryList(
            connection,
            "SELECT REQUEST_ID, PRIMARY_KEY_LITERAL "
                + "FROM "
                + tableName
                + " WHERE REQUEST_ID IN ("
                + placeholders
                + ") "
                + "ORDER BY REQUEST_ID, ORDINAL_POSITION",
            resultSet ->
                new RequestKeyRow(
                    resultSet.getString("REQUEST_ID"),
                    PrimaryKeyTupleCodec.decode(resultSet.getString("PRIMARY_KEY_LITERAL"))),
            requiredRequestIds.toArray());
    LinkedHashMap<String, List<PrimaryKeyTuple>> grouped = new LinkedHashMap<>();
    for (RequestKeyRow row : rows) {
      grouped.computeIfAbsent(row.requestId(), ignored -> new ArrayList<>()).add(row.primaryKeyLiteral());
    }
    LinkedHashMap<String, List<PrimaryKeyTuple>> immutable = new LinkedHashMap<>();
    for (Map.Entry<String, List<PrimaryKeyTuple>> entry : grouped.entrySet()) {
      immutable.put(entry.getKey(), List.copyOf(entry.getValue()));
    }
    return Map.copyOf(immutable);
  }

  private Optional<DumpRequestStatus> loadDumpRequestStatusInternal(
      Connection connection, String requestId) throws SQLException {
    return jdbc.queryOptional(
        connection,
        "SELECT SCOPE_VALUE, DATABASE_NAME, SCHEMA_NAME_VALUE, TABLE_NAME_VALUE, STATE_VALUE, ACTIVE_LOW_WATERMARK, ACTIVE_HIGH_WATERMARK, FAILURE_REASON "
            + "FROM DUMP_REQUEST_STATUS WHERE REQUEST_ID = ?",
        resultSet -> {
          DumpScope scope = DumpScope.valueOf(resultSet.getString("SCOPE_VALUE"));
          String databaseName = resultSet.getString("DATABASE_NAME");
          TableId tableId =
              databaseName == null
                  ? null
                  : new TableId(
                      databaseName,
                      resultSet.getString("SCHEMA_NAME_VALUE"),
                      resultSet.getString("TABLE_NAME_VALUE"));
          List<PrimaryKeyTuple> missingKeys =
              scope == DumpScope.PRIMARY_KEYS
                  ? loadDumpRequestMissingKeys(connection, requestId)
                  : List.of();
          return new DumpRequestStatus(
              requestId,
              scope,
              tableId,
              DumpRequestState.valueOf(resultSet.getString("STATE_VALUE")),
              missingKeys,
              resultSet.getString("FAILURE_REASON"));
        },
        requestId);
  }

  private void saveDumpRequestStatusInternal(Connection connection, DumpRequestStatus status)
      throws SQLException {
    String now = Instant.now().toString();
    int updated =
        jdbc.update(
            connection,
            "UPDATE DUMP_REQUEST_STATUS SET SCOPE_VALUE = ?, DATABASE_NAME = ?, SCHEMA_NAME_VALUE = ?, TABLE_NAME_VALUE = ?, STATE_VALUE = ?, ACTIVE_LOW_WATERMARK = ?, ACTIVE_HIGH_WATERMARK = ?, FAILURE_REASON = ?, UPDATED_AT = ? WHERE REQUEST_ID = ?",
            status.scope().name(),
            status.tableId() == null ? null : status.tableId().databaseName(),
            status.tableId() == null ? null : status.tableId().schemaName(),
            status.tableId() == null ? null : status.tableId().tableName(),
            status.state().name(),
            null,
            null,
            status.failureReason(),
            now,
            status.requestId());
    if (updated == 0) {
      jdbc.insert(
          connection,
          "INSERT INTO DUMP_REQUEST_STATUS (REQUEST_ID, SCOPE_VALUE, DATABASE_NAME, SCHEMA_NAME_VALUE, TABLE_NAME_VALUE, STATE_VALUE, ACTIVE_LOW_WATERMARK, ACTIVE_HIGH_WATERMARK, FAILURE_REASON, UPDATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
          status.requestId(),
          status.scope().name(),
          status.tableId() == null ? null : status.tableId().databaseName(),
          status.tableId() == null ? null : status.tableId().schemaName(),
          status.tableId() == null ? null : status.tableId().tableName(),
          status.state().name(),
          null,
          null,
          status.failureReason(),
          now);
    }

    replaceDumpRequestMissingKeys(connection, status);
  }

  private boolean saveDumpRequestStatusIfCurrentStateInInternal(
      Connection connection,
      DumpRequestStatus status,
      boolean allowMissing,
      DumpRequestState... allowedCurrentStates)
      throws SQLException {
    String now = Instant.now().toString();
    int updated = 0;
    if (allowedCurrentStates.length > 0) {
      StringBuilder sql =
          new StringBuilder(
              "UPDATE DUMP_REQUEST_STATUS SET SCOPE_VALUE = ?, DATABASE_NAME = ?, SCHEMA_NAME_VALUE = ?, TABLE_NAME_VALUE = ?, STATE_VALUE = ?, ACTIVE_LOW_WATERMARK = ?, ACTIVE_HIGH_WATERMARK = ?, FAILURE_REASON = ?, UPDATED_AT = ? WHERE REQUEST_ID = ? AND STATE_VALUE IN (");
      for (int index = 0; index < allowedCurrentStates.length; index++) {
        if (index > 0) {
          sql.append(", ");
        }
        sql.append("?");
      }
      sql.append(")");
      List<Object> parameters = new ArrayList<>();
      parameters.add(status.scope().name());
      parameters.add(status.tableId() == null ? null : status.tableId().databaseName());
      parameters.add(status.tableId() == null ? null : status.tableId().schemaName());
      parameters.add(status.tableId() == null ? null : status.tableId().tableName());
      parameters.add(status.state().name());
      parameters.add(null);
      parameters.add(null);
      parameters.add(status.failureReason());
      parameters.add(now);
      parameters.add(status.requestId());
      Arrays.stream(allowedCurrentStates).map(DumpRequestState::name).forEach(parameters::add);
      updated = jdbc.update(connection, sql.toString(), parameters.toArray());
    }
    if (updated == 0) {
      if (!allowMissing) {
        return false;
      }
      updated =
          jdbc.update(
              connection,
              "INSERT INTO DUMP_REQUEST_STATUS (REQUEST_ID, SCOPE_VALUE, DATABASE_NAME, SCHEMA_NAME_VALUE, TABLE_NAME_VALUE, STATE_VALUE, ACTIVE_LOW_WATERMARK, ACTIVE_HIGH_WATERMARK, FAILURE_REASON, UPDATED_AT) "
                  + "SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ? "
                  + "WHERE NOT EXISTS (SELECT 1 FROM DUMP_REQUEST_STATUS WHERE REQUEST_ID = ?)",
              status.requestId(),
              status.scope().name(),
              status.tableId() == null ? null : status.tableId().databaseName(),
              status.tableId() == null ? null : status.tableId().schemaName(),
              status.tableId() == null ? null : status.tableId().tableName(),
              status.state().name(),
              null,
              null,
              status.failureReason(),
              now,
              status.requestId());
      if (updated == 0) {
        return false;
      }
    }

    replaceDumpRequestMissingKeys(connection, status);
    return true;
  }

  private void replaceDumpRequestMissingKeys(Connection connection, DumpRequestStatus status)
      throws SQLException {
    List<PrimaryKeyTuple> desiredMissingKeys =
        status.scope() == DumpScope.PRIMARY_KEYS ? status.missingPrimaryKeyTuples() : List.of();
    List<PrimaryKeyTuple> currentMissingKeys =
        loadDumpRequestMissingKeys(connection, status.requestId());
    if (currentMissingKeys.equals(desiredMissingKeys)) {
      return;
    }
    jdbc.update(connection, "DELETE FROM DUMP_REQUEST_MISSING_KEY WHERE REQUEST_ID = ?", status.requestId());
    for (int index = 0; index < desiredMissingKeys.size(); index++) {
      jdbc.insert(
          connection,
          "INSERT INTO DUMP_REQUEST_MISSING_KEY (REQUEST_ID, ORDINAL_POSITION, PRIMARY_KEY_LITERAL) VALUES (?, ?, ?)",
          status.requestId(),
          index,
          PrimaryKeyTupleCodec.encode(desiredMissingKeys.get(index)));
    }
  }

  private static boolean isDuplicateRequestIdViolation(IllegalStateException exception) {
    Throwable cause = exception;
    while (cause != null) {
      if (cause instanceof SQLException sqlException && "23505".equals(sqlException.getSQLState())) {
        return true;
      }
      cause = cause.getCause();
    }
    return false;
  }

  private record DetailedRequestRow(
      String requestId,
      DumpScope scope,
      TableId tableId,
      Instant createdAt,
      Instant updatedAt,
      String stateValue,
      String failureReason)
      implements HasRequestScope {}

  private record RequestRow(String requestId, DumpScope scope, TableId tableId)
      implements HasRequestScope {}

  private record RequestKeyRow(String requestId, PrimaryKeyTuple primaryKeyLiteral) {}

  private static List<String> primaryKeyScopedRequestIds(List<? extends HasRequestScope> rows) {
    ArrayList<String> requestIds = new ArrayList<>();
    for (HasRequestScope row : rows) {
      if (row.scope() == DumpScope.PRIMARY_KEYS) {
        requestIds.add(row.requestId());
      }
    }
    return List.copyOf(requestIds);
  }

  private interface HasRequestScope {
    String requestId();

    DumpScope scope();
  }
}
