package io.github.aandreakis.dblog.adapter.mysql.internal;

import io.github.aandreakis.dblog.adapter.api.SourceRequiresFullDumpException;
import io.github.aandreakis.dblog.adapter.api.SourceSchemaUncertaintyException;
import io.github.aandreakis.dblog.adapter.mysql.MySqlBinlogTransaction;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourcePosition;
import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.PrimaryKeyHash;
import io.github.aandreakis.dblog.core.model.RowLayout;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.reconcile.HeartbeatMetadata;
import io.github.aandreakis.dblog.core.reconcile.WatermarkMetadata;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralValueNormalizer;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.runtime.sql.SourceFlowControlSnapshot;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * First MySQL binlog message session.
 *
 * <p>This is intentionally narrower than the broader runtime session: it can decode the current
 * MySQL binlog messages into committed transactions, surface metadata watermark and
 * heartbeat events, and fail closed on unsupported query/metadata conditions. Destructive live
 * DDL, selected-column table-map or row-tuple drift, and live primary-key updates are surfaced
 * as full-dump-required adapter signals so the checkpoint-owning wrapper can persist the
 * operator recovery state before rethrowing.
 */
public final class MySqlBinlogSession implements MySqlTransactionStream {
  private static final Logger log = LoggerFactory.getLogger(MySqlBinlogSession.class);

  private static final Object SKIP_DECODED_VALUE = new Object();
  private static final UserValueDecoder SKIP_DECODER = value -> SKIP_DECODED_VALUE;
  private static final UserValueDecoder RAW_VALUE_DECODER = value -> value;

  /**
   * Matches DBLog's own metadata bootstrap DDL when it leaks into the binlog because the
   * connection user lacks {@code SESSION_VARIABLES_ADMIN}. The regex uses backtick-quoted
   * schema/table identifiers so it cannot collide with user tables whose names merely
   * contain the substring {@code dblog_meta}. Used together with
   * {@link java.util.regex.Matcher#matches} (not {@code find}) so the whole statement must
   * line up, not just a prefix. See {@link MySqlBinlogBypass} for the primary bypass path.
   */
  private static final Pattern METADATA_BOOTSTRAP_DDL =
      Pattern.compile(
          "CREATE\\s+(?:DATABASE|SCHEMA)\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?`DBLOG_META`\\s*|"
              + "CREATE\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?`DBLOG_META`\\.`(?:WATERMARKS|HEARTBEATS)`.*");

  private static final AtomicBoolean METADATA_BOOTSTRAP_FALLBACK_WARNED = new AtomicBoolean(false);

  private static final Pattern METADATA_DML =
      Pattern.compile(
          "(?:INSERT|REPLACE)\\s+INTO\\s+(?:`DBLOG_META`\\.)?`(?:WATERMARKS|HEARTBEATS)`(?:\\s.*|$)|"
              + "UPDATE\\s+(?:`DBLOG_META`\\.)?`(?:WATERMARKS|HEARTBEATS)`(?:\\s.*|$)",
          Pattern.DOTALL);

  private static final Pattern ROW_OR_SCHEMA_AFFECTING_DDL =
      Pattern.compile(
          "(?:TRUNCATE\\s+(?:TABLE\\s+)?|ALTER\\s+TABLE\\s+|DROP\\s+TABLE\\s+|"
              + "RENAME\\s+TABLE\\s+|CREATE\\s+TABLE\\s+).*",
          Pattern.DOTALL);

  /**
   * Matches DDL families that are safe for DBLog to skip even when they arrive on the
   * captured database. These statements either target server-wide objects (users, privileges,
   * session variables) or are storage-only operations on a table that do not change the row
   * state DBLog replicates or the column-surface DBLog fingerprints. Used with
   * {@link java.util.regex.Matcher#matches} so a full-statement prefix match is required.
   *
   * <p>Statements that DO change row state (TRUNCATE) or the schema fingerprint
   * (ALTER/RENAME/DROP/CREATE TABLE) are intentionally NOT listed here and continue to fail
   * closed via {@link #unsupportedDdlReason}. Skipping TRUNCATE on a captured table would
   * silently diverge the target from source; skipping ALTER TABLE would mask schema drift
   * that spec §18 requires to fail closed.
   */
  private static final Pattern BENIGN_ADMIN_DDL =
      Pattern.compile(
          // privilege management
          "(GRANT|REVOKE)\\s+.*|"
              // users
              + "(CREATE|DROP|ALTER|RENAME)\\s+USER(\\s+.*|$)|"
              // stored routines / triggers / views / events (definer prefix optional)
              + "(CREATE|DROP|ALTER)\\s+(DEFINER\\s*=\\s*\\S+\\s+)?"
              + "(PROCEDURE|FUNCTION|TRIGGER|VIEW|EVENT)(\\s.*|$)|"
              // indexes (storage-only; column surface fingerprint is unaffected)
              + "(CREATE|DROP)\\s+(UNIQUE\\s+|FULLTEXT\\s+|SPATIAL\\s+)?INDEX\\s+.*|"
              // table maintenance operations that do not alter row state
              + "(ANALYZE|OPTIMIZE|REPAIR|CHECK)\\s+(NO_WRITE_TO_BINLOG\\s+|LOCAL\\s+)?TABLE\\s+.*|"
              // server/session housekeeping
              + "FLUSH(\\s+.*|$)|"
              + "SET\\s+.*|"
              + "RESET\\s+.*",
          // DOTALL so multi-line statement bodies (e.g. CREATE PROCEDURE ... BEGIN ... END)
          // are matched as a whole by Matcher.matches(). Without this, the trailing
          // newline-bearing body causes the full-anchored match to fail and the session
          // would fall through to the fatal DDL throw for a benign statement.
          Pattern.DOTALL);

  private static final AtomicBoolean BENIGN_ADMIN_DDL_WARNED = new AtomicBoolean(false);

  private final String currentRunId;
  private final String currentSourceStreamId;
  private final String sourceId;
  private final Map<TableKey, TableSchema> schemasByActualTable;
  private final TableId watermarkTableId;
  private final TableId heartbeatTableId;
  private final MySqlBinlogStream stream;
  private final Map<Long, TableContext> tableContextsById = new HashMap<>();
  private final List<ChangeEvent> currentEvents = new ArrayList<>();
  private String currentGtid;
  private String currentSyntheticTransactionId;
  private boolean transactionOpen;
  private boolean ownHeartbeatObserved;

  public MySqlBinlogSession(
      String currentRunId,
      String currentSourceStreamId,
      String sourceId,
      List<TableSchema> capturedSchemas,
      MySqlBinlogStream stream) {
    this.currentRunId = requireNonBlank(currentRunId, "currentRunId");
    this.currentSourceStreamId = requireNonBlank(currentSourceStreamId, "currentSourceStreamId");
    this.sourceId = requireNonBlank(sourceId, "sourceId");
    this.stream = Objects.requireNonNull(stream, "stream");
    this.schemasByActualTable = new LinkedHashMap<>(indexSchemas(capturedSchemas));
    String metadataDatabaseName = metadataDatabaseName(capturedSchemas, sourceId);
    this.watermarkTableId = WatermarkMetadata.tableIdFor(metadataDatabaseName);
    this.heartbeatTableId = HeartbeatMetadata.tableIdFor(metadataDatabaseName);
  }

  @Override
  public Optional<MySqlBinlogTransaction> readPendingTransaction() throws SQLException {
    while (true) {
      Optional<MySqlBinlogMessage> maybeMessage = stream.readMessage();
      if (maybeMessage.isEmpty()) {
        return Optional.empty();
      }
      Optional<MySqlBinlogTransaction> transaction = handleMessage(maybeMessage.orElseThrow());
      if (transaction.isPresent()) {
        return transaction;
      }
    }
  }

  @Override
  public SourceFlowControlSnapshot sourceFlowControlSnapshot() {
    return stream.sourceFlowControlSnapshot();
  }

  public List<TableSchema> currentCapturedSchemas() {
    return List.copyOf(schemasByActualTable.values());
  }

  public void updateCapturedSchema(TableSchema schema) {
    Objects.requireNonNull(schema, "schema");
    schemasByActualTable.put(
        new TableKey(schema.tableId().schemaName(), schema.tableId().tableName()), schema);
  }

  @Override
  public void close() throws Exception {
    stream.close();
  }

  private Optional<MySqlBinlogTransaction> handleMessage(MySqlBinlogMessage message)
      throws SQLException {
    if (message instanceof MySqlBinlogMessage.Rotate) {
      return Optional.empty();
    }
    if (message instanceof MySqlBinlogMessage.Gtid gtidMessage) {
      beginGtid(gtidMessage.gtid());
      return Optional.empty();
    }
    if (message instanceof MySqlBinlogMessage.Query queryMessage) {
      return handleQuery(queryMessage);
    }
    if (message instanceof MySqlBinlogMessage.TableMap tableMapMessage) {
      tableContextsById.put(tableMapMessage.tableId(), resolveTableContext(tableMapMessage));
      return Optional.empty();
    }
    if (message instanceof MySqlBinlogMessage.WriteRows writeRowsMessage) {
      openImplicitTransaction(writeRowsMessage.position());
      appendWriteRows(writeRowsMessage);
      return Optional.empty();
    }
    if (message instanceof MySqlBinlogMessage.UpdateRows updateRowsMessage) {
      openImplicitTransaction(updateRowsMessage.position());
      appendUpdateRows(updateRowsMessage);
      return Optional.empty();
    }
    if (message instanceof MySqlBinlogMessage.DeleteRows deleteRowsMessage) {
      openImplicitTransaction(deleteRowsMessage.position());
      appendDeleteRows(deleteRowsMessage);
      return Optional.empty();
    }
    if (message instanceof MySqlBinlogMessage.Commit commitMessage) {
      return commit(commitMessage.transactionId(), commitMessage.position(), commitMessage.eventTimestamp());
    }
    throw new IllegalStateException(
        "Unhandled MySQL binlog message type: " + message.getClass().getName());
  }

  private Optional<MySqlBinlogTransaction> handleQuery(MySqlBinlogMessage.Query queryMessage) {
    String normalizedSql = queryMessage.sql().trim().toUpperCase(java.util.Locale.ROOT);
    if (normalizedSql.equals("BEGIN") || normalizedSql.startsWith("XA START")) {
      beginExplicitTransaction(queryMessage.position());
      return Optional.empty();
    }
    if (normalizedSql.equals("COMMIT") || normalizedSql.startsWith("XA COMMIT")) {
      return commit(
          currentTransactionEventId(queryMessage.position()),
          queryMessage.position(),
          queryMessage.eventTimestamp());
    }
    if (normalizedSql.equals("ROLLBACK") || normalizedSql.startsWith("XA ROLLBACK")) {
      rollback();
      return Optional.empty();
    }
    if (shouldIgnoreQuery(queryMessage)) {
      return Optional.empty();
    }
    throw SourceRequiresFullDumpException.sourceLevel(unsupportedDdlReason(queryMessage));
  }

  private static String unsupportedDdlReason(MySqlBinlogMessage.Query queryMessage) {
    return "MySQL binlog session encountered row-state-affecting or schema-affecting DDL that"
        + " DBLog cannot decode (TRUNCATE, ALTER TABLE, DROP TABLE, RENAME TABLE, and"
        + " CREATE TABLE on the captured database fail closed by design — spec §18);"
        + " full dump required. To recover: stop DBLog, verify the target matches the current"
        + " source state or bootstrap the target, "
        + resetPersistedStateRecoveryStep("this DDL")
        + ". sql="
        + queryMessage.sql();
  }

  private boolean shouldIgnoreQuery(MySqlBinlogMessage.Query queryMessage) {
    String databaseName = queryMessage.databaseName();
    String normalizedSql = queryMessage.sql().trim().toUpperCase(java.util.Locale.ROOT);
    if (databaseName == null && normalizedSql.startsWith("SET ")) {
      return true;
    }
    // Defensive fallback: DBLog's own metadata bootstrap DDL reached the binlog because
    // the bootstrap connection could not disable session binary logging (missing
    // SESSION_VARIABLES_ADMIN). The statement's databaseName is typically the user's
    // default schema, not dblog_meta, so the fast path above does not catch it. Match by
    // narrow anchored regex on the backtick-quoted identifiers so we do not collide with
    // user-table names that merely contain "dblog_meta". Warn once so operators know their
    // downstream replicas are observing these statements.
    if (METADATA_BOOTSTRAP_DDL.matcher(normalizedSql).matches()) {
      if (METADATA_BOOTSTRAP_FALLBACK_WARNED.compareAndSet(false, true)) {
        log.warn(
            "DBLog metadata bootstrap DDL observed in binlog (source={}, run={}, database={}):"
                + " the bootstrap connection could not disable session binary logging; grant"
                + " SESSION_VARIABLES_ADMIN (MySQL 8+) or SUPER (5.7) to the DBLog user so"
                + " these statements stop propagating to downstream replicas. DBLog will"
                + " continue to skip them on this session. sql={}",
            sourceId,
            currentRunId,
            databaseName,
            queryMessage.sql());
      }
      return true;
    }
    if (isRelevantRowOrSchemaAffectingDdl(queryMessage, normalizedSql)) {
      return false;
    }
    if (databaseName != null && !isRelevantDatabase(databaseName)) {
      return true;
    }
    // DBLog's own metadata DML may run with the session's default database set to dblog_meta.
    // Only the exact watermark/heartbeat mutation shapes are safe to ignore; arbitrary DDL in
    // that default database must still fail closed.
    if (databaseName != null
        && WatermarkMetadata.SCHEMA_NAME.equalsIgnoreCase(databaseName)
        && METADATA_DML.matcher(normalizedSql).matches()) {
      return true;
    }
    if (BENIGN_ADMIN_DDL.matcher(normalizedSql).matches()) {
      if (BENIGN_ADMIN_DDL_WARNED.compareAndSet(false, true)) {
        log.warn(
            "DBLog observed administrative DDL in the binlog and is skipping it (source={},"
                + " run={}, database={}): families such as GRANT/REVOKE, CREATE/DROP USER,"
                + " stored routines/triggers/views/events, CREATE/DROP INDEX, ANALYZE/OPTIMIZE,"
                + " FLUSH, SET, and RESET do not change captured row state or the column-surface"
                + " fingerprint, so DBLog forwards nothing for them. Row-state- or"
                + " schema-affecting DDL (TRUNCATE, ALTER/RENAME/DROP/CREATE TABLE) still fails"
                + " closed. sql={}",
            sourceId,
            currentRunId,
            databaseName,
            queryMessage.sql());
      }
      return true;
    }
    return false;
  }

  private boolean isRelevantRowOrSchemaAffectingDdl(
      MySqlBinlogMessage.Query queryMessage, String normalizedSql) {
    if (!ROW_OR_SCHEMA_AFFECTING_DDL.matcher(normalizedSql).matches()) {
      return false;
    }
    String databaseName = queryMessage.databaseName();
    if (databaseName != null && isRelevantDatabase(databaseName)) {
      return true;
    }
    return mentionsRelevantTable(normalizedSql);
  }

  private boolean mentionsRelevantTable(String normalizedSql) {
    String sqlWithoutTicks = normalizedSql.replace("`", "");
    for (TableKey key : schemasByActualTable.keySet()) {
      if (containsQualifiedTableReference(sqlWithoutTicks, key.databaseName(), key.tableName())) {
        return true;
      }
    }
    return containsQualifiedTableReference(
            sqlWithoutTicks, WatermarkMetadata.SCHEMA_NAME, WatermarkMetadata.TABLE_NAME)
        || containsQualifiedTableReference(
            sqlWithoutTicks, HeartbeatMetadata.SCHEMA_NAME, HeartbeatMetadata.TABLE_NAME);
  }

  private static boolean containsQualifiedTableReference(
      String normalizedSqlWithoutTicks, String databaseName, String tableName) {
    String reference =
        "(?<![A-Z0-9_])"
            + Pattern.quote(databaseName.toUpperCase(java.util.Locale.ROOT))
            + "\\s*\\.\\s*"
            + Pattern.quote(tableName.toUpperCase(java.util.Locale.ROOT))
            + "(?![A-Z0-9_])";
    return Pattern.compile(reference).matcher(normalizedSqlWithoutTicks).find();
  }

  private boolean isRelevantDatabase(String databaseName) {
    if (WatermarkMetadata.SCHEMA_NAME.equalsIgnoreCase(databaseName)) {
      return true;
    }
    for (TableKey key : schemasByActualTable.keySet()) {
      if (key.databaseName().equalsIgnoreCase(databaseName)) {
        return true;
      }
    }
    return false;
  }

  private void appendWriteRows(MySqlBinlogMessage.WriteRows rowMessage) {
    TableContext context = resolveMappedContext(rowMessage.tableId());
    if (context.kind() == TableContextKind.IGNORED) {
      return;
    }
    for (Object[] rowValues : rowMessage.rows()) {
      currentEvents.addAll(decodeWriteRow(context, rowValues, rowMessage.position()));
    }
  }

  private void appendUpdateRows(MySqlBinlogMessage.UpdateRows rowMessage) {
    TableContext context = resolveMappedContext(rowMessage.tableId());
    if (context.kind() == TableContextKind.IGNORED) {
      return;
    }
    for (MySqlBinlogMessage.RowChange rowChange : rowMessage.rows()) {
      currentEvents.addAll(decodeUpdateRow(context, rowChange, rowMessage.position()));
    }
  }

  private void appendDeleteRows(MySqlBinlogMessage.DeleteRows rowMessage) {
    TableContext context = resolveMappedContext(rowMessage.tableId());
    if (context.kind() == TableContextKind.IGNORED) {
      return;
    }
    if (context.kind() != TableContextKind.USER) {
      throw SourceSchemaUncertaintyException.forTable(
          context.tableId(),
          "MySQL metadata tables must not emit DELETE events: "
              + context.tableId().displayName());
    }
    for (Object[] rowValues : rowMessage.rows()) {
      DecodedUserRow beforeRow = decodeUserRow(context, rowValues);
      currentEvents.add(
          new ChangeEvent(
              context.tableId(),
              OperationType.DELETE,
              CaptureOrigin.LOG,
              beforeRow.primaryKey(),
              beforeRow.row(),
              null,
              rowMessage.position(),
              currentTransactionEventId(rowMessage.position()),
              null,
              beforeRow.primaryKeyHash()));
    }
  }

  private List<ChangeEvent> decodeWriteRow(
      TableContext context, Object[] rowValues, MySqlSourcePosition checkpointPosition) {
    if (context.kind() == TableContextKind.WATERMARK) {
      Optional<ChangeEvent> decoded = decodeWatermarkWrite(rowValues, checkpointPosition);
      return decoded.map(List::of).orElseGet(List::of);
    }
    if (context.kind() == TableContextKind.HEARTBEAT) {
      Optional<ChangeEvent> decoded = decodeHeartbeatWrite(rowValues, checkpointPosition);
      return decoded.map(List::of).orElseGet(List::of);
    }
    DecodedUserRow afterRow = decodeUserRow(context, rowValues);
    return List.of(
        new ChangeEvent(
            context.tableId(),
            OperationType.INSERT,
            CaptureOrigin.LOG,
            afterRow.primaryKey(),
            null,
            afterRow.row(),
            checkpointPosition,
            currentTransactionEventId(checkpointPosition),
            null,
            afterRow.primaryKeyHash()));
  }

  private List<ChangeEvent> decodeUpdateRow(
      TableContext context,
      MySqlBinlogMessage.RowChange rowChange,
      MySqlSourcePosition checkpointPosition) {
    if (context.kind() == TableContextKind.WATERMARK) {
      Optional<ChangeEvent> decoded = decodeWatermarkUpdate(rowChange, checkpointPosition);
      return decoded.map(List::of).orElseGet(List::of);
    }
    if (context.kind() == TableContextKind.HEARTBEAT) {
      Optional<ChangeEvent> decoded = decodeHeartbeatUpdate(rowChange, checkpointPosition);
      return decoded.map(List::of).orElseGet(List::of);
    }
    DecodedUserRow beforeRow = decodeUserRow(context, rowChange.beforeValues());
    DecodedUserRow afterRow = decodeUserRow(context, rowChange.afterValues());
    if (!beforeRow.primaryKey().equals(afterRow.primaryKey())) {
      throw SourceRequiresFullDumpException.forTable(
          context.tableId(), primaryKeyUpdateRequiresFullDumpReason(context.tableId()));
    }
    return List.of(
        new ChangeEvent(
            context.tableId(),
            OperationType.UPDATE,
            CaptureOrigin.LOG,
            beforeRow.primaryKey(),
            beforeRow.row(),
            afterRow.row(),
            checkpointPosition,
            currentTransactionEventId(checkpointPosition),
            null,
            afterRow.primaryKeyHash()));
  }

  private Optional<ChangeEvent> decodeWatermarkWrite(
      Object[] rowValues, MySqlSourcePosition checkpointPosition) {
    ImmutableRowImage afterRow =
        decodeWatermarkMetadataRow(rowValues);
    if (isBootstrapWatermarkRow(afterRow)) {
      return Optional.empty();
    }
    if (afterRow.get(WatermarkMetadata.TOKEN_COLUMN) == null) {
      throw SourceSchemaUncertaintyException.forTable(
          watermarkTableId, "MySQL watermark metadata row is missing a non-blank token value");
    }
    if (!currentRunId.equals(afterRow.get(WatermarkMetadata.RUN_ID_COLUMN))) {
      return Optional.empty();
    }
    return Optional.of(
        new ChangeEvent(
            watermarkTableId,
            OperationType.WATERMARK,
            CaptureOrigin.LOG,
            MySqlMetadataRows.metadataPrimaryKey(),
            null,
            afterRow,
            checkpointPosition,
            currentTransactionEventId(checkpointPosition),
            null));
  }

  private Optional<ChangeEvent> decodeWatermarkUpdate(
      MySqlBinlogMessage.RowChange rowChange, MySqlSourcePosition checkpointPosition) {
    ImmutableRowImage beforeRow =
        decodeWatermarkMetadataRow(rowChange.beforeValues());
    ImmutableRowImage afterRow =
        decodeWatermarkMetadataRow(rowChange.afterValues());
    if (isBootstrapWatermarkRow(afterRow)) {
      return Optional.empty();
    }
    if (afterRow.get(WatermarkMetadata.TOKEN_COLUMN) == null) {
      throw SourceSchemaUncertaintyException.forTable(
          watermarkTableId, "MySQL watermark metadata row is missing a non-blank token value");
    }
    if (!currentRunId.equals(afterRow.get(WatermarkMetadata.RUN_ID_COLUMN))) {
      return Optional.empty();
    }
    return Optional.of(
        new ChangeEvent(
            watermarkTableId,
            OperationType.WATERMARK,
            CaptureOrigin.LOG,
            MySqlMetadataRows.metadataPrimaryKey(),
            beforeRow,
            afterRow,
            checkpointPosition,
            currentTransactionEventId(checkpointPosition),
            null));
  }

  private Optional<ChangeEvent> decodeHeartbeatWrite(
      Object[] rowValues, MySqlSourcePosition checkpointPosition) {
    ImmutableRowImage afterRow =
        decodeHeartbeatMetadataRow(rowValues);
    if (isBootstrapHeartbeatRow(afterRow)) {
      return Optional.empty();
    }
    if (afterRow.get(HeartbeatMetadata.TIMESTAMP_COLUMN) == null) {
      throw SourceSchemaUncertaintyException.forTable(
          heartbeatTableId,
          "MySQL heartbeat metadata row is missing a non-blank heartbeat timestamp");
    }
    if (!shouldSurfaceHeartbeat(afterRow)) {
      return Optional.empty();
    }
    return Optional.of(
        new ChangeEvent(
            heartbeatTableId,
            OperationType.HEARTBEAT,
            CaptureOrigin.LOG,
            MySqlMetadataRows.metadataPrimaryKey(),
            null,
            afterRow,
            checkpointPosition,
            currentTransactionEventId(checkpointPosition),
            null));
  }

  private Optional<ChangeEvent> decodeHeartbeatUpdate(
      MySqlBinlogMessage.RowChange rowChange, MySqlSourcePosition checkpointPosition) {
    ImmutableRowImage beforeRow =
        decodeHeartbeatMetadataRow(rowChange.beforeValues());
    ImmutableRowImage afterRow =
        decodeHeartbeatMetadataRow(rowChange.afterValues());
    if (isBootstrapHeartbeatRow(afterRow)) {
      return Optional.empty();
    }
    if (afterRow.get(HeartbeatMetadata.TIMESTAMP_COLUMN) == null) {
      throw SourceSchemaUncertaintyException.forTable(
          heartbeatTableId,
          "MySQL heartbeat metadata row is missing a non-blank heartbeat timestamp");
    }
    if (!shouldSurfaceHeartbeat(afterRow)) {
      return Optional.empty();
    }
    return Optional.of(
        new ChangeEvent(
            heartbeatTableId,
            OperationType.HEARTBEAT,
            CaptureOrigin.LOG,
            MySqlMetadataRows.metadataPrimaryKey(),
            beforeRow,
            afterRow,
            checkpointPosition,
            currentTransactionEventId(checkpointPosition),
            null));
  }

  private boolean shouldSurfaceHeartbeat(
      ImmutableRowImage afterRow) {
    String observedRunId = stringValue(afterRow.get(HeartbeatMetadata.RUN_ID_COLUMN));
    String observedSourceStreamId =
        stringValue(afterRow.get(HeartbeatMetadata.SOURCE_STREAM_ID_COLUMN));
    if (currentRunId.equals(observedRunId)) {
      if (observedSourceStreamId != null
          && !currentSourceStreamId.equals(observedSourceStreamId)) {
        throw SourceSchemaUncertaintyException.forTable(
            heartbeatTableId,
            "MySQL observed its own heartbeat run_id on an unexpected source stream. runId="
                + currentRunId
                + " expectedStreamId="
                + currentSourceStreamId
                + " observedStreamId="
                + observedSourceStreamId);
      }
      ownHeartbeatObserved = true;
      return true;
    }
    if (ownHeartbeatObserved
        && observedSourceStreamId != null
        && currentSourceStreamId.equals(observedSourceStreamId)) {
      throw SourceSchemaUncertaintyException.forTable(
          heartbeatTableId,
          "MySQL detected another DBLog run writing heartbeats on the same source stream after confirming its own heartbeat. currentRunId="
              + currentRunId
              + " foreignRunId="
              + observedRunId
              + " sourceStreamId="
              + currentSourceStreamId);
    }
    return false;
  }

  private ImmutableRowImage
      decodeWatermarkMetadataRow(Object[] rowValues) {
    try {
      return MySqlMetadataRows.watermarkRow(rowValues, "MySQL", false);
    } catch (IllegalStateException ex) {
      throw SourceSchemaUncertaintyException.forTable(watermarkTableId, ex.getMessage(), ex);
    }
  }

  private ImmutableRowImage
      decodeHeartbeatMetadataRow(Object[] rowValues) {
    try {
      return MySqlMetadataRows.heartbeatRow(rowValues, "MySQL", false);
    } catch (IllegalStateException ex) {
      throw SourceSchemaUncertaintyException.forTable(heartbeatTableId, ex.getMessage(), ex);
    }
  }

  private static boolean isBootstrapHeartbeatRow(
      ImmutableRowImage row) {
    return row.get(HeartbeatMetadata.RUN_ID_COLUMN) == null
        && row.get(HeartbeatMetadata.SOURCE_STREAM_ID_COLUMN) == null
        && row.get(HeartbeatMetadata.TIMESTAMP_COLUMN) == null;
  }

  private static boolean isBootstrapWatermarkRow(
      ImmutableRowImage row) {
    return row.get(WatermarkMetadata.RUN_ID_COLUMN) == null
        && row.get(WatermarkMetadata.TOKEN_COLUMN) == null;
  }

  private DecodedUserRow decodeUserRow(TableContext context, Object[] rowValues) {
    Objects.requireNonNull(rowValues, "rowValues");
    if (rowValues.length < context.requiredTupleWidth()) {
      throw SourceRequiresFullDumpException.forTable(
          context.tableId(), rowTupleRequiresFullDumpReason(context, rowValues.length));
    }
    Object[] row = new Object[context.rowLayout().size()];
    Object[] primaryKey = new Object[context.primaryKeyLayout().size()];
    List<UserColumnPlan> decodePlan = context.userColumnPlans();
    for (UserColumnPlan plan : decodePlan) {
      Object normalizedValue = plan.decoder().decode(rowValues[plan.tupleIndex()]);
      if (normalizedValue == SKIP_DECODED_VALUE) {
        continue;
      }
      if (plan.rowIndex() >= 0) {
        row[plan.rowIndex()] = normalizedValue;
      }
      if (plan.primaryKeyIndex() >= 0) {
        primaryKey[plan.primaryKeyIndex()] = normalizedValue;
      }
    }
    ImmutableRowImage primaryKeyImage =
        ImmutableRowImage.ofLayout(
            context.primaryKeyLayout(), primaryKey);
    PrimaryKeyHash primaryKeyHash =
        context.schema().primaryKeyHashFor(primaryKeyImage);
    return new DecodedUserRow(
        primaryKeyImage,
        ImmutableRowImage.ofLayout(context.rowLayout(), row),
        primaryKeyHash);
  }

  private TableContext resolveMappedContext(long tableId) {
    TableContext context = tableContextsById.get(tableId);
    if (context == null) {
      throw new IllegalStateException(
          "MySQL row event arrived before a matching TABLE_MAP event for tableId=" + tableId);
    }
    return context;
  }

  private TableContext resolveTableContext(MySqlBinlogMessage.TableMap tableMap) {
    if (WatermarkMetadata.SCHEMA_NAME.equalsIgnoreCase(tableMap.databaseName())
        && WatermarkMetadata.TABLE_NAME.equalsIgnoreCase(tableMap.tableName())) {
      return new TableContext(
          TableContextKind.WATERMARK, watermarkTableId, null, null, null, List.of(), 0);
    }
    if (HeartbeatMetadata.SCHEMA_NAME.equalsIgnoreCase(tableMap.databaseName())
        && HeartbeatMetadata.TABLE_NAME.equalsIgnoreCase(tableMap.tableName())) {
      return new TableContext(
          TableContextKind.HEARTBEAT, heartbeatTableId, null, null, null, List.of(), 0);
    }
    TableKey key = new TableKey(tableMap.databaseName(), tableMap.tableName());
    TableSchema schema = schemasByActualTable.get(key);
    if (schema == null) {
      return new TableContext(TableContextKind.IGNORED, null, null, null, null, List.of(), 0);
    }
    List<String> columnNames =
        tableMap.columnNames().isEmpty()
            ? schema.columns().stream().map(ColumnDefinition::name).toList()
            : tableMap.columnNames();
    return userTableContext(schema, columnNames);
  }

  private static TableContext userTableContext(TableSchema schema, List<String> columnNames) {
    List<String> primaryKeyColumnNames =
        schema.primaryKeyDefinitions().stream()
            .map(ColumnDefinition::name)
            .toList();
    Map<String, Integer> primaryKeyIndexByName = indexByName(primaryKeyColumnNames);
    Map<String, Integer> tupleIndexByName = indexByName(columnNames);
    List<UserColumnPlan> plans = new ArrayList<>(schema.selectedColumns().size());
    List<String> rowColumnNames = new ArrayList<>();
    int requiredTupleWidth = 0;
    for (ColumnDefinition definition :
        schema.selectedColumns()) {
      Integer tupleIndex = tupleIndexByName.get(definition.name());
      if (tupleIndex == null) {
        throw SourceRequiresFullDumpException.forTable(
            schema.tableId(),
            tableMapColumnsRequireFullDumpReason(schema, definition.name(), columnNames));
      }
      requiredTupleWidth = Math.max(requiredTupleWidth, tupleIndex + 1);
      int rowIndex = rowColumnNames.size();
      rowColumnNames.add(definition.name());
      Integer indexedPrimaryKey = primaryKeyIndexByName.get(definition.name());
      int primaryKeyIndex = indexedPrimaryKey == null ? -1 : indexedPrimaryKey;
      plans.add(
          new UserColumnPlan(
              definition.name(),
              tupleIndex,
              rowIndex,
              primaryKeyIndex,
              decoderFor(definition)));
    }
    return new TableContext(
        TableContextKind.USER,
        schema.tableId(),
        schema,
        RowLayout.forColumns(List.copyOf(rowColumnNames)),
        RowLayout.forColumns(primaryKeyColumnNames),
        List.copyOf(plans),
        requiredTupleWidth);
  }

  private static String tableMapColumnsRequireFullDumpReason(
      TableSchema schema, String missingColumn, List<String> tableMapColumnNames) {
    return "MySQL table-map columns do not match the known selected-column schema for "
        + schema.tableId().displayName()
        + ": missing "
        + missingColumn
        + "; full dump required. This check runs as a runtime guard when TABLE_MAP metadata changes"
        + " after the startup preflight. To recover: verify the source schema and target state,"
        + " "
        + resetPersistedStateRecoveryStep("this table-map metadata")
        + ". observedTableMapColumns="
        + tableMapColumnNames;
  }

  private static String rowTupleRequiresFullDumpReason(
      TableContext context, int observedTupleWidth) {
    return "MySQL row tuple for "
        + context.tableId().displayName()
        + " is shorter than the selected-column shape from TABLE_MAP: observedTupleWidth="
        + observedTupleWidth
        + " requiredTupleWidth="
        + context.requiredTupleWidth()
        + "; full dump required. This indicates the live row image no longer matches the"
        + " selected-column schema DBLog is decoding. To recover: verify the source schema and"
        + " target state, "
        + resetPersistedStateRecoveryStep("this row tuple")
        + ".";
  }

  private static String primaryKeyUpdateRequiresFullDumpReason(TableId tableId) {
    return "Primary-key update observed for "
        + tableId.displayName()
        + "; primary-key update is unsupported in the current MySQL live session; full dump"
        + " required. DBLog represents UPDATE as a mutation of one stable row identity, so an"
        + " identity-changing update cannot be forwarded safely. To recover: stop DBLog, verify"
        + " the target matches the current source state or bootstrap the target, "
        + resetPersistedStateRecoveryStep("this primary-key update")
        + ".";
  }

  private static String resetPersistedStateRecoveryStep(String replayBoundary) {
    return "reset the persisted runtime state/checkpoint for this source (dblog.runtime.state-path"
        + " is an H2 file prefix; remove <state-path>.mv.db and any"
        + " <state-path>.trace.db/<state-path>.lock.db files, or use a new state path) so"
        + " restart does not resume before "
        + replayBoundary
        + ", then restart with that reset state and submit a fresh ALL_TABLES dump";
  }

  private static Map<String, Integer> indexByName(List<String> columnNames) {
    LinkedHashMap<String, Integer> indexed = new LinkedHashMap<>();
    for (int index = 0; index < columnNames.size(); index++) {
      indexed.put(columnNames.get(index), index);
    }
    return Map.copyOf(indexed);
  }

  private Optional<MySqlBinlogTransaction> commit(
      String commitTransactionId,
      MySqlSourcePosition checkpointPosition,
      Instant commitTimestamp) {
    Objects.requireNonNull(commitTransactionId, "commitTransactionId");
    Objects.requireNonNull(checkpointPosition, "checkpointPosition");
    Objects.requireNonNull(commitTimestamp, "commitTimestamp");
    if (!transactionOpen && currentEvents.isEmpty() && currentGtid == null && currentSyntheticTransactionId == null) {
      return Optional.empty();
    }
    if (currentEvents.isEmpty()) {
      reset();
      return Optional.empty();
    }
    String transactionId =
        currentGtid != null
            ? currentGtid
            : currentSyntheticTransactionId != null ? currentSyntheticTransactionId : commitTransactionId;
    MySqlBinlogTransaction transaction =
        new MySqlBinlogTransaction(
            transactionId,
            currentGtid,
            checkpointPosition,
            commitTimestamp,
            currentEvents);
    reset();
    return Optional.of(transaction);
  }

  private void beginGtid(String gtid) {
    if (transactionOpen && !currentEvents.isEmpty()) {
      throw new IllegalStateException(
          "MySQL GTID event arrived before the previous relevant transaction committed");
    }
    transactionOpen = true;
    currentGtid = gtid;
    currentSyntheticTransactionId = gtid;
  }

  private void beginExplicitTransaction(MySqlSourcePosition position) {
    transactionOpen = true;
    if (currentSyntheticTransactionId == null) {
      currentSyntheticTransactionId = syntheticTransactionId(position);
    }
  }

  private void openImplicitTransaction(MySqlSourcePosition position) {
    transactionOpen = true;
    if (currentSyntheticTransactionId == null) {
      currentSyntheticTransactionId = syntheticTransactionId(position);
    }
  }

  private String currentTransactionEventId(MySqlSourcePosition position) {
    if (currentGtid != null) {
      return currentGtid;
    }
    if (currentSyntheticTransactionId != null) {
      return currentSyntheticTransactionId;
    }
    return syntheticTransactionId(position);
  }

  private String syntheticTransactionId(MySqlSourcePosition position) {
    return "mysql-tx@" + position.displayValue();
  }

  private void rollback() {
    reset();
  }

  private void reset() {
    currentEvents.clear();
    currentGtid = null;
    currentSyntheticTransactionId = null;
    transactionOpen = false;
  }

  private static Map<TableKey, TableSchema> indexSchemas(List<TableSchema> capturedSchemas) {
    Map<TableKey, TableSchema> indexed = new LinkedHashMap<>();
    for (TableSchema schema : capturedSchemas == null ? List.<TableSchema>of() : capturedSchemas) {
      indexed.put(new TableKey(schema.tableId().schemaName(), schema.tableId().tableName()), schema);
    }
    return Map.copyOf(indexed);
  }

  private static String metadataDatabaseName(List<TableSchema> capturedSchemas, String fallback) {
    Objects.requireNonNull(fallback, "fallback");
    if (capturedSchemas != null && !capturedSchemas.isEmpty()) {
      return capturedSchemas.getFirst().tableId().databaseName();
    }
    return fallback;
  }

  private static String stringValue(Object value) {
    return value == null ? null : String.valueOf(value);
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private record TableContext(
      TableContextKind kind,
      TableId tableId,
      TableSchema schema,
      RowLayout rowLayout,
      RowLayout primaryKeyLayout,
      List<UserColumnPlan> userColumnPlans,
      int requiredTupleWidth) {
  }

  private record UserColumnPlan(
      String columnName,
      int tupleIndex,
      int rowIndex,
      int primaryKeyIndex,
      UserValueDecoder decoder) {}

  private record DecodedUserRow(
      ImmutableRowImage primaryKey,
      ImmutableRowImage row,
      PrimaryKeyHash primaryKeyHash) {}

  @FunctionalInterface
  private interface UserValueDecoder {
    Object decode(Object value);
  }

  private enum TableContextKind {
    USER,
    WATERMARK,
    HEARTBEAT,
    IGNORED
  }

  private record TableKey(String databaseName, String tableName) {}

  private static UserValueDecoder decoderFor(
      ColumnDefinition definition) {
    if (definition == null) {
      return SKIP_DECODER;
    }
    if (!definition.supported()) {
      return definition.primaryKey() ? RAW_VALUE_DECODER : SKIP_DECODER;
    }
    return switch (definition.neutralType()) {
      case BOOLEAN -> value -> value == null ? null : NeutralValueNormalizer.normalizeBooleanValue(value);
      case INTEGER -> value -> value == null ? null : NeutralValueNormalizer.normalizeIntegerValue(value);
      case FLOAT, DECIMAL ->
          value -> value == null ? null : NeutralValueNormalizer.normalizeDecimalValue(value);
      case STRING, ENUM_STRING, XML ->
          value -> value == null ? null : NeutralValueNormalizer.normalizeStringLikeValue(value);
      case JSON -> value ->
          value == null
              ? null
              : NeutralValueNormalizer.normalizeJsonValue(
                  MySqlValueDecoder.INSTANCE.decodeJson(value));
      case BINARY -> value -> value == null ? null : NeutralValueNormalizer.normalizeBinaryValue(value);
      case DATE -> value -> value == null ? null : NeutralValueNormalizer.normalizeDateValue(value);
      case TIME -> value -> value == null ? null : NeutralValueNormalizer.normalizeTimeValue(value);
      case TIMESTAMP ->
          value -> value == null ? null : NeutralValueNormalizer.normalizeTimestampValue(value);
      case UUID -> value -> value == null ? null : NeutralValueNormalizer.normalizeUuidValue(value);
      case UNSUPPORTED -> definition.primaryKey() ? RAW_VALUE_DECODER : SKIP_DECODER;
    };
  }
}
