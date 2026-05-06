package io.github.aandreakis.dblog.adapter.postgres.internal;

import io.github.aandreakis.dblog.adapter.api.SourceRequiresFullDumpException;
import io.github.aandreakis.dblog.adapter.api.SourceSchemaUncertaintyException;
import io.github.aandreakis.dblog.adapter.postgres.PostgresLsn;
import io.github.aandreakis.dblog.adapter.postgres.PostgresPgoutputTransaction;
import io.github.aandreakis.dblog.adapter.postgres.PostgresSourceCheckpointStore;
import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.RowLayout;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.reconcile.HeartbeatMetadata;
import io.github.aandreakis.dblog.core.reconcile.WatermarkMetadata;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralValueNormalizer;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.runtime.sql.RuntimeStreamingSession;
import io.github.aandreakis.dblog.runtime.sql.SourceFlowControlSnapshot;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * First PostgreSQL pgoutput message session.
 *
 * <p>This intentionally covers the narrow core needed to turn committed pgoutput messages into
 * committed neutral transactions, including metadata watermark/heartbeat handling and durable
 * checkpoint advancement. Destructive pgoutput shapes such as captured-table TRUNCATE, runtime
 * replica-identity drift, selected-column relation metadata drift, key-only old tuples, and live
 * primary-key updates fail closed after recording a full-dump-required signal in the runtime state
 * store.
 */
public final class PostgresTransactionStreamingSession
    implements RuntimeStreamingSession<PostgresPgoutputTransaction> {
  private static final TupleValueDecoder RAW_TUPLE_DECODER = value -> value;
  private static final Logger log =
      LoggerFactory.getLogger(PostgresTransactionStreamingSession.class);

  private final String databaseName;
  private final String currentRunId;
  private final String currentSourceStreamId;
  private final String sourceId;
  private final Map<TableId, TableSchema> schemasByTableId;
  private final PostgresPgoutputStream stream;
  private final PostgresSourceCheckpointStore checkpointStore;
  private final PostgresPgoutputDecoder decoder = new PostgresPgoutputDecoder();
  private final Map<Integer, RelationContext> relationsById = new HashMap<>();
  /**
   * Tracks whether replication-slot feedback writes (applied/flush LSN + status updates) are
   * currently succeeding. Used to WARN only on health transitions so a persistently failing
   * slot produces one log line, not one per transaction. Failure does not halt the runtime
   * because the local durable checkpoint has already been persisted; however a feedback outage
   * that persists will prevent the Postgres server from releasing WAL segments, which can
   * eventually exhaust disk on the source. The transition log exists to give operators an
   * early signal.
   */
  private final AtomicBoolean slotFeedbackHealthy = new AtomicBoolean(true);

  private TransactionBuffer currentTransaction;
  private boolean ownHeartbeatObserved;

  public PostgresTransactionStreamingSession(
      String databaseName,
      String currentRunId,
      String currentSourceStreamId,
      String sourceId,
      List<TableSchema> capturedSchemas,
      PostgresPgoutputStream stream,
      PostgresSourceCheckpointStore checkpointStore) {
    this.databaseName = requireNonBlank(databaseName, "databaseName");
    this.currentRunId = requireNonBlank(currentRunId, "currentRunId");
    this.currentSourceStreamId = requireNonBlank(currentSourceStreamId, "currentSourceStreamId");
    this.sourceId = requireNonBlank(sourceId, "sourceId");
    this.schemasByTableId = new HashMap<>(indexSchemas(capturedSchemas));
    this.stream = Objects.requireNonNull(stream, "stream");
    this.checkpointStore = Objects.requireNonNull(checkpointStore, "checkpointStore");
  }

  @Override
  public Optional<PostgresPgoutputTransaction> readPendingTransaction() throws SQLException {
    try {
      while (true) {
        Optional<java.nio.ByteBuffer> nextMessage = stream.readPending();
        if (nextMessage.isEmpty()) {
          return Optional.empty();
        }
        Optional<PostgresPgoutputTransaction> transaction =
            handleDecoded(decoder.decode(nextMessage.orElseThrow()));
        if (transaction.isPresent()) {
          return transaction;
        }
      }
    } catch (SourceRequiresFullDumpException ex) {
      persistSignalFailureAsSuppressed(
          ex,
          () -> checkpointStore.saveFullDumpRequiredSignal(sourceId, ex.tableId(), ex.getMessage()));
      throw ex;
    } catch (SourceSchemaUncertaintyException ex) {
      persistSignalFailureAsSuppressed(
          ex,
          () -> checkpointStore.saveSchemaUncertaintySignal(sourceId, ex.tableId(), ex.getMessage()));
      throw ex;
    }
  }

  @Override
  public void acknowledge(PostgresPgoutputTransaction transaction) {
    Objects.requireNonNull(transaction, "transaction");
    Optional<PostgresLsn> currentCheckpoint = checkpointStore.load(sourceId);
    boolean advancesCheckpoint =
        currentCheckpoint.isEmpty()
            || transaction.checkpointLsn().compareTo(currentCheckpoint.orElseThrow()) > 0;
    PostgresLsn durableCheckpoint =
        advancesCheckpoint ? transaction.checkpointLsn() : currentCheckpoint.orElseThrow();
    if (advancesCheckpoint) {
      checkpointStore.save(sourceId, durableCheckpoint);
    }
    try {
      stream.setAppliedLsn(durableCheckpoint);
      stream.setFlushedLsn(durableCheckpoint);
      if (slotFeedbackHealthy.compareAndSet(false, true)) {
        log.info(
            "PostgreSQL replication slot feedback recovered sourceId={} lsn={}",
            sourceId,
            durableCheckpoint.displayValue());
      }
    } catch (SQLException feedbackFailure) {
      // Local durable checkpoint is already persisted, so crash recovery will resume correctly.
      // Persistent feedback failure is still dangerous because the server cannot release WAL
      // segments until it hears from us; log on transition so operators see the first signal
      // without spamming per transaction.
      if (slotFeedbackHealthy.compareAndSet(true, false)) {
        log.warn(
            "PostgreSQL replication slot feedback failed sourceId={} lsn={}; "
                + "durable checkpoint is persisted but slot WAL may accumulate on the server "
                + "until feedback recovers",
            sourceId,
            durableCheckpoint.displayValue(),
            feedbackFailure);
      }
    }
  }

  @Override
  public List<TableSchema> currentCapturedSchemas() {
    return List.copyOf(schemasByTableId.values());
  }

  @Override
  public void updateCapturedSchema(TableSchema schema) {
    Objects.requireNonNull(schema, "schema");
    schemasByTableId.put(schema.tableId(), schema);
    checkpointStore.saveObservedTableSchema(schema);
  }

  @Override
  public SourceFlowControlSnapshot sourceFlowControlSnapshot() {
    return SourceFlowControlSnapshot.directPoll();
  }

  @Override
  public void close() throws Exception {
    try {
      stream.forceUpdateStatus();
    } catch (SQLException shutdownFeedbackFailure) {
      // Best-effort final flush on shutdown: log at DEBUG so a diagnostic trace can see it,
      // but don't WARN (we're closing; the next process will re-establish feedback).
      log.debug(
          "Final slot feedback flush on shutdown failed sourceId={}",
          sourceId,
          shutdownFeedbackFailure);
    }
    stream.close();
  }

  private Optional<PostgresPgoutputTransaction> handleDecoded(PostgresPgoutputDecoder.DecodedMessage decoded)
      throws SQLException {
    if (decoded instanceof PostgresPgoutputDecoder.RelationMessage relationMessage) {
      relationsById.put(relationMessage.relationId(), relationContext(relationMessage));
      return Optional.empty();
    }
    if (decoded instanceof PostgresPgoutputDecoder.TypeMessage || decoded instanceof PostgresPgoutputDecoder.OriginMessage) {
      return Optional.empty();
    }
    if (decoded instanceof PostgresPgoutputDecoder.BeginMessage beginMessage) {
      beginTransaction(beginMessage);
      return Optional.empty();
    }
    if (decoded instanceof PostgresPgoutputDecoder.CommitMessage commitMessage) {
      TransactionBuffer transaction = requireActiveTransaction();
      PostgresLsn checkpointLsn = stream.lastReceiveLsn().orElse(commitMessage.endLsn());
      currentTransaction = null;
      if (transaction.events().isEmpty()) {
        return Optional.empty();
      }
      return Optional.of(transaction.commit(commitMessage, checkpointLsn));
    }
    if (decoded instanceof PostgresPgoutputDecoder.InsertMessage insertMessage) {
      appendChange(decodeInsert(insertMessage));
      return Optional.empty();
    }
    if (decoded instanceof PostgresPgoutputDecoder.UpdateMessage updateMessage) {
      appendChange(decodeUpdate(updateMessage));
      return Optional.empty();
    }
    if (decoded instanceof PostgresPgoutputDecoder.DeleteMessage deleteMessage) {
      appendChange(decodeDelete(deleteMessage));
      return Optional.empty();
    }
    if (decoded instanceof PostgresPgoutputDecoder.TruncateMessage truncateMessage) {
      handleTruncate(truncateMessage);
      return Optional.empty();
    }
    throw new IllegalStateException(
        "Unsupported pgoutput message in the current next PostgreSQL session: "
            + decoded.getClass().getSimpleName());
  }

  private void handleTruncate(PostgresPgoutputDecoder.TruncateMessage truncateMessage) {
    for (Integer relationId : truncateMessage.relationIds()) {
      RelationContext context = requireRelation(relationId);
      if (context.kind() == RelationKind.IGNORED) {
        continue;
      }
      String reason = truncateRequiresFullDumpReason(context.tableId(), truncateMessage);
      if (context.kind() == RelationKind.USER) {
        throw SourceRequiresFullDumpException.forTable(context.tableId(), reason);
      }
      throw SourceRequiresFullDumpException.sourceLevel(reason);
    }
  }

  private static String truncateRequiresFullDumpReason(
      TableId tableId, PostgresPgoutputDecoder.TruncateMessage truncateMessage) {
    return "PostgreSQL pgoutput TRUNCATE cannot be represented as row-level DELETE events for "
        + tableId.displayName()
        + "; full dump required. To recover: stop DBLog, verify the target matches the current"
        + " source state or bootstrap the target, "
        + resetPersistedStateRecoveryStep("this TRUNCATE")
        + ". relationIds="
        + truncateMessage.relationIds();
  }

  private void appendChange(Optional<ChangeEvent> change) {
    change.ifPresent(event -> requireActiveTransaction().events().add(event));
  }

  private Optional<ChangeEvent> decodeInsert(PostgresPgoutputDecoder.InsertMessage insertMessage) {
    RelationContext context = requireRelation(insertMessage.relationId());
    if (context.kind() == RelationKind.WATERMARK) {
      return decodeWatermark(context, insertMessage.newTuple(), null);
    }
    if (context.kind() == RelationKind.HEARTBEAT) {
      return decodeHeartbeat(context, insertMessage.newTuple(), null);
    }
    if (context.kind() == RelationKind.IGNORED) {
      return Optional.empty();
    }
    ImmutableRowImage afterRow = decodeUserTuple(context, insertMessage.newTuple());
    ImmutableRowImage primaryKey = context.schema().primaryKeyRow(afterRow);
    return Optional.of(
        new ChangeEvent(
            context.tableId(),
            OperationType.INSERT,
            CaptureOrigin.LOG,
            primaryKey,
            null,
            afterRow,
            currentCheckpointPosition(),
            requireActiveTransaction().transactionId(),
            null,
            context.schema().primaryKeyHashFor(primaryKey)));
  }

  private Optional<ChangeEvent> decodeUpdate(PostgresPgoutputDecoder.UpdateMessage updateMessage) {
    RelationContext context = requireRelation(updateMessage.relationId());
    if (context.kind() == RelationKind.WATERMARK) {
      return decodeWatermark(context, updateMessage.newTuple(), updateMessage.oldTuple());
    }
    if (context.kind() == RelationKind.HEARTBEAT) {
      return decodeHeartbeat(context, updateMessage.newTuple(), updateMessage.oldTuple());
    }
    if (context.kind() == RelationKind.IGNORED) {
      return Optional.empty();
    }
    if (updateMessage.oldTuple() == null || updateMessage.oldTupleIsKey()) {
      throw SourceRequiresFullDumpException.forTable(
          context.tableId(), oldTupleRequiresFullDumpReason(context.tableId(), "UPDATE"));
    }
    ImmutableRowImage beforeRow = decodeUserTuple(context, updateMessage.oldTuple());
    ImmutableRowImage afterRow = decodeUserTuple(context, updateMessage.newTuple());
    ImmutableRowImage beforePrimaryKey = context.schema().primaryKeyRow(beforeRow);
    ImmutableRowImage afterPrimaryKey = context.schema().primaryKeyRow(afterRow);
    if (!beforePrimaryKey.equals(afterPrimaryKey)) {
      throw SourceRequiresFullDumpException.forTable(
          context.tableId(), primaryKeyUpdateRequiresFullDumpReason(context.tableId()));
    }
    return Optional.of(
        new ChangeEvent(
            context.tableId(),
            OperationType.UPDATE,
            CaptureOrigin.LOG,
            beforePrimaryKey,
            beforeRow,
            afterRow,
            currentCheckpointPosition(),
            requireActiveTransaction().transactionId(),
            null,
            context.schema().primaryKeyHashFor(beforePrimaryKey)));
  }

  private Optional<ChangeEvent> decodeDelete(PostgresPgoutputDecoder.DeleteMessage deleteMessage) {
    RelationContext context = requireRelation(deleteMessage.relationId());
    if (context.kind() == RelationKind.IGNORED) {
      return Optional.empty();
    }
    if (context.kind() != RelationKind.USER) {
      throw SourceSchemaUncertaintyException.forTable(
          context.tableId(),
          "Metadata tables must not emit DELETE events in the next PostgreSQL session: "
              + context.tableId().displayName());
    }
    if (deleteMessage.oldTuple() == null || deleteMessage.oldTupleIsKey()) {
      throw SourceRequiresFullDumpException.forTable(
          context.tableId(), oldTupleRequiresFullDumpReason(context.tableId(), "DELETE"));
    }
    ImmutableRowImage beforeRow = decodeUserTuple(context, deleteMessage.oldTuple());
    ImmutableRowImage primaryKey = context.schema().primaryKeyRow(beforeRow);
    return Optional.of(
        new ChangeEvent(
            context.tableId(),
            OperationType.DELETE,
            CaptureOrigin.LOG,
            primaryKey,
            beforeRow,
            null,
            currentCheckpointPosition(),
            requireActiveTransaction().transactionId(),
            null,
            context.schema().primaryKeyHashFor(primaryKey)));
  }

  private Optional<ChangeEvent> decodeWatermark(
      RelationContext context,
      PostgresPgoutputDecoder.TupleData newTuple,
      PostgresPgoutputDecoder.TupleData oldTuple) {
    ImmutableRowImage afterRow = decodeMetadataTuple(context, newTuple, RelationKind.WATERMARK, true);
    if (!currentRunId.equals(afterRow.get(WatermarkMetadata.RUN_ID_COLUMN))) {
      return Optional.empty();
    }
    ImmutableRowImage beforeRow =
        oldTuple == null
            ? null
            : decodeMetadataTuple(context, oldTuple, RelationKind.WATERMARK, false);
    return Optional.of(
        new ChangeEvent(
            WatermarkMetadata.tableIdFor(databaseName),
            OperationType.WATERMARK,
            CaptureOrigin.LOG,
            WatermarkMetadata.singletonPrimaryKey(),
            beforeRow,
            afterRow,
            currentCheckpointPosition(),
            requireActiveTransaction().transactionId(),
            null));
  }

  private Optional<ChangeEvent> decodeHeartbeat(
      RelationContext context,
      PostgresPgoutputDecoder.TupleData newTuple,
      PostgresPgoutputDecoder.TupleData oldTuple) {
    ImmutableRowImage afterRow = decodeMetadataTuple(context, newTuple, RelationKind.HEARTBEAT, true);
    if (!shouldSurfaceHeartbeat(afterRow)) {
      return Optional.empty();
    }
    ImmutableRowImage beforeRow =
        oldTuple == null
            ? null
            : decodeMetadataTuple(context, oldTuple, RelationKind.HEARTBEAT, false);
    return Optional.of(
        new ChangeEvent(
            HeartbeatMetadata.tableIdFor(databaseName),
            OperationType.HEARTBEAT,
            CaptureOrigin.LOG,
            HeartbeatMetadata.singletonPrimaryKey(),
            beforeRow,
            afterRow,
            currentCheckpointPosition(),
            requireActiveTransaction().transactionId(),
            null));
  }

  private ImmutableRowImage decodeMetadataTuple(
      RelationContext context,
      PostgresPgoutputDecoder.TupleData tuple,
      RelationKind kind,
      boolean requireNonBlankMetadataValue) {
    MetadataTuplePlan metadataPlan = context.metadataTuplePlan();
    List<String> tupleValues;
    try {
      tupleValues = requireTupleValues(tuple, context);
    } catch (IllegalStateException ex) {
      throw SourceSchemaUncertaintyException.forTable(context.tableId(), ex.getMessage(), ex);
    }
    String idValue = metadataPlan.valueAt(tupleValues, metadataPlan.idIndex());
    if (!"1".equals(idValue)) {
      throw SourceSchemaUncertaintyException.forTable(
          context.tableId(),
          "PostgreSQL metadata tuple targeted a non-singleton primary key: " + idValue);
    }
    if (kind == RelationKind.WATERMARK) {
      String token = metadataPlan.valueAt(tupleValues, metadataPlan.tokenIndex());
      if (requireNonBlankMetadataValue && (token == null || token.isBlank())) {
        throw SourceSchemaUncertaintyException.forTable(
            context.tableId(),
            "PostgreSQL watermark metadata changes must carry a non-blank token value");
      }
      return ImmutableRowImage.ofLayout(
          MetadataTuplePlan.WATERMARK_OUTPUT_LAYOUT,
          new Object[] {
            metadataPlan.valueAt(tupleValues, metadataPlan.runIdIndex()),
            token
          });
    }
    String timestamp = metadataPlan.valueAt(tupleValues, metadataPlan.timestampIndex());
    if (!requireNonBlankMetadataValue && (timestamp == null || timestamp.isBlank())) {
      return ImmutableRowImage.ofLayout(
          MetadataTuplePlan.HEARTBEAT_OUTPUT_LAYOUT,
          new Object[] {
            metadataPlan.valueAt(tupleValues, metadataPlan.runIdIndex()),
            metadataPlan.valueAt(tupleValues, metadataPlan.sourceStreamIdIndex()),
            null
          });
    }
    if (timestamp == null || timestamp.isBlank()) {
      throw SourceSchemaUncertaintyException.forTable(
          context.tableId(),
          "PostgreSQL heartbeat metadata changes must carry a non-blank heartbeat timestamp");
    }
    Instant normalizedTimestamp;
    try {
      normalizedTimestamp = parseHeartbeatTimestamp(timestamp);
    } catch (Exception ex) {
      throw SourceSchemaUncertaintyException.forTable(
          context.tableId(),
          "PostgreSQL heartbeat metadata changes must carry a parseable heartbeat timestamp: "
              + timestamp,
          ex);
    }
    return ImmutableRowImage.ofLayout(
        MetadataTuplePlan.HEARTBEAT_OUTPUT_LAYOUT,
        new Object[] {
          metadataPlan.valueAt(tupleValues, metadataPlan.runIdIndex()),
          metadataPlan.valueAt(tupleValues, metadataPlan.sourceStreamIdIndex()),
          normalizedTimestamp
        });
  }

  private Instant parseHeartbeatTimestamp(String value) {
    try {
      return Instant.parse(value);
    } catch (DateTimeParseException ignored) {
      String normalized = value.replace(' ', 'T');
      if (normalized.matches(".*[+-]\\d{2}$")) {
        normalized = normalized + ":00";
      } else if (normalized.matches(".*[+-]\\d{4}$")) {
        normalized =
            normalized.substring(0, normalized.length() - 2)
                + ":"
                + normalized.substring(normalized.length() - 2);
      }
      return OffsetDateTime.parse(normalized).toInstant();
    }
  }

  private boolean shouldSurfaceHeartbeat(ImmutableRowImage afterRow) {
    String observedRunId = stringValue(afterRow.get(HeartbeatMetadata.RUN_ID_COLUMN));
    String observedSourceStreamId =
        stringValue(afterRow.get(HeartbeatMetadata.SOURCE_STREAM_ID_COLUMN));
    if (currentRunId.equals(observedRunId)) {
      if (observedSourceStreamId != null
          && !currentSourceStreamId.equals(observedSourceStreamId)) {
        throw SourceSchemaUncertaintyException.forTable(
            HeartbeatMetadata.tableIdFor(databaseName),
            "PostgreSQL observed its own heartbeat run_id on an unexpected source stream. runId="
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
          HeartbeatMetadata.tableIdFor(databaseName),
          "PostgreSQL detected another DBLog run writing heartbeats on the same source stream after confirming its own heartbeat. currentRunId="
              + currentRunId
              + " foreignRunId="
              + observedRunId
              + " sourceStreamId="
              + currentSourceStreamId);
    }
    return false;
  }

  private ImmutableRowImage decodeUserTuple(
      RelationContext context, PostgresPgoutputDecoder.TupleData tuple) {
    List<String> tupleValues = requireTupleValues(tuple, context);
    Object[] values = new Object[context.userColumnPlans().size()];
    for (int index = 0; index < context.userColumnPlans().size(); index++) {
      UserColumnPlan plan = context.userColumnPlans().get(index);
      values[index] = plan.decoder().decode(tupleValues.get(plan.tupleIndex()));
    }
    return ImmutableRowImage.ofLayout(context.userRowLayout(), values);
  }

  private List<String> requireTupleValues(
      PostgresPgoutputDecoder.TupleData tuple, RelationContext context) {
    if (tuple == null) {
      throw new IllegalStateException("pgoutput tuple must not be null");
    }
    if (tuple.values().size() != context.columnNames().size()) {
      throw new IllegalStateException(
          "pgoutput relation columns do not match the known table schema for "
              + context.tableId().displayName());
    }
    return tuple.values();
  }

  private RelationContext requireRelation(int relationId) {
    RelationContext context = relationsById.get(relationId);
    if (context == null) {
      throw new IllegalStateException(
          "pgoutput relation metadata is missing for relationId=" + relationId);
    }
    return context;
  }

  private RelationContext relationContext(PostgresPgoutputDecoder.RelationMessage relationMessage) {
    if (WatermarkMetadata.SCHEMA_NAME.equalsIgnoreCase(relationMessage.namespace())
        && WatermarkMetadata.TABLE_NAME.equalsIgnoreCase(relationMessage.relationName())) {
      List<String> columnNames =
          relationMessage.columns().stream().map(PostgresPgoutputDecoder.RelationColumn::name).toList();
      return new RelationContext(
          RelationKind.WATERMARK,
          WatermarkMetadata.tableIdFor(databaseName),
          null,
          columnNames,
          List.of(),
          null,
          MetadataTuplePlan.forWatermark(columnNames));
    }
    if (HeartbeatMetadata.SCHEMA_NAME.equalsIgnoreCase(relationMessage.namespace())
        && HeartbeatMetadata.TABLE_NAME.equalsIgnoreCase(relationMessage.relationName())) {
      List<String> columnNames =
          relationMessage.columns().stream().map(PostgresPgoutputDecoder.RelationColumn::name).toList();
      return new RelationContext(
          RelationKind.HEARTBEAT,
          HeartbeatMetadata.tableIdFor(databaseName),
          null,
          columnNames,
          List.of(),
          null,
          MetadataTuplePlan.forHeartbeat(columnNames));
    }
    TableSchema schema =
        schemasByTableId.get(new TableId(databaseName, relationMessage.namespace(), relationMessage.relationName()));
    if (schema == null) {
      return new RelationContext(
          RelationKind.IGNORED, null, null, List.of(), List.of(), null, null);
    }
    if (relationMessage.replicaIdentity() != 'f') {
      throw SourceRequiresFullDumpException.forTable(
          schema.tableId(), replicaIdentityRequiresFullDumpReason(schema, relationMessage));
    }
    List<String> columnNames =
        relationMessage.columns().stream().map(PostgresPgoutputDecoder.RelationColumn::name).toList();
    List<UserColumnPlan> userColumnPlans = buildUserColumnPlans(schema, columnNames);
    List<String> userOutputColumnNames =
        userColumnPlans.stream().map(UserColumnPlan::columnName).toList();
    return new RelationContext(
        RelationKind.USER,
        schema.tableId(),
        schema,
        columnNames,
        userColumnPlans,
        RowLayout.forColumns(userOutputColumnNames),
        null);
  }

  private static List<UserColumnPlan> buildUserColumnPlans(
      TableSchema schema, List<String> relationColumnNames) {
    Map<String, Integer> tupleIndexByName = new LinkedHashMap<>();
    for (int index = 0; index < relationColumnNames.size(); index++) {
      tupleIndexByName.put(relationColumnNames.get(index), index);
    }
    List<UserColumnPlan> plans = new ArrayList<>();
    for (ColumnDefinition column : schema.selectedColumns()) {
      Integer tupleIndex = tupleIndexByName.get(column.name());
      if (tupleIndex == null) {
        throw SourceRequiresFullDumpException.forTable(
            schema.tableId(),
            relationColumnsRequireFullDumpReason(schema, column.name(), relationColumnNames));
      }
      plans.add(new UserColumnPlan(column.name(), tupleIndex, decoderFor(column)));
    }
    return List.copyOf(plans);
  }

  private static String relationColumnsRequireFullDumpReason(
      TableSchema schema, String missingColumn, List<String> relationColumnNames) {
    return "PostgreSQL pgoutput relation columns do not match the known selected-column schema for "
        + schema.tableId().displayName()
        + ": missing "
        + missingColumn
        + "; full dump required. This check runs as a runtime guard when relation metadata changes"
        + " after the startup preflight. To recover: verify the source schema and target state,"
        + " "
        + resetPersistedStateRecoveryStep("this relation metadata")
        + ". observedRelationColumns="
        + relationColumnNames;
  }

  private static String replicaIdentityRequiresFullDumpReason(
      TableSchema schema, PostgresPgoutputDecoder.RelationMessage relationMessage) {
    return "Captured PostgreSQL tables must use REPLICA IDENTITY FULL, but "
        + schema.tableId().displayName()
        + " uses '"
        + relationMessage.replicaIdentity()
        + "' (d=default, n=nothing, f=full, i=index); full dump required. This check runs as"
        + " a runtime guard if replica identity was changed after the startup preflight. To"
        + " recover: run \"ALTER TABLE "
        + schema.tableId().displayName()
        + " REPLICA IDENTITY FULL\" on the source, verify the target matches the current"
        + " source state or bootstrap the target, "
        + resetPersistedStateRecoveryStep("this relation metadata")
        + ".";
  }

  private static String primaryKeyUpdateRequiresFullDumpReason(TableId tableId) {
    return "Primary-key update observed for "
        + tableId.displayName()
        + "; primary-key update is unsupported in the current PostgreSQL pgoutput session; full"
        + " dump required. DBLog represents UPDATE as a mutation of one stable row identity, so"
        + " an identity-changing update cannot be forwarded safely. To recover: stop DBLog,"
        + " verify the target matches the current source state or bootstrap the target, "
        + resetPersistedStateRecoveryStep("this primary-key update")
        + ".";
  }

  private static String oldTupleRequiresFullDumpReason(TableId tableId, String operation) {
    return "PostgreSQL pgoutput "
        + operation
        + " for "
        + tableId.displayName()
        + " did not include the full old tuple. DBLog requires full old tuples under REPLICA"
        + " IDENTITY FULL so it can emit selected-column before-row images and detect"
        + " primary-key changes; key-only or missing old tuple observed; full dump required."
        + " To recover: ensure the captured table uses REPLICA IDENTITY FULL, verify the target"
        + " matches the current source state or bootstrap the target, "
        + resetPersistedStateRecoveryStep("this " + operation)
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

  private void beginTransaction(PostgresPgoutputDecoder.BeginMessage beginMessage) {
    if (currentTransaction != null) {
      throw new IllegalStateException(
          "Received a pgoutput BEGIN while another transaction was still active");
    }
    currentTransaction =
        new TransactionBuffer(Integer.toString(beginMessage.transactionId()), beginMessage.finalLsn());
  }

  private TransactionBuffer requireActiveTransaction() {
    if (currentTransaction == null) {
      throw new IllegalStateException("pgoutput message arrived without an active transaction");
    }
    return currentTransaction;
  }

  private PostgresLsn currentCheckpointPosition() {
    return streamLastReceiveLsn().orElseGet(() -> requireActiveTransaction().beginFinalLsn());
  }

  private Optional<PostgresLsn> streamLastReceiveLsn() {
    try {
      return stream.lastReceiveLsn();
    } catch (SQLException ex) {
      throw new IllegalStateException("Failed to read pgoutput receive LSN", ex);
    }
  }

  private static Map<TableId, TableSchema> indexSchemas(List<TableSchema> schemas) {
    Map<TableId, TableSchema> indexed = new LinkedHashMap<>();
    for (TableSchema schema : schemas == null ? List.<TableSchema>of() : schemas) {
      indexed.put(schema.tableId(), schema);
    }
    return Map.copyOf(indexed);
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

  private static void persistSignalFailureAsSuppressed(
      IllegalStateException signalFailure, Runnable persistSignal) {
    try {
      persistSignal.run();
    } catch (RuntimeException | Error saveFailure) {
      signalFailure.addSuppressed(saveFailure);
    }
  }

  // buildUserOutputIndexByName no longer used; layout is built via RowLayout.forColumns.
  private record RelationContext(
      RelationKind kind,
      TableId tableId,
      TableSchema schema,
      List<String> columnNames,
      List<UserColumnPlan> userColumnPlans,
      RowLayout userRowLayout,
      MetadataTuplePlan metadataTuplePlan) {}

  private record UserColumnPlan(String columnName, int tupleIndex, TupleValueDecoder decoder) {}

  @FunctionalInterface
  private interface TupleValueDecoder {
    Object decode(String value);
  }

  private record MetadataTuplePlan(
      int idIndex, int runIdIndex, int sourceStreamIdIndex, int tokenIndex, int timestampIndex) {
    private static final List<String> WATERMARK_OUTPUT_COLUMNS =
        List.of(WatermarkMetadata.RUN_ID_COLUMN, WatermarkMetadata.TOKEN_COLUMN);
    private static final Map<String, Integer> WATERMARK_OUTPUT_INDEX =
        Map.of(WatermarkMetadata.RUN_ID_COLUMN, 0, WatermarkMetadata.TOKEN_COLUMN, 1);
    private static final RowLayout WATERMARK_OUTPUT_LAYOUT =
        new RowLayout(
            WATERMARK_OUTPUT_COLUMNS, WATERMARK_OUTPUT_INDEX);
    private static final List<String> HEARTBEAT_OUTPUT_COLUMNS =
        List.of(
            HeartbeatMetadata.RUN_ID_COLUMN,
            HeartbeatMetadata.SOURCE_STREAM_ID_COLUMN,
            HeartbeatMetadata.TIMESTAMP_COLUMN);
    private static final Map<String, Integer> HEARTBEAT_OUTPUT_INDEX =
        Map.of(
            HeartbeatMetadata.RUN_ID_COLUMN,
            0,
            HeartbeatMetadata.SOURCE_STREAM_ID_COLUMN,
            1,
            HeartbeatMetadata.TIMESTAMP_COLUMN,
            2);
    private static final RowLayout HEARTBEAT_OUTPUT_LAYOUT =
        new RowLayout(
            HEARTBEAT_OUTPUT_COLUMNS, HEARTBEAT_OUTPUT_INDEX);

    private static MetadataTuplePlan forWatermark(List<String> columnNames) {
      return new MetadataTuplePlan(
          requireIndex(columnNames, "id"),
          requireIndex(columnNames, WatermarkMetadata.RUN_ID_COLUMN),
          -1,
          requireIndex(columnNames, WatermarkMetadata.TOKEN_COLUMN),
          -1);
    }

    private static MetadataTuplePlan forHeartbeat(List<String> columnNames) {
      return new MetadataTuplePlan(
          requireIndex(columnNames, "id"),
          requireIndex(columnNames, HeartbeatMetadata.RUN_ID_COLUMN),
          optionalIndex(columnNames, HeartbeatMetadata.SOURCE_STREAM_ID_COLUMN),
          -1,
          requireIndex(columnNames, HeartbeatMetadata.TIMESTAMP_COLUMN));
    }

    private String valueAt(List<String> tupleValues, int index) {
      return index < 0 ? null : tupleValues.get(index);
    }

    private static int requireIndex(List<String> columnNames, String columnName) {
      int index = columnNames.indexOf(columnName);
      if (index < 0) {
        throw new IllegalStateException("pgoutput metadata relation is missing column " + columnName);
      }
      return index;
    }

    private static int optionalIndex(List<String> columnNames, String columnName) {
      return columnNames.indexOf(columnName);
    }
  }

  private enum RelationKind {
    USER,
    WATERMARK,
    HEARTBEAT,
    IGNORED
  }

  private record TransactionBuffer(
      String transactionId, PostgresLsn beginFinalLsn, List<ChangeEvent> events) {
    private TransactionBuffer(String transactionId, PostgresLsn beginFinalLsn) {
      this(transactionId, beginFinalLsn, new ArrayList<>());
    }

    private PostgresPgoutputTransaction commit(
        PostgresPgoutputDecoder.CommitMessage commitMessage, PostgresLsn checkpointLsn) {
      return new PostgresPgoutputTransaction(
          transactionId,
          beginFinalLsn,
          commitMessage.commitLsn(),
          commitMessage.endLsn(),
          checkpointLsn,
          commitMessage.commitTimestamp(),
          events);
      }
  }

  private static TupleValueDecoder decoderFor(ColumnDefinition definition) {
    if (!definition.supported()) {
      return definition.primaryKey() ? RAW_TUPLE_DECODER : value -> null;
    }
    return switch (definition.neutralType()) {
      case BOOLEAN -> value -> value == null ? null : NeutralValueNormalizer.normalizeBooleanValue(value);
      case INTEGER -> value -> value == null ? null : NeutralValueNormalizer.normalizeIntegerValue(value);
      case FLOAT, DECIMAL ->
          value -> value == null ? null : NeutralValueNormalizer.normalizeDecimalValue(value);
      case STRING, ENUM_STRING, XML ->
          value -> value == null ? null : NeutralValueNormalizer.normalizeStringLikeValue(value);
      case JSON -> value -> value == null ? null : NeutralValueNormalizer.normalizeJsonValue(value);
      case BINARY -> value -> value == null ? null : NeutralValueNormalizer.normalizeBinaryValue(value);
      case DATE -> value -> value == null ? null : NeutralValueNormalizer.normalizeDateValue(value);
      case TIME -> value -> value == null ? null : NeutralValueNormalizer.normalizeTimeValue(value);
      case TIMESTAMP ->
          value -> value == null ? null : NeutralValueNormalizer.normalizeTimestampValue(value);
      case UUID -> value -> value == null ? null : NeutralValueNormalizer.normalizeUuidValue(value);
      case UNSUPPORTED -> definition.primaryKey() ? RAW_TUPLE_DECODER : value -> null;
    };
  }
}
