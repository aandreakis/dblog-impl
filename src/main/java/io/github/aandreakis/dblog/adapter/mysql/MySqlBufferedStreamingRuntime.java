package io.github.aandreakis.dblog.adapter.mysql;

import io.github.aandreakis.dblog.adapter.api.ChunkReadWithWindowWork;
import io.github.aandreakis.dblog.adapter.api.ChunkReadWork;
import io.github.aandreakis.dblog.adapter.api.CommittedTransactionIngress;
import io.github.aandreakis.dblog.adapter.api.ConnectionBoundChunkReader;
import io.github.aandreakis.dblog.adapter.api.HeartbeatMetadataWriter;
import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.api.SourceSqlWork;
import io.github.aandreakis.dblog.adapter.api.WatermarkMetadataWriter;
import io.github.aandreakis.dblog.adapter.api.WatermarkWindowResult;
import io.github.aandreakis.dblog.adapter.api.WatermarkWindowRuntime;
import io.github.aandreakis.dblog.adapter.api.WatermarkWindowWork;
import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.WatermarkToken;
import io.github.aandreakis.dblog.core.reconcile.HeartbeatMetadata;
import io.github.aandreakis.dblog.core.reconcile.WatermarkMetadata;
import io.github.aandreakis.dblog.core.reconcile.WatermarkWindow;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.runtime.sql.AbstractQueueBackedRelationalRuntime;
import io.github.aandreakis.dblog.tap.NoopTap;
import io.github.aandreakis.dblog.tap.Tap;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Queue-backed MySQL runtime shell for the adapter.
 *
 * <p>It executes SQL work over a real JDBC connection, can accept externally supplied committed
 * MySQL transactions, and still synthesizes metadata transactions for watermark windows and
 * heartbeats until the full binlog session is ported.
 */
public final class MySqlBufferedStreamingRuntime
    extends AbstractQueueBackedRelationalRuntime<MySqlBinlogTransaction, MySqlSourcePosition>
    implements WatermarkWindowRuntime<MySqlBinlogTransaction>,
        CommittedTransactionIngress<MySqlBinlogTransaction> {
  private final String sourceId;
  private final String sourceStreamId;
  private final WatermarkMetadataWriter watermarkWriter;
  private final HeartbeatMetadataWriter heartbeatWriter;
  private final Tap tap;

  private long nextSyntheticPosition;
  private boolean metadataInitialized;
  private Instant nextHeartbeatDueAt;

  public MySqlBufferedStreamingRuntime(
      Connection sqlConnection,
      String sourceId,
      String sourceStreamId,
      List<TableSchema> capturedSchemas,
      WatermarkMetadataWriter watermarkWriter,
      HeartbeatMetadataWriter heartbeatWriter) {
    this(
        sqlConnection,
        sourceId,
        sourceStreamId,
        capturedSchemas,
        watermarkWriter,
        heartbeatWriter,
        checkpoint -> {},
        null,
        1024,
        NoopTap.INSTANCE);
  }

  public MySqlBufferedStreamingRuntime(
      Connection sqlConnection,
      String sourceId,
      String sourceStreamId,
      List<TableSchema> capturedSchemas,
      WatermarkMetadataWriter watermarkWriter,
      HeartbeatMetadataWriter heartbeatWriter,
      Consumer<MySqlSourcePosition> checkpointPersister,
      MySqlSourcePosition loadedCheckpoint) {
    this(
        sqlConnection,
        sourceId,
        sourceStreamId,
        capturedSchemas,
        watermarkWriter,
        heartbeatWriter,
        checkpointPersister,
        loadedCheckpoint,
        1024,
        NoopTap.INSTANCE);
  }

  public MySqlBufferedStreamingRuntime(
      Connection sqlConnection,
      String sourceId,
      String sourceStreamId,
      List<TableSchema> capturedSchemas,
      WatermarkMetadataWriter watermarkWriter,
      HeartbeatMetadataWriter heartbeatWriter,
      Consumer<MySqlSourcePosition> checkpointPersister,
      MySqlSourcePosition loadedCheckpoint,
      Tap tap) {
    this(
        sqlConnection,
        sourceId,
        sourceStreamId,
        capturedSchemas,
        watermarkWriter,
        heartbeatWriter,
        checkpointPersister,
        loadedCheckpoint,
        1024,
        tap);
  }

  public MySqlBufferedStreamingRuntime(
      Connection sqlConnection,
      String sourceId,
      String sourceStreamId,
      List<TableSchema> capturedSchemas,
      WatermarkMetadataWriter watermarkWriter,
      HeartbeatMetadataWriter heartbeatWriter,
      Consumer<MySqlSourcePosition> checkpointPersister,
      MySqlSourcePosition loadedCheckpoint,
      int queueCapacity,
      Tap tap) {
    super(
        sqlConnection,
        capturedSchemas,
        checkpointPersister,
        loadedCheckpoint,
        queueCapacity,
        "mysql");
    this.sourceId = requireNonBlank(sourceId, "sourceId");
    this.sourceStreamId = requireNonBlank(sourceStreamId, "sourceStreamId");
    this.watermarkWriter = Objects.requireNonNull(watermarkWriter, "watermarkWriter");
    this.heartbeatWriter = Objects.requireNonNull(heartbeatWriter, "heartbeatWriter");
    this.tap = Objects.requireNonNull(tap, "tap");
  }

  public void enqueueCommittedTransaction(MySqlBinlogTransaction transaction) {
    enqueueCommittedTransactionInternal(transaction);
  }

  public List<MySqlSourcePosition> acknowledgedPositions() {
    return acknowledgedPositionsInternal();
  }

  public <T> T executeSqlWork(SourceSqlWork<T> work)
      throws SQLException {
    return executeRuntimeSqlWork(work);
  }

  public <T> WatermarkWindowResult<T> executeWithinWatermarkWindow(WatermarkWindowWork<T> work)
      throws SQLException {
    Objects.requireNonNull(work, "work");
    ensureMetadataInitialized();
    WatermarkWindow window = new WatermarkWindow(WatermarkToken.random(), WatermarkToken.random());
    watermarkWriter.writeWatermark(sqlConnection(), runId(), window.low());
    tap.onWatermarkWritten(Tap.WatermarkLevel.LOW, window.low());
    T value = work.execute(sqlConnection(), window);
    watermarkWriter.writeWatermark(sqlConnection(), runId(), window.high());
    tap.onWatermarkWritten(Tap.WatermarkLevel.HIGH, window.high());
    enqueueSyntheticWatermarkTransaction(window);
    return new WatermarkWindowResult<>(value, window);
  }

  @Override
  public String currentRunId() {
    return runId();
  }

  @Override
  public <T> T executeChunkRead(
      SourceChunkReader reader,
      ChunkReadWork<T> work)
      throws SQLException {
    return executeSqlWork(
        connection ->
            work.execute(
                new ConnectionBoundChunkReader(
                    connection, reader)));
  }

  @Override
  public <T> WatermarkWindowResult<T> executeChunkReadInWatermarkWindow(
      SourceChunkReader reader,
      ChunkReadWithWindowWork<T> work)
      throws SQLException {
    return executeWithinWatermarkWindow(
        (connection, window) ->
            work.execute(
                new ConnectionBoundChunkReader(
                    connection, reader),
                window));
  }

  @Override
  public boolean emitHeartbeatIfDue(Instant heartbeatTime, Duration minimumInterval)
      throws SQLException {
    Objects.requireNonNull(heartbeatTime, "heartbeatTime");
    Duration requiredMinimumInterval = requirePositive(minimumInterval, "minimumInterval");
    if (nextHeartbeatDueAt != null && heartbeatTime.isBefore(nextHeartbeatDueAt)) {
      return false;
    }
    ensureMetadataInitialized();
    if (heartbeatWriter.writeHeartbeatIfDue(
        sqlConnection(), runId(), sourceStreamId, heartbeatTime, requiredMinimumInterval)) {
      nextHeartbeatDueAt = heartbeatTime.plus(requiredMinimumInterval);
      enqueueSyntheticHeartbeatTransaction(heartbeatTime);
      return true;
    }
    return false;
  }

  private void enqueueSyntheticWatermarkTransaction(WatermarkWindow window) {
    MySqlSourcePosition checkpoint = nextSyntheticCheckpoint("watermark");
    enqueueCommittedTransaction(
        new MySqlBinlogTransaction(
            "mysql-window-" + checkpoint.binlogPosition(),
            null,
            checkpoint,
            Instant.now(),
            List.of(
                watermarkEvent(window.low().value(), checkpoint),
                watermarkEvent(window.high().value(), checkpoint))));
  }

  private void enqueueSyntheticHeartbeatTransaction(Instant heartbeatTime) {
    MySqlSourcePosition checkpoint = nextSyntheticCheckpoint("heartbeat");
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put(HeartbeatMetadata.RUN_ID_COLUMN, runId());
    afterRow.put(HeartbeatMetadata.SOURCE_STREAM_ID_COLUMN, sourceStreamId);
    afterRow.put(HeartbeatMetadata.TIMESTAMP_COLUMN, heartbeatTime.toString());
    enqueueCommittedTransaction(
        new MySqlBinlogTransaction(
            "mysql-heartbeat-" + checkpoint.binlogPosition(),
            null,
            checkpoint,
            Instant.now(),
            List.of(
                new ChangeEvent(
                    HeartbeatMetadata.tableIdFor(metadataDatabaseName()),
                    OperationType.HEARTBEAT,
                    CaptureOrigin.LOG,
                    HeartbeatMetadata.singletonPrimaryKey(),
                    null,
                    ImmutableRowImage.of(afterRow),
                    checkpoint,
                    null,
                    null))));
  }

  private ChangeEvent watermarkEvent(String token, MySqlSourcePosition checkpoint) {
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put(WatermarkMetadata.RUN_ID_COLUMN, runId());
    afterRow.put(WatermarkMetadata.TOKEN_COLUMN, token);
    return new ChangeEvent(
        WatermarkMetadata.tableIdFor(metadataDatabaseName()),
        OperationType.WATERMARK,
        CaptureOrigin.LOG,
        WatermarkMetadata.singletonPrimaryKey(),
        null,
        ImmutableRowImage.of(afterRow),
        checkpoint,
        null,
        null);
  }

  private MySqlSourcePosition nextSyntheticCheckpoint(String tag) {
    long position = ++nextSyntheticPosition;
    return new MySqlSourcePosition("inspection-" + sourceId + "-" + tag, position, null);
  }

  private String metadataDatabaseName() {
    return capturedSchemas().isEmpty() ? sourceId : capturedSchemas().getFirst().tableId().databaseName();
  }

  @Override
  protected MySqlSourcePosition checkpointPosition(MySqlBinlogTransaction transaction) {
    return transaction.checkpointPosition();
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private void ensureMetadataInitialized() throws SQLException {
    if (metadataInitialized) {
      return;
    }
    watermarkWriter.ensureMetadataTable(sqlConnection());
    heartbeatWriter.ensureHeartbeatTable(sqlConnection());
    metadataInitialized = true;
  }

  private static Duration requirePositive(Duration value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isZero() || value.isNegative()) {
      throw new IllegalArgumentException(name + " must be > 0");
    }
    return value;
  }
}
