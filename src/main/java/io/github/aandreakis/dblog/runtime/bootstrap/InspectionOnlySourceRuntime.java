package io.github.aandreakis.dblog.runtime.bootstrap;

import io.github.aandreakis.dblog.adapter.api.ChunkReadWithWindowWork;
import io.github.aandreakis.dblog.adapter.api.ChunkReadWork;
import io.github.aandreakis.dblog.adapter.api.CommittedTransactionIngress;
import io.github.aandreakis.dblog.adapter.api.ConnectionBoundChunkReader;
import io.github.aandreakis.dblog.adapter.api.HeartbeatMetadataWriter;
import io.github.aandreakis.dblog.adapter.api.RuntimeStatusInspectable;
import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.api.SourceSqlWork;
import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.adapter.api.WatermarkMetadataWriter;
import io.github.aandreakis.dblog.adapter.api.WatermarkWindowResult;
import io.github.aandreakis.dblog.adapter.api.WatermarkWindowRuntime;
import io.github.aandreakis.dblog.adapter.api.WatermarkWindowWork;
import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.SourcePosition;
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

/**
 * Minimal runtime shell used while adapters have real schema/chunk paths implemented but the
 * change-stream side is still being migrated.
 *
 * <p>It executes SQL work against a real JDBC connection and synthesizes low/high watermark
 * transactions so the shared dump/repair coordinators can run end to end in an inspection mode.
 */
public final class InspectionOnlySourceRuntime
    extends AbstractQueueBackedRelationalRuntime<
        InspectionOnlySourceRuntime.InspectionOnlyTransaction,
        InspectionOnlySourceRuntime.InspectionCheckpointPosition>
    implements WatermarkWindowRuntime<InspectionOnlySourceRuntime.InspectionOnlyTransaction>,
        CommittedTransactionIngress<InspectionOnlySourceRuntime.InspectionOnlyTransaction>,
        RuntimeStatusInspectable {
  private final String adapterLabel;
  private final String watermarkDatabaseName;
  private final String sourceStreamId;
  private final WatermarkMetadataWriter watermarkWriter;
  private final HeartbeatMetadataWriter heartbeatWriter;
  private final Tap tap;

  private long nextCheckpointOrdinal;
  private boolean metadataInitialized;
  private Instant nextHeartbeatDueAt;

  public InspectionOnlySourceRuntime(
      String adapterLabel, Connection sqlConnection, List<TableSchema> capturedSchemas) {
    this(
        adapterLabel,
        sqlConnection,
        capturedSchemas,
        adapterLabel.toLowerCase(),
        null,
        null,
        1024,
        NoopTap.INSTANCE);
  }

  public InspectionOnlySourceRuntime(
      String adapterLabel,
      Connection sqlConnection,
      List<TableSchema> capturedSchemas,
      String sourceStreamId,
      WatermarkMetadataWriter watermarkWriter,
      HeartbeatMetadataWriter heartbeatWriter) {
    this(
        adapterLabel,
        sqlConnection,
        capturedSchemas,
        sourceStreamId,
        watermarkWriter,
        heartbeatWriter,
        1024,
        NoopTap.INSTANCE);
  }

  public InspectionOnlySourceRuntime(
      String adapterLabel,
      Connection sqlConnection,
      List<TableSchema> capturedSchemas,
      String sourceStreamId,
      WatermarkMetadataWriter watermarkWriter,
      HeartbeatMetadataWriter heartbeatWriter,
      Tap tap) {
    this(
        adapterLabel,
        sqlConnection,
        capturedSchemas,
        sourceStreamId,
        watermarkWriter,
        heartbeatWriter,
        1024,
        tap);
  }

  public InspectionOnlySourceRuntime(
      String adapterLabel,
      Connection sqlConnection,
      List<TableSchema> capturedSchemas,
      String sourceStreamId,
      WatermarkMetadataWriter watermarkWriter,
      HeartbeatMetadataWriter heartbeatWriter,
      int queueCapacity,
      Tap tap) {
    super(
        sqlConnection,
        capturedSchemas,
        checkpoint -> {},
        null,
        queueCapacity,
        "inspection");
    this.adapterLabel = requireNonBlank(adapterLabel, "adapterLabel");
    this.watermarkDatabaseName =
        capturedSchemas().isEmpty()
            ? "inspection"
            : capturedSchemas().getFirst().tableId().databaseName();
    this.sourceStreamId = requireNonBlank(sourceStreamId, "sourceStreamId");
    this.watermarkWriter = watermarkWriter;
    this.heartbeatWriter = heartbeatWriter;
    this.tap = Objects.requireNonNull(tap, "tap");
  }

  public String adapterLabel() {
    return adapterLabel;
  }

  @Override
  public void enqueueCommittedTransaction(InspectionOnlyTransaction transaction) {
    enqueueCommittedTransactionInternal(transaction);
  }

  public <T> T executeSqlWork(SourceSqlWork<T> work) throws SQLException {
    return executeRuntimeSqlWork(work);
  }

  public <T> WatermarkWindowResult<T> executeWithinWatermarkWindow(WatermarkWindowWork<T> work)
      throws SQLException {
    Objects.requireNonNull(work, "work");
    WatermarkWindow window = new WatermarkWindow(WatermarkToken.random(), WatermarkToken.random());
    ensureMetadataTablesIfConfigured();
    if (watermarkWriter != null) {
      watermarkWriter.writeWatermark(sqlConnection(), runId(), window.low());
    }
    tap.onWatermarkWritten(Tap.WatermarkLevel.LOW, window.low());
    T value = work.execute(sqlConnection(), window);
    if (watermarkWriter != null) {
      watermarkWriter.writeWatermark(sqlConnection(), runId(), window.high());
    }
    tap.onWatermarkWritten(Tap.WatermarkLevel.HIGH, window.high());
    enqueueSyntheticWatermarkTransaction(window);
    return new WatermarkWindowResult<>(value, window);
  }

  @Override
  public String currentRunId() {
    return runId();
  }

  @Override
  public <T> T executeChunkRead(SourceChunkReader reader, ChunkReadWork<T> work)
      throws SQLException {
    return executeSqlWork(
        connection -> work.execute(new ConnectionBoundChunkReader(connection, reader)));
  }

  @Override
  public <T> WatermarkWindowResult<T> executeChunkReadInWatermarkWindow(
      SourceChunkReader reader, ChunkReadWithWindowWork<T> work) throws SQLException {
    return executeWithinWatermarkWindow(
        (connection, window) ->
            work.execute(new ConnectionBoundChunkReader(connection, reader), window));
  }

  @Override
  public boolean emitHeartbeatIfDue(Instant heartbeatTime, Duration minimumInterval)
      throws SQLException {
    Objects.requireNonNull(heartbeatTime, "heartbeatTime");
    Duration requiredMinimumInterval = requirePositive(minimumInterval, "minimumInterval");
    if (heartbeatWriter == null) {
      return false;
    }
    if (nextHeartbeatDueAt != null && heartbeatTime.isBefore(nextHeartbeatDueAt)) {
      return false;
    }
    ensureMetadataTablesIfConfigured();
    boolean written =
        heartbeatWriter.writeHeartbeatIfDue(
            sqlConnection(), runId(), sourceStreamId, heartbeatTime, requiredMinimumInterval);
    if (written) {
      nextHeartbeatDueAt = heartbeatTime.plus(requiredMinimumInterval);
      enqueueSyntheticHeartbeatTransaction(heartbeatTime);
    }
    return written;
  }

  private void enqueueSyntheticWatermarkTransaction(WatermarkWindow window) {
    long checkpointOrdinal = ++nextCheckpointOrdinal;
    InspectionCheckpointPosition checkpointPosition = new InspectionCheckpointPosition(checkpointOrdinal);
    List<ChangeEvent> events =
        List.of(
            watermarkEvent(window.low().value(), checkpointOrdinal, "low"),
            watermarkEvent(window.high().value(), checkpointOrdinal, "high"));
    enqueueCommittedTransactionInternal(
        new InspectionOnlyTransaction(
            "inspection-window-" + checkpointOrdinal,
            checkpointPosition,
            Instant.now(),
            events));
  }

  private void enqueueSyntheticHeartbeatTransaction(Instant heartbeatTime) {
    long checkpointOrdinal = ++nextCheckpointOrdinal;
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put(HeartbeatMetadata.RUN_ID_COLUMN, runId());
    afterRow.put(HeartbeatMetadata.SOURCE_STREAM_ID_COLUMN, sourceStreamId);
    afterRow.put(HeartbeatMetadata.TIMESTAMP_COLUMN, heartbeatTime.toString());
    enqueueCommittedTransactionInternal(
        new InspectionOnlyTransaction(
            "inspection-heartbeat-" + checkpointOrdinal,
            new InspectionCheckpointPosition(checkpointOrdinal, "heartbeat"),
            Instant.now(),
            List.of(
                new ChangeEvent(
                    HeartbeatMetadata.tableIdFor(watermarkDatabaseName),
                    OperationType.HEARTBEAT,
                    CaptureOrigin.LOG,
                    HeartbeatMetadata.singletonPrimaryKey(),
                    null,
                    ImmutableRowImage.of(afterRow),
                    new InspectionCheckpointPosition(checkpointOrdinal, "heartbeat"),
                    null,
                    null))));
  }

  private void ensureMetadataTablesIfConfigured() throws SQLException {
    if (metadataInitialized) {
      return;
    }
    if (watermarkWriter != null) {
      watermarkWriter.ensureMetadataTable(sqlConnection());
    }
    if (heartbeatWriter != null) {
      heartbeatWriter.ensureHeartbeatTable(sqlConnection());
    }
    metadataInitialized = true;
  }

  private ChangeEvent watermarkEvent(String token, long ordinal, String suffix) {
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put(WatermarkMetadata.RUN_ID_COLUMN, runId());
    afterRow.put(WatermarkMetadata.TOKEN_COLUMN, token);
    return new ChangeEvent(
        WatermarkMetadata.tableIdFor(watermarkDatabaseName),
        OperationType.WATERMARK,
        CaptureOrigin.LOG,
        WatermarkMetadata.singletonPrimaryKey(),
        null,
        ImmutableRowImage.of(afterRow),
        new InspectionCheckpointPosition(ordinal, suffix),
        null,
        null);
  }

  @Override
  protected InspectionCheckpointPosition checkpointPosition(InspectionOnlyTransaction transaction) {
    return transaction.checkpointPosition();
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static Duration requirePositive(Duration value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isZero() || value.isNegative()) {
      throw new IllegalArgumentException(name + " must be > 0");
    }
    return value;
  }

  public record InspectionOnlyTransaction(
      String transactionId,
      InspectionCheckpointPosition checkpointPosition,
      Instant commitTimestamp,
      List<ChangeEvent> events)
      implements SourceTransaction<InspectionCheckpointPosition> {
    public InspectionOnlyTransaction {
      transactionId = transactionId == null ? "inspection-only" : transactionId;
      checkpointPosition =
          checkpointPosition == null ? new InspectionCheckpointPosition(0L) : checkpointPosition;
      commitTimestamp = commitTimestamp == null ? Instant.EPOCH : commitTimestamp;
      events = events == null ? List.of() : List.copyOf(events);
    }
  }

  public record InspectionCheckpointPosition(long ordinal, String suffix)
      implements SourcePosition, Comparable<InspectionCheckpointPosition> {
    public InspectionCheckpointPosition(long ordinal) {
      this(ordinal, null);
    }

    @Override
    public String displayValue() {
      return suffix == null ? "inspection:" + ordinal : "inspection:" + ordinal + ":" + suffix;
    }

    @Override
    public int compareTo(InspectionCheckpointPosition other) {
      return Long.compare(ordinal, other.ordinal);
    }
  }
}
