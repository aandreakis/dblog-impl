package io.github.aandreakis.dblog.adapter.postgres;

import io.github.aandreakis.dblog.adapter.api.ChunkReadWithWindowWork;
import io.github.aandreakis.dblog.adapter.api.ChunkReadWork;
import io.github.aandreakis.dblog.adapter.api.ConnectionBoundChunkReader;
import io.github.aandreakis.dblog.adapter.api.HeartbeatMetadataWriter;
import io.github.aandreakis.dblog.adapter.api.RuntimeStatusInspectable;
import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.api.SourceSqlWork;
import io.github.aandreakis.dblog.adapter.api.WatermarkMetadataWriter;
import io.github.aandreakis.dblog.adapter.api.WatermarkWindowResult;
import io.github.aandreakis.dblog.adapter.api.WatermarkWindowRuntime;
import io.github.aandreakis.dblog.adapter.api.WatermarkWindowWork;
import io.github.aandreakis.dblog.adapter.postgres.internal.PostgresTransactionStreamingSession;
import io.github.aandreakis.dblog.core.model.WatermarkToken;
import io.github.aandreakis.dblog.core.reconcile.WatermarkWindow;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.runtime.sql.SourceFlowControlSnapshot;
import io.github.aandreakis.dblog.tap.Tap;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * First true stream-fed PostgreSQL runtime path.
 *
 * <p>Unlike the buffered inspection shell, this runtime reads committed transactions from a live
 * pgoutput session collaborator instead of manual enqueue calls.
 */
public final class PostgresLiveStreamingRuntime
    implements WatermarkWindowRuntime<PostgresPgoutputTransaction>, RuntimeStatusInspectable {
  private final String runId;
  private final Connection sqlConnection;
  private final Connection replicationConnection;
  private final String sourceId;
  private final String sourceStreamId;
  private final PostgresSourceCheckpointStore checkpointStore;
  private final WatermarkMetadataWriter watermarkWriter;
  private final HeartbeatMetadataWriter heartbeatWriter;
  private final PostgresTransactionStreamingSession session;
  private final Tap tap;
  private boolean metadataInitialized;

  /**
   * Best-effort in-process cache of when the next heartbeat is due, used only to skip a DB
   * round-trip through {@link HeartbeatMetadataWriter#writeHeartbeatIfDue} when we already know
   * the minimum interval has not elapsed. The authoritative rate limit is enforced by
   * {@link HeartbeatMetadataWriter#writeHeartbeatIfDue} reading the last heartbeat timestamp from
   * {@code dblog_meta.heartbeats}; after a process restart this field is {@code null} and the
   * first call delegates to the DB writer, which may elect not to write if the previous run's
   * heartbeat is still within the minimum interval.
   */
  private Instant nextHeartbeatDueAt;

  public PostgresLiveStreamingRuntime(
      Connection sqlConnection,
      Connection replicationConnection,
      String sourceId,
      String sourceStreamId,
      PostgresSourceCheckpointStore checkpointStore,
      WatermarkMetadataWriter watermarkWriter,
      HeartbeatMetadataWriter heartbeatWriter,
      PostgresTransactionStreamingSession session,
      Tap tap) {
    this(
        UUID.randomUUID().toString(),
        sqlConnection,
        replicationConnection,
        sourceId,
        sourceStreamId,
        checkpointStore,
        watermarkWriter,
        heartbeatWriter,
        session,
        tap);
  }

  public PostgresLiveStreamingRuntime(
      String runId,
      Connection sqlConnection,
      Connection replicationConnection,
      String sourceId,
      String sourceStreamId,
      PostgresSourceCheckpointStore checkpointStore,
      WatermarkMetadataWriter watermarkWriter,
      HeartbeatMetadataWriter heartbeatWriter,
      PostgresTransactionStreamingSession session,
      Tap tap) {
    this.runId = requireNonBlank(runId, "runId");
    this.sqlConnection = Objects.requireNonNull(sqlConnection, "sqlConnection");
    this.replicationConnection = Objects.requireNonNull(replicationConnection, "replicationConnection");
    this.sourceId = requireNonBlank(sourceId, "sourceId");
    this.sourceStreamId = requireNonBlank(sourceStreamId, "sourceStreamId");
    this.checkpointStore = Objects.requireNonNull(checkpointStore, "checkpointStore");
    this.watermarkWriter = Objects.requireNonNull(watermarkWriter, "watermarkWriter");
    this.heartbeatWriter = Objects.requireNonNull(heartbeatWriter, "heartbeatWriter");
    this.session = Objects.requireNonNull(session, "session");
    this.tap = Objects.requireNonNull(tap, "tap");
  }

  public <T> T executeSqlWork(SourceSqlWork<T> work) throws SQLException {
    return Objects.requireNonNull(work, "work").execute(sqlConnection);
  }

  public <T> WatermarkWindowResult<T> executeWithinWatermarkWindow(WatermarkWindowWork<T> work)
      throws SQLException {
    Objects.requireNonNull(work, "work");
    ensureMetadataInitialized();
    WatermarkWindow window = new WatermarkWindow(WatermarkToken.random(), WatermarkToken.random());
    watermarkWriter.writeWatermark(sqlConnection, runId, window.low());
    tap.onWatermarkWritten(Tap.WatermarkLevel.LOW, window.low());
    T value = work.execute(sqlConnection, window);
    watermarkWriter.writeWatermark(sqlConnection, runId, window.high());
    tap.onWatermarkWritten(Tap.WatermarkLevel.HIGH, window.high());
    return new WatermarkWindowResult<>(value, window);
  }

  @Override
  public String currentRunId() {
    return runId;
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
  public Optional<PostgresPgoutputTransaction> readPendingTransaction() throws SQLException {
    return session.readPendingTransaction();
  }

  @Override
  public void acknowledge(PostgresPgoutputTransaction transaction) {
    session.acknowledge(transaction);
  }

  /**
   * Emits a heartbeat row if the minimum interval has elapsed. The {@link #nextHeartbeatDueAt}
   * field is a best-effort in-process cache that lets us short-circuit when we already know a
   * write is not due; it is <em>not</em> the authoritative throttle. The canonical rate limit
   * is enforced by {@link HeartbeatMetadataWriter#writeHeartbeatIfDue(Connection, String, String,
   * Instant, Duration)}, which inspects the last persisted heartbeat timestamp on every call.
   * Consequently, after a process restart the first call will delegate to the DB writer even
   * though {@code nextHeartbeatDueAt} is {@code null}, and the writer may skip the write if the
   * previous run's heartbeat is still within the minimum interval.
   */
  @Override
  public boolean emitHeartbeatIfDue(Instant heartbeatTime, Duration minimumInterval)
      throws SQLException {
    Objects.requireNonNull(heartbeatTime, "heartbeatTime");
    Duration requiredMinimumInterval = requirePositive(minimumInterval, "minimumInterval");
    if (nextHeartbeatDueAt != null && heartbeatTime.isBefore(nextHeartbeatDueAt)) {
      return false;
    }
    ensureMetadataInitialized();
    boolean written =
        heartbeatWriter.writeHeartbeatIfDue(
            sqlConnection, runId, sourceStreamId, heartbeatTime, requiredMinimumInterval);
    if (written) {
      nextHeartbeatDueAt = heartbeatTime.plus(requiredMinimumInterval);
    }
    return written;
  }

  @Override
  public List<TableSchema> currentCapturedSchemas() {
    return session.currentCapturedSchemas();
  }

  @Override
  public void updateCapturedSchema(TableSchema schema) {
    session.updateCapturedSchema(schema);
  }

  @Override
  public String lastAcknowledgedCheckpointDisplayValue() {
    return checkpointStore.load(sourceId).map(PostgresLsn::displayValue).orElse(null);
  }

  @Override
  public int pendingTransactionCount() {
    // pgoutput is consumed via direct-poll: the orchestrator reads the replication stream on
    // demand rather than through an intermediate staging queue. There is no "pending" to
    // count, so this adapter returns the {@link RuntimeStatusInspectable#pendingTransactionCount
    // "unknown / not applicable"} sentinel ({@code -1}). See also
    // {@link #sourceFlowControlSnapshot()}, which reports {@link
    // SourceFlowControlSnapshot.Mode#DIRECT_POLL}.
    return -1;
  }

  @Override
  public int capturedTableCount() {
    return currentCapturedSchemas().size();
  }

  @Override
  public SourceFlowControlSnapshot sourceFlowControlSnapshot() {
    return session.sourceFlowControlSnapshot();
  }

  @Override
  public void close() throws Exception {
    Exception firstFailure = null;
    try {
      session.close();
    } catch (Exception failure) {
      firstFailure = failure;
    }
    try {
      replicationConnection.close();
    } catch (Exception failure) {
      if (firstFailure != null) {
        firstFailure.addSuppressed(failure);
      } else {
        firstFailure = failure;
      }
    }
    try {
      sqlConnection.close();
    } catch (Exception failure) {
      if (firstFailure != null) {
        firstFailure.addSuppressed(failure);
      } else {
        firstFailure = failure;
      }
    }
    if (firstFailure != null) {
      throw firstFailure;
    }
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
    watermarkWriter.ensureMetadataTable(sqlConnection);
    heartbeatWriter.ensureHeartbeatTable(sqlConnection);
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
