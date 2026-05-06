package io.github.aandreakis.dblog.runtime.sql;

import io.github.aandreakis.dblog.adapter.api.RuntimeStatusInspectable;
import io.github.aandreakis.dblog.adapter.api.SourceRuntime;
import io.github.aandreakis.dblog.adapter.api.SourceSqlWork;
import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Shared bounded-queue relational runtime foundation for source sessions.
 *
 * <p>This owns the runtime SQL connection, mutable captured-schema view, source-flow queue, and
 * durable checkpoint acknowledgement hook. Concrete adapters remain responsible for stream
 * decoding, synthetic metadata event creation, and source-specific position types.
 */
public abstract class AbstractQueueBackedRelationalRuntime<
        TX extends SourceTransaction<?>, CP extends SourcePosition>
    implements SourceRuntime<TX>, RuntimeStatusInspectable {
  private final java.sql.Connection sqlConnection;
  private final QueueBackedStreamingSession<TX, CP> session;
  private final String runId = UUID.randomUUID().toString();

  protected AbstractQueueBackedRelationalRuntime(
      java.sql.Connection sqlConnection,
      List<TableSchema> capturedSchemas,
      Consumer<CP> checkpointPersister,
      CP loadedCheckpoint,
      int queueCapacity,
      String queueLabel) {
    this.sqlConnection = Objects.requireNonNull(sqlConnection, "sqlConnection");
    this.session =
        new QueueBackedStreamingSession<>(
            queueLabel,
            queueCapacity,
            capturedSchemas,
            checkpointPersister,
            this::checkpointPosition,
            loadedCheckpoint);
  }

  protected final java.sql.Connection sqlConnection() {
    return sqlConnection;
  }

  protected final List<TableSchema> capturedSchemas() {
    return session.currentCapturedSchemas();
  }

  protected final String runId() {
    return runId;
  }

  protected final void enqueueCommittedTransactionInternal(TX transaction) {
    session.enqueue(transaction);
  }

  protected final <T> T executeRuntimeSqlWork(SourceSqlWork<T> work) throws java.sql.SQLException {
    return Objects.requireNonNull(work, "work").execute(sqlConnection);
  }

  protected final List<CP> acknowledgedPositionsInternal() {
    return session.acknowledgedPositions();
  }

  protected final long acknowledgedCountInternal() {
    return session.acknowledgedCount();
  }

  @Override
  public final Optional<TX> readPendingTransaction() throws SQLException {
    return session.readPendingTransaction();
  }

  @Override
  public final void acknowledge(TX transaction) {
    session.acknowledge(transaction);
  }

  @Override
  public final String lastAcknowledgedCheckpointDisplayValue() {
    return session.lastAcknowledgedCheckpointDisplayValue();
  }

  @Override
  public final int pendingTransactionCount() {
    return session.pendingTransactionCount();
  }

  @Override
  public final int capturedTableCount() {
    return session.capturedTableCount();
  }

  @Override
  public final List<TableSchema> currentCapturedSchemas() {
    return session.currentCapturedSchemas();
  }

  public final void updateCapturedSchema(TableSchema schema) {
    session.updateCapturedSchema(schema);
  }

  @Override
  public final SourceFlowControlSnapshot sourceFlowControlSnapshot() {
    return session.sourceFlowControlSnapshot();
  }

  @Override
  public void close() throws java.sql.SQLException {
    session.close();
    sqlConnection.close();
  }

  protected abstract CP checkpointPosition(TX transaction);

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
