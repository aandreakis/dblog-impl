package io.github.aandreakis.dblog.adapter.mysql.internal;

import io.github.aandreakis.dblog.adapter.api.SourceRequiresFullDumpException;
import io.github.aandreakis.dblog.adapter.api.SourceSchemaUncertaintyException;
import io.github.aandreakis.dblog.adapter.mysql.MySqlBinlogTransaction;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourceCheckpointStore;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourcePosition;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.runtime.sql.RuntimeStreamingSession;
import io.github.aandreakis.dblog.runtime.sql.SourceFlowControlSnapshot;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Stream-fed MySQL committed-transaction session.
 *
 * <p>This is the first live-runtime seam beyond the inspection queue shells: transactions come from
 * a stream collaborator instead of being manually enqueued into the runtime itself.
 */
public final class MySqlTransactionStreamingSession
    implements RuntimeStreamingSession<MySqlBinlogTransaction> {
  private final String sourceId;
  private final List<TableSchema> capturedSchemas;
  private final MySqlTransactionStream stream;
  private final MySqlSourceCheckpointStore checkpointStore;

  public MySqlTransactionStreamingSession(
      String sourceId,
      List<TableSchema> capturedSchemas,
      MySqlTransactionStream stream,
      MySqlSourceCheckpointStore checkpointStore) {
    this.sourceId = requireNonBlank(sourceId, "sourceId");
    this.capturedSchemas =
        new ArrayList<>(List.copyOf(Objects.requireNonNull(capturedSchemas, "capturedSchemas")));
    this.stream = Objects.requireNonNull(stream, "stream");
    this.checkpointStore = Objects.requireNonNull(checkpointStore, "checkpointStore");
  }

  @Override
  public Optional<MySqlBinlogTransaction> readPendingTransaction() throws SQLException {
    try {
      return stream.readPendingTransaction();
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
  public void acknowledge(MySqlBinlogTransaction transaction) {
    Objects.requireNonNull(transaction, "transaction");
    Optional<MySqlSourcePosition> currentCheckpoint = checkpointStore.load(sourceId);
    if (currentCheckpoint.isEmpty()
        || transaction.checkpointPosition().compareTo(currentCheckpoint.orElseThrow()) > 0) {
      checkpointStore.save(sourceId, transaction.checkpointPosition());
    }
  }

  @Override
  public List<TableSchema> currentCapturedSchemas() {
    return List.copyOf(capturedSchemas);
  }

  @Override
  public void updateCapturedSchema(TableSchema schema) {
    Objects.requireNonNull(schema, "schema");
    for (int index = 0; index < capturedSchemas.size(); index++) {
      if (capturedSchemas.get(index).tableId().equals(schema.tableId())) {
        capturedSchemas.set(index, schema);
        checkpointStore.saveObservedTableSchema(schema);
        return;
      }
    }
    capturedSchemas.add(schema);
    checkpointStore.saveObservedTableSchema(schema);
  }

  @Override
  public SourceFlowControlSnapshot sourceFlowControlSnapshot() {
    return stream.sourceFlowControlSnapshot();
  }

  @Override
  public void close() throws Exception {
    stream.close();
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
}
