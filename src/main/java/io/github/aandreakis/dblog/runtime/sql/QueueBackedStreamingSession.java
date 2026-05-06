package io.github.aandreakis.dblog.runtime.sql;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Shared bounded-queue streaming session implementation for runtimes.
 *
 * <p>This owns the queue, mutable captured-schema view, and checkpoint acknowledgement behavior
 * while leaving actual event production to the surrounding adapter/runtime.
 */
public final class QueueBackedStreamingSession<
        TX extends SourceTransaction<?>, CP extends SourcePosition>
    implements RuntimeStreamingSession<TX> {
  private final List<TableSchema> capturedSchemas;
  private List<TableSchema> capturedSchemasView;
  private final Consumer<CP> checkpointPersister;
  private final Function<TX, CP> checkpointExtractor;
  private final BoundedSourceEventQueue<TX> pendingTransactions;
  private CP lastAcknowledged;
  private long acknowledgedCount;

  public QueueBackedStreamingSession(
      String queueLabel,
      int queueCapacity,
      List<TableSchema> capturedSchemas,
      Consumer<CP> checkpointPersister,
      Function<TX, CP> checkpointExtractor,
      CP loadedCheckpoint) {
    this.capturedSchemas =
        new ArrayList<>(List.copyOf(Objects.requireNonNull(capturedSchemas, "capturedSchemas")));
    this.capturedSchemasView = List.copyOf(this.capturedSchemas);
    this.checkpointPersister = Objects.requireNonNull(checkpointPersister, "checkpointPersister");
    this.checkpointExtractor = Objects.requireNonNull(checkpointExtractor, "checkpointExtractor");
    this.pendingTransactions =
        new BoundedSourceEventQueue<>(requireNonBlank(queueLabel, "queueLabel"), queueCapacity);
    if (loadedCheckpoint != null) {
      lastAcknowledged = loadedCheckpoint;
    }
  }

  public void enqueue(TX transaction) {
    if (!pendingTransactions.enqueue(Objects.requireNonNull(transaction, "transaction"))) {
      throw new IllegalStateException("queue-backed streaming session is closed");
    }
  }

  public List<CP> acknowledgedPositions() {
    return lastAcknowledged == null ? List.of() : List.of(lastAcknowledged);
  }

  public long acknowledgedCount() {
    return acknowledgedCount;
  }

  public String lastAcknowledgedCheckpointDisplayValue() {
    return lastAcknowledged == null ? null : lastAcknowledged.displayValue();
  }

  public int pendingTransactionCount() {
    return pendingTransactions.depth();
  }

  public int capturedTableCount() {
    return capturedSchemas.size();
  }

  @Override
  public Optional<TX> readPendingTransaction() {
    return pendingTransactions.pollNow();
  }

  @Override
  public void acknowledge(TX transaction) {
    CP checkpoint = checkpointExtractor.apply(Objects.requireNonNull(transaction, "transaction"));
    checkpointPersister.accept(checkpoint);
    lastAcknowledged = checkpoint;
    acknowledgedCount++;
  }

  @Override
  public List<TableSchema> currentCapturedSchemas() {
    return capturedSchemasView;
  }

  @Override
  public void updateCapturedSchema(TableSchema schema) {
    Objects.requireNonNull(schema, "schema");
    for (int index = 0; index < capturedSchemas.size(); index++) {
      if (capturedSchemas.get(index).tableId().equals(schema.tableId())) {
        capturedSchemas.set(index, schema);
        capturedSchemasView = List.copyOf(capturedSchemas);
        return;
      }
    }
    capturedSchemas.add(schema);
    capturedSchemasView = List.copyOf(capturedSchemas);
  }

  @Override
  public SourceFlowControlSnapshot sourceFlowControlSnapshot() {
    return pendingTransactions.snapshot();
  }

  @Override
  public void close() {
    pendingTransactions.close();
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
