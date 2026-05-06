package io.github.aandreakis.dblog.core.model;

import java.util.Objects;

/**
 * A captured row-level change. Row images (primary key, before, after) are carried as
 * {@link ImmutableRowImage}; metadata events (watermark, heartbeat) still use the same
 * structure with a single-row singleton image.
 *
 * <p>{@link #primaryKeyHash} is an optional, pre-computed lookup key the reconciler uses on
 * the hot path to avoid rebuilding a heavy {@code PrimaryKeyTuple} per event. Adapter log
 * decoders populate it eagerly from normalised values they already hold; other producers may
 * leave it {@code null}, in which case the reconciler computes it on demand from the schema.
 */
public record ChangeEvent(
    TableId tableId,
    OperationType operationType,
    CaptureOrigin captureOrigin,
    ImmutableRowImage primaryKey,
    ImmutableRowImage beforeRow,
    ImmutableRowImage afterRow,
    SourcePosition sourcePosition,
    String transactionId,
    String dumpId,
    PrimaryKeyHash primaryKeyHash) {

  public ChangeEvent {
    Objects.requireNonNull(tableId, "tableId");
    Objects.requireNonNull(operationType, "operationType");
    Objects.requireNonNull(captureOrigin, "captureOrigin");
    Objects.requireNonNull(primaryKey, "primaryKey");
    if (primaryKey.isEmpty()) {
      throw new IllegalArgumentException("primaryKey must not be empty");
    }
    Objects.requireNonNull(sourcePosition, "sourcePosition");
  }

  /** Backward-compatible constructor for callers that do not pre-compute the primary-key hash. */
  public ChangeEvent(
      TableId tableId,
      OperationType operationType,
      CaptureOrigin captureOrigin,
      ImmutableRowImage primaryKey,
      ImmutableRowImage beforeRow,
      ImmutableRowImage afterRow,
      SourcePosition sourcePosition,
      String transactionId,
      String dumpId) {
    this(
        tableId,
        operationType,
        captureOrigin,
        primaryKey,
        beforeRow,
        afterRow,
        sourcePosition,
        transactionId,
        dumpId,
        null);
  }
}
