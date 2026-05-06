package io.github.aandreakis.dblog.core.request;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.reconcile.WatermarkWindow;
import java.util.List;
import java.util.Objects;

public record DumpWindowBatch<TX extends SourceTransaction<?>>(
    DumpTableProgress activeProgress,
    Chunk chunk,
    WatermarkWindow window,
    List<ChangeEvent> emittedEvents,
    TX checkpointTransaction)
    implements DumpWindowOutcome<TX> {
  public DumpWindowBatch {
    activeProgress = Objects.requireNonNull(activeProgress, "activeProgress");
    chunk = Objects.requireNonNull(chunk, "chunk");
    window = Objects.requireNonNull(window, "window");
    emittedEvents = List.copyOf(Objects.requireNonNull(emittedEvents, "emittedEvents"));
    checkpointTransaction =
        Objects.requireNonNull(checkpointTransaction, "checkpointTransaction");

    if (!activeProgress.hasActiveChunk()) {
      throw new IllegalArgumentException(
          "activeProgress must represent an in-flight chunk for dump/window coordination");
    }
    if (!activeProgress.jobId().equals(chunk.jobId())) {
      throw new IllegalArgumentException(
          "activeProgress jobId does not match coordinated chunk jobId");
    }
    if (!activeProgress.tableName().equals(chunk.tableName())) {
      throw new IllegalArgumentException(
          "activeProgress tableName does not match coordinated chunk tableName");
    }
    if (!Objects.equals(activeProgress.activeChunkStartAfter(), chunk.startAfterPrimaryKey())) {
      throw new IllegalArgumentException(
          "activeProgress activeChunkStartAfter does not match coordinated chunk startAfterPrimaryKey");
    }
    if (!window.low().value().equals(activeProgress.activeLowWatermark())) {
      throw new IllegalArgumentException(
          "activeProgress activeLowWatermark does not match coordinated window low token");
    }
    if (!window.high().value().equals(activeProgress.activeHighWatermark())) {
      throw new IllegalArgumentException(
          "activeProgress activeHighWatermark does not match coordinated window high token");
    }
  }

  @Override
  public TableId tableId() {
    return chunk.schema().tableId();
  }

  public SourcePosition checkpointPosition() {
    return checkpointTransaction.checkpointPosition();
  }
}
