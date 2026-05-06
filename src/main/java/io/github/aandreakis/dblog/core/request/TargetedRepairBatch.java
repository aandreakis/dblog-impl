package io.github.aandreakis.dblog.core.request;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.reconcile.WatermarkWindow;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import java.util.List;
import java.util.Objects;

public record TargetedRepairBatch<TX extends SourceTransaction<?>>(
    DumpRequest request,
    Chunk chunk,
    WatermarkWindow window,
    List<PrimaryKeyTuple> missingPrimaryKeyTuples,
    List<ChangeEvent> emittedEvents,
    TX checkpointTransaction)
    implements TargetedRepairOutcome<TX> {
  public TargetedRepairBatch {
    request = Objects.requireNonNull(request, "request");
    chunk = Objects.requireNonNull(chunk, "chunk");
    window = Objects.requireNonNull(window, "window");
    missingPrimaryKeyTuples =
        List.copyOf(Objects.requireNonNull(missingPrimaryKeyTuples, "missingPrimaryKeyTuples"));
    emittedEvents = List.copyOf(Objects.requireNonNull(emittedEvents, "emittedEvents"));
    checkpointTransaction =
        Objects.requireNonNull(checkpointTransaction, "checkpointTransaction");

    if (request.scope() != DumpScope.PRIMARY_KEYS) {
      throw new IllegalArgumentException(
          "targeted repair batches require a PRIMARY_KEYS dump request");
    }
    if (!request.requestId().equals(chunk.jobId())) {
      throw new IllegalArgumentException("requestId does not match targeted repair chunk jobId");
    }
    if (!Objects.equals(request.tableId(), chunk.schema().tableId())) {
      throw new IllegalArgumentException(
          "dump request table does not match targeted repair chunk schema table");
    }
    if (!chunk.finalChunk()) {
      throw new IllegalArgumentException(
          "targeted repair chunks must always be final in the current DBLog core");
    }
  }

  public SourcePosition checkpointPosition() {
    return checkpointTransaction.checkpointPosition();
  }
}
