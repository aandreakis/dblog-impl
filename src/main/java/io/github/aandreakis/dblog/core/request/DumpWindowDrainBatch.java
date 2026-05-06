package io.github.aandreakis.dblog.core.request;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.reconcile.WatermarkWindow;
import java.util.List;
import java.util.Objects;

/**
 * Drain-only outcome from {@link DumpWindowCoordinator#coordinateNextTableChunk}: the
 * watermark-scoped chunk SELECT returned zero rows, but the coordinator still opened LW/HW and
 * reconciled log events across the window. Carries whatever live log events arrived between LW
 * and HW and the transaction that carried the matching HW (for source-checkpoint advancement).
 *
 * <p>No progress advancement happens on ack — there was no chunk to complete — only the source
 * checkpoint moves forward.
 */
public record DumpWindowDrainBatch<TX extends SourceTransaction<?>>(
    String jobId,
    TableId tableId,
    WatermarkWindow window,
    List<ChangeEvent> emittedEvents,
    TX checkpointTransaction)
    implements DumpWindowOutcome<TX> {
  public DumpWindowDrainBatch {
    if (Objects.requireNonNull(jobId, "jobId").isBlank()) {
      throw new IllegalArgumentException("jobId must not be blank");
    }
    Objects.requireNonNull(tableId, "tableId");
    Objects.requireNonNull(window, "window");
    emittedEvents = List.copyOf(Objects.requireNonNull(emittedEvents, "emittedEvents"));
    Objects.requireNonNull(checkpointTransaction, "checkpointTransaction");
  }
}
