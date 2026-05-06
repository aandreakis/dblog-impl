package io.github.aandreakis.dblog.core.request;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.reconcile.WatermarkWindow;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import java.util.List;
import java.util.Objects;

/**
 * Drain-only outcome from {@link TargetedRepairCoordinator#coordinate}: the watermark-scoped
 * targeted-key SELECT returned zero rows (every requested key is currently missing from the
 * source), but the coordinator still opened LW/HW and reconciled log events across the window.
 * Carries whatever live log events arrived between LW and HW and the transaction that carried
 * the matching HW (for source-checkpoint advancement).
 */
public record TargetedRepairDrainBatch<TX extends SourceTransaction<?>>(
    DumpRequest request,
    WatermarkWindow window,
    List<PrimaryKeyTuple> missingPrimaryKeyTuples,
    List<ChangeEvent> emittedEvents,
    TX checkpointTransaction)
    implements TargetedRepairOutcome<TX> {
  public TargetedRepairDrainBatch {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(window, "window");
    missingPrimaryKeyTuples =
        List.copyOf(Objects.requireNonNull(missingPrimaryKeyTuples, "missingPrimaryKeyTuples"));
    emittedEvents = List.copyOf(Objects.requireNonNull(emittedEvents, "emittedEvents"));
    Objects.requireNonNull(checkpointTransaction, "checkpointTransaction");

    if (request.scope() != DumpScope.PRIMARY_KEYS) {
      throw new IllegalArgumentException(
          "targeted repair drain batches require a PRIMARY_KEYS dump request");
    }
  }
}
