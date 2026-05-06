package io.github.aandreakis.dblog.core.request;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.reconcile.WatermarkWindow;
import java.util.List;

/**
 * Result of coordinating one watermark window for a table dump request.
 *
 * <p>Two variants:
 *
 * <ul>
 *   <li>{@link DumpWindowBatch} — the chunk SELECT returned rows; emittedEvents interleaves those
 *       rows with log events reconciled across the window, and the coordinator expects progress to
 *       advance to the chunk's last primary key on ack.
 *   <li>{@link DumpWindowDrainBatch} — the chunk SELECT returned zero rows (table already dumped
 *       or tail deleted after the upper-bound capture); emittedEvents carries only log events
 *       reconciled across the drain window, and ack advances only the source checkpoint.
 * </ul>
 *
 * <p>Both variants carry the live log events collected between LW and HW via the reconciler, so
 * downstream consumers can treat them uniformly when forwarding to the sink.
 */
public sealed interface DumpWindowOutcome<TX extends SourceTransaction<?>>
    permits DumpWindowBatch, DumpWindowDrainBatch {
  WatermarkWindow window();

  TableId tableId();

  List<ChangeEvent> emittedEvents();

  TX checkpointTransaction();

  default SourcePosition checkpointPosition() {
    return checkpointTransaction().checkpointPosition();
  }
}
