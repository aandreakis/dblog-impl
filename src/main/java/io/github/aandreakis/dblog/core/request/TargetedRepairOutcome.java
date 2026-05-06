package io.github.aandreakis.dblog.core.request;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.reconcile.WatermarkWindow;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import java.util.List;

/**
 * Result of coordinating one watermark window for a targeted primary-key repair.
 *
 * <p>Two variants mirror {@link DumpWindowOutcome}: a real {@link TargetedRepairBatch} carrying
 * the repaired rows, or a {@link TargetedRepairDrainBatch} produced when none of the requested
 * primary keys currently exist (every requested key reported missing). Both surface live log
 * events reconciled across the window so the sink never loses in-window traffic.
 */
public sealed interface TargetedRepairOutcome<TX extends SourceTransaction<?>>
    permits TargetedRepairBatch, TargetedRepairDrainBatch {
  DumpRequest request();

  WatermarkWindow window();

  default TableId tableId() {
    return request().tableId();
  }

  List<ChangeEvent> emittedEvents();

  List<PrimaryKeyTuple> missingPrimaryKeyTuples();

  default List<String> missingPrimaryKeys() {
    return missingPrimaryKeyTuples().stream().map(PrimaryKeyTuple::literal).toList();
  }

  TX checkpointTransaction();

  default SourcePosition checkpointPosition() {
    return checkpointTransaction().checkpointPosition();
  }
}
