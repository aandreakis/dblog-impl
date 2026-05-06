package io.github.aandreakis.dblog.core.reconcile;

import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import java.util.List;
import java.util.Map;

public record ReconciliationResult(
    List<ChangeEvent> emittedEvents,
    Map<PrimaryKeyTuple, Map<String, Object>> remainingSnapshotRows,
    boolean chunkCompleted) {}
