package io.github.aandreakis.dblog.verification.scenario;

import java.time.Instant;

/** Latest sink-visible row state for one table/key in the scenario harness. */
public record ScenarioRowState(
    String scenarioId,
    String tableDisplayName,
    String primaryKeyLiteral,
    boolean present,
    String payload,
    String lastOperationType,
    String lastCaptureOrigin,
    long lastEventSequence,
    Instant updatedAt) {}
