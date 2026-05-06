package io.github.aandreakis.dblog.verification.scenario;

import java.time.Instant;

/** One persisted event observed by the scenario harness sink. */
public record ScenarioEventRecord(
    long sequenceNumber,
    String scenarioId,
    String stageLabel,
    String tableDisplayName,
    String operationType,
    String captureOrigin,
    String primaryKeyLiteral,
    String sourcePosition,
    String transactionId,
    String dumpId,
    String beforePayload,
    String afterPayload,
    Instant recordedAt) {}
