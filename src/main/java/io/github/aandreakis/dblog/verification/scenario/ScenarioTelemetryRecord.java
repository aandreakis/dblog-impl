package io.github.aandreakis.dblog.verification.scenario;

import java.time.Instant;

/** One persisted telemetry point emitted by the scenario harness. */
public record ScenarioTelemetryRecord(
    long sequenceNumber,
    String scenarioId,
    String category,
    String message,
    String detail,
    Instant recordedAt) {}
