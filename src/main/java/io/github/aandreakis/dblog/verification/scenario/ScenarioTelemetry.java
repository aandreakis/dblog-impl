package io.github.aandreakis.dblog.verification.scenario;

import java.time.Duration;
import java.util.Objects;

/** Small formatting helpers so scenario telemetry stays consistent across adapters. */
public final class ScenarioTelemetry {
  private ScenarioTelemetry() {}

  public static void record(
      ScenarioStore store, String scenarioId, String category, String message, String detail) {
    Objects.requireNonNull(store, "store");
    store.recordTelemetry(scenarioId, category, message, detail);
  }

  public static void runtimeRead(
      ScenarioStore store,
      String scenarioId,
      String adapter,
      String transactionId,
      String checkpoint,
      int eventCount) {
    record(
        store,
        scenarioId,
        "runtime-read",
        adapter + " read transaction",
        "transactionId="
            + transactionId
            + " checkpoint="
            + checkpoint
            + " eventCount="
            + eventCount);
  }

  public static void runtimeAcknowledge(
      ScenarioStore store,
      String scenarioId,
      String adapter,
      String transactionId,
      String checkpoint) {
    record(
        store,
        scenarioId,
        "checkpoint",
        adapter + " acknowledge",
        "transactionId=" + transactionId + " checkpoint=" + checkpoint);
  }

  public static void requestStatus(
      ScenarioStore store, String scenarioId, String adapter, Object status) {
    record(store, scenarioId, "request-status", adapter + " request status", status.toString());
  }

  public static void delay(
      ScenarioStore store, String scenarioId, String category, String message, Duration duration) {
    record(store, scenarioId, category, message, "delay=" + duration);
  }

  public static void injectedFailure(
      ScenarioStore store, String scenarioId, String category, String message, String detail) {
    record(store, scenarioId, category, message, detail);
  }
}
