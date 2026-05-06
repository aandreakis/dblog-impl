package io.github.aandreakis.dblog.verification.scenario.support;

import io.github.aandreakis.dblog.verification.scenario.ScenarioJdbcSupport;
import io.github.aandreakis.dblog.verification.scenario.ScenarioStore;
import io.github.aandreakis.dblog.verification.scenario.ScenarioTelemetry;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/** Shared helper for scenario-only delay/failure injection counters. */
public final class ScenarioFailureSupport {
  private final ScenarioStore scenarioStore;
  private final String scenarioId;
  private final AtomicInteger invocations = new AtomicInteger();

  public ScenarioFailureSupport(ScenarioStore scenarioStore, String scenarioId) {
    this.scenarioStore = Objects.requireNonNull(scenarioStore, "scenarioStore");
    this.scenarioId = Objects.requireNonNull(scenarioId, "scenarioId");
  }

  public int nextInvocation() {
    return invocations.incrementAndGet();
  }

  public void maybeDelay(String category, String message, Duration delay) {
    Objects.requireNonNull(delay, "delay");
    if (delay.isZero()) {
      return;
    }
    ScenarioTelemetry.delay(scenarioStore, scenarioId, category, message, delay);
    ScenarioJdbcSupport.sleepQuietly(delay);
  }

  public void fail(String category, String message, String detail) {
    ScenarioTelemetry.injectedFailure(scenarioStore, scenarioId, category, message, detail);
    throw new IllegalStateException(message + ": " + detail);
  }
}
