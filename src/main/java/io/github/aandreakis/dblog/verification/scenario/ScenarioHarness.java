package io.github.aandreakis.dblog.verification.scenario;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Downstream scenario-mode shell.
 *
 * <p>This harness does not shape runtime internals. It simply executes a supplied scenario action,
 * optionally verifies the resulting observation, and returns a durable success/failure summary that
 * callers can log or persist.
 */
public final class ScenarioHarness {
  public <T> ScenarioResult<T> runSafely(ScenarioRequest<T> request) {
    Objects.requireNonNull(request, "request");
    Instant startedAt = Instant.now();
    T observation = null;
    Throwable failure = null;
    try {
      observation = request.execution().execute();
      if (request.verifier() != null) {
        request.verifier().verify(observation);
      }
    } catch (Throwable ex) {
      failure = ex;
    } finally {
      closeQuietly(request.cleanup(), failure);
    }
    return new ScenarioResult<>(request.scenarioId(), request.description(), startedAt, Instant.now(), observation, failure);
  }

  public <T> ScenarioResult<T> run(ScenarioRequest<T> request) throws Exception {
    ScenarioResult<T> result = runSafely(request);
    if (result.failure() == null) {
      return result;
    }
    if (result.failure() instanceof Exception exception) {
      throw exception;
    }
    throw new IllegalStateException(
        "Scenario execution failed with a non-Exception throwable", result.failure());
  }

  private static void closeQuietly(AutoCloseable cleanup, Throwable priorFailure) {
    if (cleanup == null) {
      return;
    }
    try {
      cleanup.close();
    } catch (Exception closeFailure) {
      if (priorFailure != null) {
        priorFailure.addSuppressed(closeFailure);
      } else {
        throw new IllegalStateException("Scenario cleanup failed", closeFailure);
      }
    }
  }

  public record ScenarioRequest<T>(
      String scenarioId,
      String description,
      ScenarioExecution<T> execution,
      ScenarioVerifier<T> verifier,
      AutoCloseable cleanup) {
    public ScenarioRequest {
      scenarioId = requireNonBlank(scenarioId, "scenarioId");
      description = description == null ? "" : description.trim();
      execution = Objects.requireNonNull(execution, "execution");
    }
  }

  public record ScenarioResult<T>(
      String scenarioId,
      String description,
      Instant startedAt,
      Instant finishedAt,
      T observation,
      Throwable failure) {
    public ScenarioResult {
      scenarioId = requireNonBlank(scenarioId, "scenarioId");
      description = description == null ? "" : description;
      startedAt = Objects.requireNonNull(startedAt, "startedAt");
      finishedAt = Objects.requireNonNull(finishedAt, "finishedAt");
    }

    public boolean succeeded() {
      return failure == null;
    }

    public Duration duration() {
      return Duration.between(startedAt, finishedAt);
    }

    public String failureMessage() {
      return failure == null ? "" : Objects.toString(failure.getMessage(), failure.getClass().getName());
    }
  }

  @FunctionalInterface
  public interface ScenarioExecution<T> {
    T execute() throws Exception;
  }

  @FunctionalInterface
  public interface ScenarioVerifier<T> {
    void verify(T observation) throws Exception;
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
