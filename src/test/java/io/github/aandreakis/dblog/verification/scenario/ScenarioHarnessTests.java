package io.github.aandreakis.dblog.verification.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ScenarioHarnessTests {
  @Test
  void runsScenarioAndVerifierAndReturnsSuccessfulResult() throws Exception {
    ScenarioHarness harness = new ScenarioHarness();
    AtomicReference<String> verified = new AtomicReference<>();

    ScenarioHarness.ScenarioResult<String> result =
        harness.run(
            new ScenarioHarness.ScenarioRequest<>(
                "scenario-a",
                "example scenario",
                () -> "ok",
                verified::set,
                null));

    assertThat(result.succeeded()).isTrue();
    assertThat(result.observation()).isEqualTo("ok");
    assertThat(result.failure()).isNull();
    assertThat(result.failureMessage()).isEmpty();
    assertThat(result.duration().isNegative()).isFalse();
    assertThat(verified.get()).isEqualTo("ok");
  }

  @Test
  void runSafelyCapturesFailureAndStillClosesCleanup() {
    ScenarioHarness harness = new ScenarioHarness();
    AtomicBoolean cleanupClosed = new AtomicBoolean(false);

    ScenarioHarness.ScenarioResult<String> result =
        harness.runSafely(
            new ScenarioHarness.ScenarioRequest<>(
                "scenario-b",
                "failing scenario",
                () -> {
                  throw new IllegalStateException("boom");
                },
                observation -> {},
                () -> cleanupClosed.set(true)));

    assertThat(result.succeeded()).isFalse();
    assertThat(result.observation()).isNull();
    assertThat(result.failure()).isInstanceOf(IllegalStateException.class);
    assertThat(result.failureMessage()).contains("boom");
    assertThat(cleanupClosed).isTrue();
  }

  @Test
  void runRethrowsScenarioFailure() {
    ScenarioHarness harness = new ScenarioHarness();

    assertThatThrownBy(
            () ->
                harness.run(
                    new ScenarioHarness.ScenarioRequest<>(
                        "scenario-c",
                        "throws",
                        () -> {
                          throw new IllegalArgumentException("bad");
                        },
                        observation -> {},
                        null)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("bad");
  }
}
