package io.github.aandreakis.dblog.verification.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class ScenarioFaultPlanTests {
  @Test
  void matchesNamedStateStoreOperationOnlyAfterThreshold() {
    ScenarioFaultPlan plan =
        new ScenarioFaultPlan(
            Duration.ZERO,
            null,
            null,
            Duration.ZERO,
            null,
            Duration.ZERO,
            null,
            Duration.ZERO,
            null,
            Duration.ZERO,
            3,
            "saveDumpRequestStatus",
            Duration.ZERO,
            Duration.ZERO,
            Duration.ZERO,
            Duration.ZERO,
            Duration.ZERO,
            Duration.ZERO);

    assertThat(plan.shouldFailStateStoreOperation("saveDumpRequestStatus", 1)).isFalse();
    assertThat(plan.shouldFailStateStoreOperation("loadPendingDumpRequests", 3)).isFalse();
    assertThat(plan.shouldFailStateStoreOperation("saveDumpRequestStatus", 2)).isFalse();
    assertThat(plan.shouldFailStateStoreOperation("saveDumpRequestStatus", 3)).isTrue();
    assertThat(plan.shouldFailStateStoreOperation("saveDumpRequestStatus", 4)).isTrue();
  }

  @Test
  void rejectsNegativeDurationsAndNonPositiveThresholds() {
    assertThatThrownBy(
            () ->
                new ScenarioFaultPlan(
                    Duration.ofSeconds(-1),
                    null,
                    null,
                    Duration.ZERO,
                    null,
                    Duration.ZERO,
                    null,
                    Duration.ZERO,
                    null,
                    Duration.ZERO,
                    null,
                    null,
                    Duration.ZERO,
                    Duration.ZERO,
                    Duration.ZERO,
                    Duration.ZERO,
                    Duration.ZERO,
                    Duration.ZERO))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("sinkDelay");

    assertThatThrownBy(
            () ->
                new ScenarioFaultPlan(
                    Duration.ZERO,
                    null,
                    null,
                    Duration.ZERO,
                    0,
                    Duration.ZERO,
                    null,
                    Duration.ZERO,
                    null,
                    Duration.ZERO,
                    null,
                    null,
                    Duration.ZERO,
                    Duration.ZERO,
                    Duration.ZERO,
                    Duration.ZERO,
                    Duration.ZERO,
                    Duration.ZERO))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("failRuntimeReadAfterCount");
  }
}
