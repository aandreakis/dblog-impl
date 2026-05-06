package io.github.aandreakis.dblog.verification.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

class ScenarioConfigCommonSupportTests {
  @Test
  void scenarioChunkSizePrefersScenarioSpecificProperty() {
    MockEnvironment environment =
        new MockEnvironment()
            .withProperty("dblog.chunk.size", "25")
            .withProperty("dblog.scenario.chunk-size", "40");

    assertThat(ScenarioConfigCommonSupport.scenarioChunkSize(environment)).isEqualTo(40);
  }

  @Test
  void scenarioChunkSizeFallsBackToRuntimeChunkSize() {
    MockEnvironment environment = new MockEnvironment().withProperty("dblog.chunk.size", "25");

    assertThat(ScenarioConfigCommonSupport.scenarioChunkSize(environment)).isEqualTo(25);
  }

  @Test
  void scenarioChunkSizeRejectsNonPositiveScenarioValue() {
    MockEnvironment environment = new MockEnvironment().withProperty("dblog.scenario.chunk-size", "0");

    assertThatThrownBy(() -> ScenarioConfigCommonSupport.scenarioChunkSize(environment))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("scenario chunk size");
  }
}
