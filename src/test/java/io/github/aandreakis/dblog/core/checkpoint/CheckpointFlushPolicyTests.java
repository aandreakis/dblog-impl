package io.github.aandreakis.dblog.core.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class CheckpointFlushPolicyTests {
  @Test
  void defaultsMatchShippedCountAndTimePolicy() {
    CheckpointFlushPolicy defaults = CheckpointFlushPolicy.defaults();

    assertThat(defaults.maxBufferedEvents()).isEqualTo(100);
    assertThat(defaults.maxBufferedTime()).isEqualTo(Duration.ofSeconds(5));
  }

  @Test
  void rejectsNonPositiveThresholds() {
    assertThatThrownBy(() -> new CheckpointFlushPolicy(0, Duration.ofSeconds(5)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maxBufferedEvents");
    assertThatThrownBy(() -> new CheckpointFlushPolicy(10, Duration.ZERO))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maxBufferedTime");
  }
}
