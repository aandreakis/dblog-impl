package io.github.aandreakis.dblog.tap;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Educational observability tap ({@code dblog.tap}). Off by default. When on, the tap blocks the
 * DBLog pump thread whenever the attached TUI lags the producer. Never enable in production.
 */
@Validated
@ConfigurationProperties(prefix = "dblog.tap")
public class TapConfig {
  private boolean enabled = false;

  @Min(16)
  @Max(1_048_576)
  private int queueCapacity = 65_536;

  @Min(50)
  @Max(5_000)
  private int standbyThresholdMs = 1_000;

  @NotNull private Duration heartbeatInterval = Duration.ofMillis(2_000);

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public int getQueueCapacity() {
    return queueCapacity;
  }

  public void setQueueCapacity(int queueCapacity) {
    this.queueCapacity = queueCapacity;
  }

  public int getStandbyThresholdMs() {
    return standbyThresholdMs;
  }

  public void setStandbyThresholdMs(int standbyThresholdMs) {
    this.standbyThresholdMs = standbyThresholdMs;
  }

  public Duration getHeartbeatInterval() {
    return heartbeatInterval;
  }

  public void setHeartbeatInterval(Duration heartbeatInterval) {
    this.heartbeatInterval = heartbeatInterval;
  }
}
