package io.github.aandreakis.dblog.core.reconcile;

import io.github.aandreakis.dblog.core.model.WatermarkToken;
import java.util.Objects;

public record WatermarkWindow(WatermarkToken low, WatermarkToken high) {
  public WatermarkWindow {
    Objects.requireNonNull(low, "low");
    Objects.requireNonNull(high, "high");
    if (low.equals(high)) {
      throw new IllegalArgumentException("low and high watermark tokens must be distinct");
    }
  }
}
