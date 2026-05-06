package io.github.aandreakis.dblog.adapter.api;

import io.github.aandreakis.dblog.core.reconcile.WatermarkWindow;
import java.util.Objects;

public record WatermarkWindowResult<T>(T value, WatermarkWindow window) {
  public WatermarkWindowResult {
    Objects.requireNonNull(window, "window");
  }
}
