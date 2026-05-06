package io.github.aandreakis.dblog.core.model;

import java.util.Objects;
import java.util.UUID;

public record WatermarkToken(String value) {
  public WatermarkToken {
    Objects.requireNonNull(value, "value");
    if (value.isBlank()) {
      throw new IllegalArgumentException("watermark token must not be blank");
    }
  }

  public static WatermarkToken random() {
    return new WatermarkToken(UUID.randomUUID().toString());
  }
}
