package io.github.aandreakis.dblog.core.model;

import java.util.Objects;

public record OpaqueSourcePosition(String displayValue) implements SourcePosition {
  public OpaqueSourcePosition {
    Objects.requireNonNull(displayValue, "displayValue");
    if (displayValue.isBlank()) {
      throw new IllegalArgumentException("displayValue must not be blank");
    }
  }
}
