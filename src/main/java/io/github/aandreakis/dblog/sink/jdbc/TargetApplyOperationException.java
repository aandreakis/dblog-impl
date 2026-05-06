package io.github.aandreakis.dblog.sink.jdbc;

import java.util.Objects;

/** Operational target-apply exception carrying structured failure details. */
public final class TargetApplyOperationException extends IllegalStateException {
  private final TargetApplyFailure failure;

  public TargetApplyOperationException(
      TargetApplyFailure failure, String message, Throwable cause) {
    super(message, cause);
    this.failure = Objects.requireNonNull(failure, "failure");
  }

  public TargetApplyFailure failure() {
    return failure;
  }
}
