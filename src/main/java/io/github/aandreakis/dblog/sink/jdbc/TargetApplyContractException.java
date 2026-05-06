package io.github.aandreakis.dblog.sink.jdbc;

import java.util.Objects;

/** Fail-closed contract exception carrying structured target-apply failure details. */
public final class TargetApplyContractException extends IllegalStateException {
  private final TargetApplyFailure failure;

  public TargetApplyContractException(TargetApplyFailure failure, String message) {
    super(message);
    this.failure = Objects.requireNonNull(failure, "failure");
  }

  public TargetApplyContractException(TargetApplyFailure failure, String message, Throwable cause) {
    super(message, cause);
    this.failure = Objects.requireNonNull(failure, "failure");
  }

  public TargetApplyFailure failure() {
    return failure;
  }
}
