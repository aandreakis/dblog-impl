package io.github.aandreakis.dblog.core.checkpoint;

/** Fail-closed runtime exception for checkpoint-advance boundaries that cannot be completed safely. */
public class CheckpointAdvanceException extends IllegalStateException {
  public CheckpointAdvanceException(String message) {
    super(message);
  }

  public CheckpointAdvanceException(String message, Throwable cause) {
    super(message, cause);
  }
}
