package io.github.aandreakis.dblog;

/** Wraps a checked exception as unchecked. No message constructor by design — every throw site just rethrows the cause. */
public final class DbLogRuntimeException extends RuntimeException {
  public DbLogRuntimeException(Throwable cause) {
    super(cause);
  }
}
