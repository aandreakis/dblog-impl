package io.github.aandreakis.dblog.core.request;

/** Core-owned failure for request coordination when a lower-level runtime port fails. */
public final class CoreRequestExecutionException extends RuntimeException {
  public CoreRequestExecutionException(String message, Throwable cause) {
    super(message, cause);
  }
}
