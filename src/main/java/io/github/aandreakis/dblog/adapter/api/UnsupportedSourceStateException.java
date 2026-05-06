package io.github.aandreakis.dblog.adapter.api;

/** Fail-closed runtime exception for source states the current slice intentionally does not handle. */
public class UnsupportedSourceStateException extends IllegalStateException {
  public UnsupportedSourceStateException(String message) {
    super(message);
  }

  public UnsupportedSourceStateException(String message, Throwable cause) {
    super(message, cause);
  }
}
