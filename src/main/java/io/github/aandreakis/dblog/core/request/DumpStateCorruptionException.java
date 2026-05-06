package io.github.aandreakis.dblog.core.request;

public final class DumpStateCorruptionException extends IllegalStateException {
  public DumpStateCorruptionException(String message) {
    super(message);
  }
}
