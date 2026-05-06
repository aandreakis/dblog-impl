package io.github.aandreakis.dblog.core.reconcile;

public final class WatermarkSequenceException extends IllegalStateException {
  public WatermarkSequenceException(String message) {
    super(message);
  }
}
