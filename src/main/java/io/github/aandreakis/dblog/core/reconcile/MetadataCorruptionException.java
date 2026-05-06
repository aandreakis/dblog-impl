package io.github.aandreakis.dblog.core.reconcile;

/** Fail-closed runtime exception for invalid metadata-table shape or contents. */
public class MetadataCorruptionException extends IllegalStateException {
  public MetadataCorruptionException(String message) {
    super(message);
  }

  public MetadataCorruptionException(String message, Throwable cause) {
    super(message, cause);
  }
}
