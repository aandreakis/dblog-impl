package io.github.aandreakis.dblog.core.schema;

/** Fail-closed runtime exception for schema compatibility boundaries. */
public class SchemaCompatibilityException extends IllegalStateException {
  public SchemaCompatibilityException(String message) {
    super(message);
  }

  public SchemaCompatibilityException(String message, Throwable cause) {
    super(message, cause);
  }
}
