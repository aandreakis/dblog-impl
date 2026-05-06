package io.github.aandreakis.dblog.core.schema;

public final class SchemaDriftException extends IllegalStateException {
  public SchemaDriftException(String message) {
    super(message);
  }
}
