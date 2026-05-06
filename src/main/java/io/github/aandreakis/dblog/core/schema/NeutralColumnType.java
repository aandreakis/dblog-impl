package io.github.aandreakis.dblog.core.schema;

public enum NeutralColumnType {
  BOOLEAN(true),
  INTEGER(true),
  FLOAT(true),
  DECIMAL(true),
  STRING(true),
  BINARY(true),
  DATE(true),
  TIME(true),
  TIMESTAMP(true),
  UUID(true),
  JSON(true),
  XML(true),
  ENUM_STRING(true),
  UNSUPPORTED(false);

  private final boolean supported;

  NeutralColumnType(boolean supported) {
    this.supported = supported;
  }

  public boolean isSupported() {
    return supported;
  }
}
