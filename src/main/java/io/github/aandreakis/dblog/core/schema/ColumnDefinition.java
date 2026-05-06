package io.github.aandreakis.dblog.core.schema;

import java.util.Objects;

public record ColumnDefinition(
    String name,
    String sourceType,
    NeutralColumnType neutralType,
    boolean primaryKey,
    int primaryKeyOrdinal,
    boolean nullable) {

  public ColumnDefinition(
      String name,
      String sourceType,
      NeutralColumnType neutralType,
      boolean primaryKey,
      boolean nullable) {
    this(name, sourceType, neutralType, primaryKey, primaryKey ? 1 : 0, nullable);
  }

  public ColumnDefinition {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(sourceType, "sourceType");
    Objects.requireNonNull(neutralType, "neutralType");
    primaryKeyOrdinal = primaryKey ? Math.max(1, primaryKeyOrdinal) : 0;
    if (name.isBlank()) {
      throw new IllegalArgumentException("column name must not be blank");
    }
  }

  public boolean supported() {
    return neutralType.isSupported();
  }
}
