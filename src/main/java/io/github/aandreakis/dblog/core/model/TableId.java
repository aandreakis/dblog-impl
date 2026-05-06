package io.github.aandreakis.dblog.core.model;

import java.util.Objects;

public record TableId(String databaseName, String schemaName, String tableName) {
  public TableId {
    Objects.requireNonNull(databaseName, "databaseName");
    Objects.requireNonNull(schemaName, "schemaName");
    Objects.requireNonNull(tableName, "tableName");
    if (databaseName.isBlank() || schemaName.isBlank() || tableName.isBlank()) {
      throw new IllegalArgumentException("table identifier parts must not be blank");
    }
  }

  public String displayName() {
    return databaseName + "." + schemaName + "." + tableName;
  }
}
