package io.github.aandreakis.dblog.adapter.postgres.internal;

import io.github.aandreakis.dblog.config.PostgresResourceOwnership;
import io.github.aandreakis.dblog.core.model.TableId;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

/** Explicit-table publication target for the PostgreSQL slice. */
public record PostgresPublicationConfig(
    String databaseName,
    String publicationName,
    List<TableId> capturedTables,
    PostgresResourceOwnership ownership) {
  public PostgresPublicationConfig {
    databaseName = requireNonBlank(databaseName, "databaseName");
    publicationName = requireNonBlank(publicationName, "publicationName");
    Objects.requireNonNull(capturedTables, "capturedTables");
    ownership = Objects.requireNonNull(ownership, "ownership");
    capturedTables = List.copyOf(capturedTables);
    if (capturedTables.isEmpty()) {
      throw new IllegalArgumentException("capturedTables must not be empty");
    }
    if (new LinkedHashSet<>(capturedTables).size() != capturedTables.size()) {
      throw new IllegalArgumentException("capturedTables must not contain duplicates: " + capturedTables);
    }
    for (TableId tableId : capturedTables) {
      Objects.requireNonNull(tableId, "capturedTables element");
      if (!databaseName.equals(tableId.databaseName())) {
        throw new IllegalArgumentException(
            "captured table database does not match publication database: "
                + tableId.displayName()
                + " expected database "
                + databaseName);
      }
    }
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
