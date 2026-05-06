package io.github.aandreakis.dblog.adapter.postgres.internal;

import io.github.aandreakis.dblog.core.model.TableId;
import java.util.List;
import java.util.Objects;

/** Current PostgreSQL publication shape as seen through the explicit-table next slice. */
public record PostgresPublicationState(
    String databaseName,
    String publicationName,
    boolean allTables,
    boolean schemaScoped,
    boolean publishesInsert,
    boolean publishesUpdate,
    boolean publishesDelete,
    boolean publishesTruncate,
    boolean publishViaPartitionRoot,
    List<PostgresPublicationTableState> tables) {
  public PostgresPublicationState {
    databaseName = requireNonBlank(databaseName, "databaseName");
    publicationName = requireNonBlank(publicationName, "publicationName");
    Objects.requireNonNull(tables, "tables");
    tables = List.copyOf(tables);
  }

  public List<TableId> tableIds() {
    return tables.stream().map(PostgresPublicationTableState::tableId).toList();
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
