package io.github.aandreakis.dblog.adapter.postgres.internal;

import java.util.Objects;

/** Publication and logical-slot state after PostgreSQL resource preflight completes. */
public record PostgresReplicationResourcesResult(
    String databaseName,
    PostgresPublicationState publication,
    PostgresReplicationSlotState slot) {
  public PostgresReplicationResourcesResult {
    databaseName = requireNonBlank(databaseName, "databaseName");
    publication = Objects.requireNonNull(publication, "publication");
    slot = Objects.requireNonNull(slot, "slot");
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
