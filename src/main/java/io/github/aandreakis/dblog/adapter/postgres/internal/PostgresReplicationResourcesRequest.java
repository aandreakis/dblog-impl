package io.github.aandreakis.dblog.adapter.postgres.internal;

import java.util.Objects;

/** Publication and logical-slot plan used before the live pgoutput runtime begins. */
public record PostgresReplicationResourcesRequest(
    String databaseName,
    PostgresPublicationConfig publication,
    PostgresReplicationSlotConfig slot) {
  public PostgresReplicationResourcesRequest {
    databaseName = requireNonBlank(databaseName, "databaseName");
    publication = Objects.requireNonNull(publication, "publication");
    slot = Objects.requireNonNull(slot, "slot");
    if (!databaseName.equals(publication.databaseName())) {
      throw new IllegalArgumentException(
          "publication database does not match replication-resources database: "
              + publication.databaseName()
              + " expected "
              + databaseName);
    }
    if (!databaseName.equals(slot.databaseName())) {
      throw new IllegalArgumentException(
          "slot database does not match replication-resources database: "
              + slot.databaseName()
              + " expected "
              + databaseName);
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
