package io.github.aandreakis.dblog.adapter.postgres.internal;

import io.github.aandreakis.dblog.config.PostgresResourceOwnership;
import java.util.Objects;

/** Target logical replication slot contract for the PostgreSQL slice. */
public record PostgresReplicationSlotConfig(
    String slotName,
    String databaseName,
    String pluginName,
    boolean temporary,
    boolean twoPhase,
    boolean failover,
    PostgresResourceOwnership ownership) {
  public PostgresReplicationSlotConfig {
    slotName = requireNonBlank(slotName, "slotName");
    databaseName = requireNonBlank(databaseName, "databaseName");
    pluginName = requireNonBlank(pluginName, "pluginName");
    ownership = Objects.requireNonNull(ownership, "ownership");
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
