package io.github.aandreakis.dblog.adapter.postgres.internal;

import io.github.aandreakis.dblog.adapter.postgres.PostgresLsn;
import java.util.Objects;
import java.util.Optional;

/** Snapshot of one PostgreSQL replication slot as seen from {@code pg_replication_slots}. */
public record PostgresReplicationSlotState(
    String slotName,
    String slotType,
    String databaseName,
    String pluginName,
    boolean temporary,
    boolean active,
    boolean twoPhase,
    boolean failover,
    Optional<PostgresLsn> restartLsn,
    Optional<PostgresLsn> confirmedFlushLsn,
    String walStatus,
    String invalidationReason) {
  public PostgresReplicationSlotState {
    slotName = requireNonBlank(slotName, "slotName");
    slotType = requireNonBlank(slotType, "slotType");
    databaseName = normalizeOptionalText(databaseName);
    pluginName = normalizeOptionalText(pluginName);
    restartLsn = restartLsn == null ? Optional.empty() : restartLsn;
    confirmedFlushLsn = confirmedFlushLsn == null ? Optional.empty() : confirmedFlushLsn;
    walStatus = normalizeOptionalText(walStatus);
    invalidationReason = normalizeOptionalText(invalidationReason);
  }

  public boolean isLogical() {
    return "logical".equals(slotType);
  }

  public boolean isUsable() {
    return invalidationReason.isBlank() && !"lost".equals(walStatus);
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static String normalizeOptionalText(String value) {
    return value == null ? "" : value;
  }
}
