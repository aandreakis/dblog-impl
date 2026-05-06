package io.github.aandreakis.dblog.adapter.postgres.internal;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

/** Reads and, when explicitly owned, creates the narrow PostgreSQL logical slot shape. */
public interface PostgresReplicationSlotManager {
  Optional<PostgresReplicationSlotState> readSlot(Connection connection, String slotName)
      throws SQLException;

  PostgresReplicationSlotState ensureLogicalSlot(
      Connection connection, PostgresReplicationSlotConfig config)
      throws SQLException;
}
