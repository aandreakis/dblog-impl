package io.github.aandreakis.dblog.adapter.postgres.internal;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

/** Ensures the narrow PostgreSQL publication and logical-slot contract before live streaming begins. */
public final class PostgresReplicationResourcesPreflight {
  private final PostgresPublicationManager publicationManager;
  private final PostgresReplicationSlotManager slotManager;

  public PostgresReplicationResourcesPreflight() {
    this(new JdbcPostgresPublicationManager(), new JdbcPostgresReplicationSlotManager());
  }

  public PostgresReplicationResourcesPreflight(
      PostgresPublicationManager publicationManager, PostgresReplicationSlotManager slotManager) {
    this.publicationManager = Objects.requireNonNull(publicationManager, "publicationManager");
    this.slotManager = Objects.requireNonNull(slotManager, "slotManager");
  }

  public PostgresReplicationResourcesResult inspectAndEnsure(
      Connection connection, PostgresReplicationResourcesRequest request)
      throws SQLException {
    Objects.requireNonNull(connection, "connection");
    Objects.requireNonNull(request, "request");

    PostgresPublicationState publication =
        publicationManager.ensurePublication(connection, request.publication());
    PostgresReplicationSlotState slot = slotManager.ensureLogicalSlot(connection, request.slot());
    return new PostgresReplicationResourcesResult(request.databaseName(), publication, slot);
  }
}
