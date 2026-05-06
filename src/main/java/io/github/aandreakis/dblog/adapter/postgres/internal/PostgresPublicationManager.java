package io.github.aandreakis.dblog.adapter.postgres.internal;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

/** Reads and, when explicitly owned, repairs the narrow PostgreSQL publication shape. */
public interface PostgresPublicationManager {
  Optional<PostgresPublicationState> readPublication(
      Connection connection, String databaseName, String publicationName)
      throws SQLException;

  PostgresPublicationState ensurePublication(Connection connection, PostgresPublicationConfig config)
      throws SQLException;
}
