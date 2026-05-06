package io.github.aandreakis.dblog.adapter.postgres.internal;

import java.sql.Connection;
import java.sql.SQLException;

/** Opens a JDBC connection in PostgreSQL logical-replication mode for the adapter. */
@FunctionalInterface
public interface PostgresReplicationConnectionFactory {
  Connection open(String jdbcUrl, String username, String password) throws SQLException;
}
