package io.github.aandreakis.dblog.adapter.postgres.internal;

import java.sql.Connection;
import java.sql.SQLException;

/** Opens the PostgreSQL pgoutput stream wrapper. */
public interface PostgresPgoutputStreamFactory {
  PostgresPgoutputStream open(Connection replicationConnection, PostgresPgoutputStreamRequest request)
      throws SQLException;
}
