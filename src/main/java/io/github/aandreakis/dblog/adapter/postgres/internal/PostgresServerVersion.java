package io.github.aandreakis.dblog.adapter.postgres.internal;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Objects;

/** Minimal PostgreSQL server-version helper for pgoutput option compatibility decisions. */
public record PostgresServerVersion(int majorVersion) {
  public PostgresServerVersion {
    if (majorVersion <= 0) {
      throw new IllegalArgumentException("majorVersion must be > 0");
    }
  }

  public static PostgresServerVersion from(Connection connection) throws SQLException {
    Objects.requireNonNull(connection, "connection");
    DatabaseMetaData metadata = connection.getMetaData();
    int major = metadata.getDatabaseMajorVersion();
    if (major <= 0) {
      throw new SQLException("PostgreSQL major version metadata was unavailable");
    }
    return new PostgresServerVersion(major);
  }

  public boolean supportsPgoutputOriginOption() {
    return majorVersion >= 16;
  }

  public boolean supportsPublicationNamespaceCatalog() {
    return majorVersion >= 15;
  }

  public boolean supportsPublicationRowFilters() {
    return majorVersion >= 15;
  }

  public boolean supportsReplicationSlotFailover() {
    return majorVersion >= 17;
  }

  public boolean supportsReplicationSlotInvalidationReason() {
    return majorVersion >= 17;
  }

  public boolean supportsReplicationSlotCreationFailoverArgument() {
    return majorVersion >= 17;
  }
}
