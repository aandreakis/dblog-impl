package io.github.aandreakis.dblog.config;

/** Explicit ownership mode for PostgreSQL publications and logical replication slots. */
public enum PostgresResourceOwnership {
  DBLOG_MANAGED,
  EXTERNALLY_MANAGED;

  public boolean isDblogManaged() {
    return this == DBLOG_MANAGED;
  }
}
