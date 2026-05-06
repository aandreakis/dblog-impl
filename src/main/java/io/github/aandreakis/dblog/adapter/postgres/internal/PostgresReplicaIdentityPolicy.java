package io.github.aandreakis.dblog.adapter.postgres.internal;

import java.util.Objects;

/** Small fail-closed policy helper around PostgreSQL replica identity. */
public final class PostgresReplicaIdentityPolicy {
  public void requireReplicaIdentityFull(PostgresReplicaIdentityState state) {
    Objects.requireNonNull(state, "state");
    if (state.replicaIdentity() != PostgresReplicaIdentity.FULL) {
      throw new IllegalStateException(
          "captured PostgreSQL tables must use REPLICA IDENTITY FULL: "
              + state.tableId().displayName()
              + " uses "
              + state.replicaIdentity());
    }
  }
}
