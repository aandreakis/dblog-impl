package io.github.aandreakis.dblog.adapter.postgres.internal;

import io.github.aandreakis.dblog.core.model.TableId;
import java.util.Objects;

/** Replica-identity state for one PostgreSQL table. */
public record PostgresReplicaIdentityState(
    TableId tableId, PostgresReplicaIdentity replicaIdentity) {
  public PostgresReplicaIdentityState {
    Objects.requireNonNull(tableId, "tableId");
    Objects.requireNonNull(replicaIdentity, "replicaIdentity");
  }
}
