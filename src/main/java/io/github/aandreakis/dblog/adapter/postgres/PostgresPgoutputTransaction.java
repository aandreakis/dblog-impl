package io.github.aandreakis.dblog.adapter.postgres;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * One committed PostgreSQL logical-replication transaction in the adapter.
 *
 * <p>This is intentionally simple for now and carries already-materialized neutral events.
 */
public record PostgresPgoutputTransaction(
    String transactionId,
    PostgresLsn beginFinalLsn,
    PostgresLsn commitLsn,
    PostgresLsn endLsn,
    PostgresLsn checkpointLsn,
    Instant commitTimestamp,
    List<ChangeEvent> events)
    implements SourceTransaction<PostgresLsn> {
  public PostgresPgoutputTransaction {
    transactionId = requireNonBlank(transactionId, "transactionId");
    beginFinalLsn = Objects.requireNonNull(beginFinalLsn, "beginFinalLsn");
    commitLsn = Objects.requireNonNull(commitLsn, "commitLsn");
    endLsn = Objects.requireNonNull(endLsn, "endLsn");
    checkpointLsn = Objects.requireNonNull(checkpointLsn, "checkpointLsn");
    commitTimestamp = Objects.requireNonNull(commitTimestamp, "commitTimestamp");
    events = List.copyOf(Objects.requireNonNull(events, "events"));
  }

  @Override
  public PostgresLsn checkpointPosition() {
    return checkpointLsn;
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
