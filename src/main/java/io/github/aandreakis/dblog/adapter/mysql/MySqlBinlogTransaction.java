package io.github.aandreakis.dblog.adapter.mysql;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * One committed MySQL transaction in the adapter.
 *
 * <p>This is intentionally simple for now: it carries already-materialized neutral events.
 */
public record MySqlBinlogTransaction(
    String transactionId,
    String gtid,
    MySqlSourcePosition checkpointPosition,
    Instant commitTimestamp,
    List<ChangeEvent> events)
    implements SourceTransaction<MySqlSourcePosition> {
  public MySqlBinlogTransaction {
    transactionId = requireNonBlank(transactionId, "transactionId");
    checkpointPosition = Objects.requireNonNull(checkpointPosition, "checkpointPosition");
    commitTimestamp = Objects.requireNonNull(commitTimestamp, "commitTimestamp");
    events = List.copyOf(Objects.requireNonNull(events, "events"));
    gtid = gtid == null || gtid.isBlank() ? null : gtid;
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
