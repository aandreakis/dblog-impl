package io.github.aandreakis.dblog.adapter.api;

import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface SourceRuntime<TX extends SourceTransaction<?>> extends AutoCloseable {
  /**
   * Returns the next pending source transaction, if one is available.
   *
   * <p><b>Single-consumer contract:</b> this method must be invoked from exactly one thread — the
   * runtime orchestrator (see {@code RuntimeRequestPump#runUntilStopped}). This is the mechanism
   * by which the DBLog paper's Algorithm 1 step 1 ("pause log event processing") is satisfied:
   * because a single thread is either draining transactions to the sink <em>or</em> running a
   * watermark window, never both concurrently, events cannot leak to the sink during an open
   * window. Running a second concurrent consumer would silently break history-order preservation.
   *
   * <p>Implementations may enforce this at runtime when {@code -Ddblog.assertions=true} is set.
   */
  Optional<TX> readPendingTransaction() throws SQLException;

  void acknowledge(TX transaction) throws SQLException;

  default boolean emitHeartbeatIfDue(Instant heartbeatTime, Duration minimumInterval)
      throws SQLException {
    return false;
  }

  default List<TableSchema> currentCapturedSchemas() {
    return List.of();
  }

  @Override
  default void close() throws Exception {}
}
