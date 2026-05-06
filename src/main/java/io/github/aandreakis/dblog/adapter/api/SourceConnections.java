package io.github.aandreakis.dblog.adapter.api;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;

/**
 * Named multi-connection facet bundle held by an adapter for the lifetime of an opened source
 * session.
 *
 * <p>Always present: {@link #sql()} — the JDBC connection used for metadata, chunk reads,
 * watermark writes, and heartbeats.
 *
 * <p>Optional: {@link #replication()} — a second JDBC connection the adapter uses for live
 * streaming (PostgreSQL's {@code replication=database} connection, future Oracle XStream Out).
 *
 * <p>Optional: {@link #miner()} — a dedicated mining-session connection for adapters that need
 * one (future Oracle LogMiner). Currently unused; present to document the intended seam.
 *
 * <p>{@link #close()} closes every held connection with suppressed-exception semantics so partial
 * teardown after a mid-setup failure cannot leak a socket.
 */
public final class SourceConnections implements AutoCloseable {
  private final Connection sql;
  private final Connection replication;
  private final Connection miner;

  private SourceConnections(Connection sql, Connection replication, Connection miner) {
    this.sql = Objects.requireNonNull(sql, "sql");
    this.replication = replication;
    this.miner = miner;
  }

  /** Build a single-connection bundle for adapters that only need a JDBC SQL link. */
  public static SourceConnections ofSql(Connection sql) {
    return new SourceConnections(sql, null, null);
  }

  /**
   * Build a bundle with a dedicated replication connection, e.g. PostgreSQL's logical replication
   * slot consumer.
   */
  public static SourceConnections ofSqlAndReplication(Connection sql, Connection replication) {
    Objects.requireNonNull(replication, "replication");
    return new SourceConnections(sql, replication, null);
  }

  public Connection sql() {
    return sql;
  }

  public Optional<Connection> replication() {
    return Optional.ofNullable(replication);
  }

  public Optional<Connection> miner() {
    return Optional.ofNullable(miner);
  }

  @Override
  public void close() throws SQLException {
    SQLException firstFailure = null;
    firstFailure = closeQuietly(miner, firstFailure);
    firstFailure = closeQuietly(replication, firstFailure);
    firstFailure = closeQuietly(sql, firstFailure);
    if (firstFailure != null) {
      throw firstFailure;
    }
  }

  private static SQLException closeQuietly(Connection connection, SQLException previous) {
    if (connection == null) {
      return previous;
    }
    try {
      connection.close();
      return previous;
    } catch (SQLException failure) {
      if (previous == null) {
        return failure;
      }
      previous.addSuppressed(failure);
      return previous;
    }
  }
}
