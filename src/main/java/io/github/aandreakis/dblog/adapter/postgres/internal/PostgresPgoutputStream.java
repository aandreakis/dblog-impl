package io.github.aandreakis.dblog.adapter.postgres.internal;

import io.github.aandreakis.dblog.adapter.postgres.PostgresLsn;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Optional;

/** Small abstraction over a live PostgreSQL logical-replication stream. */
public interface PostgresPgoutputStream extends AutoCloseable {
  Optional<ByteBuffer> readPending() throws SQLException;

  Optional<PostgresLsn> lastReceiveLsn() throws SQLException;

  void setAppliedLsn(PostgresLsn lsn) throws SQLException;

  void setFlushedLsn(PostgresLsn lsn) throws SQLException;

  void forceUpdateStatus() throws SQLException;

  @Override
  void close() throws SQLException;
}
