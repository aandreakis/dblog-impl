package io.github.aandreakis.dblog.adapter.mysql.internal;

import io.github.aandreakis.dblog.adapter.mysql.MySqlSourcePosition;
import io.github.aandreakis.dblog.runtime.sql.SourceFlowControlSnapshot;
import java.sql.SQLException;
import java.util.Optional;

/** Thin wrapper around a live MySQL binlog message stream. */
public interface MySqlBinlogStream extends AutoCloseable {
  Optional<MySqlBinlogMessage> readMessage() throws SQLException;

  default Optional<MySqlSourcePosition> connectedPosition() {
    return Optional.empty();
  }

  default SourceFlowControlSnapshot sourceFlowControlSnapshot() {
    return SourceFlowControlSnapshot.unavailable();
  }

  @Override
  void close() throws SQLException;
}
