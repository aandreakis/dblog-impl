package io.github.aandreakis.dblog.adapter.mysql.internal;

import io.github.aandreakis.dblog.adapter.mysql.MySqlBinlogTransaction;
import io.github.aandreakis.dblog.runtime.sql.SourceFlowControlSnapshot;
import java.sql.SQLException;
import java.util.Optional;

/** Narrow committed-transaction stream seam for MySQL live runtimes. */
public interface MySqlTransactionStream extends AutoCloseable {
  Optional<MySqlBinlogTransaction> readPendingTransaction() throws SQLException;

  default SourceFlowControlSnapshot sourceFlowControlSnapshot() {
    return SourceFlowControlSnapshot.directPoll();
  }

  @Override
  default void close() throws Exception {}
}
