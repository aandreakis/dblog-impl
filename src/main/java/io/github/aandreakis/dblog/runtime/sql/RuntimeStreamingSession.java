package io.github.aandreakis.dblog.runtime.sql;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

/**
 * Narrow streaming-session seam for source runtimes.
 *
 * <p>Concrete adapter sessions can own their own decode mechanics, but the shared runtime layer
 * only needs this small surface to drain transactions, advance checkpoints, and observe schema and
 * flow-control state.
 */
public interface RuntimeStreamingSession<TX extends SourceTransaction<?>> extends AutoCloseable {
  Optional<TX> readPendingTransaction() throws SQLException;

  void acknowledge(TX transaction);

  List<TableSchema> currentCapturedSchemas();

  void updateCapturedSchema(TableSchema schema);

  default SourceFlowControlSnapshot sourceFlowControlSnapshot() {
    return SourceFlowControlSnapshot.unavailable();
  }

  @Override
  default void close() throws Exception {}
}
