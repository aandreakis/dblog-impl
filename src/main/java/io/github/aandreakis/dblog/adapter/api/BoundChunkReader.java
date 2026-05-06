package io.github.aandreakis.dblog.adapter.api;

import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

/**
 * Connection-bound view of {@link SourceChunkReader} that the semantic core uses during
 * watermark-window coordination. Binding {@code (Connection, SourceChunkReader)} into this
 * narrow port keeps raw JDBC out of the core's call path.
 *
 * <p>Adapter internals continue to use {@code SourceChunkReader} with an explicit {@code
 * Connection} — that is the natural convenience inside a JDBC-native adapter module. This port
 * only exists to hide the {@code Connection} seam from the core.
 */
public interface BoundChunkReader {
  Optional<PrimaryKeyTuple> tableScanUpperBoundPrimaryKeyTuple(TableSchema schema) throws SQLException;

  Optional<Chunk> nextTableChunk(
      String jobId,
      TableSchema schema,
      PrimaryKeyTuple startAfterPrimaryKey,
      PrimaryKeyTuple stopAtPrimaryKey,
      int chunkSize)
      throws SQLException;

  Optional<Chunk> targetedPrimaryKeyTuples(
      String jobId, TableSchema schema, List<PrimaryKeyTuple> requestedPrimaryKeys)
      throws SQLException;
}
