package io.github.aandreakis.dblog.adapter.api;

import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Binding of a {@link Connection} to a {@link SourceChunkReader}, exposing the Connection-free
 * {@link BoundChunkReader} port. Adapter runtime implementations construct this inside their
 * {@code executeChunkRead} / {@code executeChunkReadInWatermarkWindow} methods so the semantic
 * core never sees a raw {@link Connection} on its lambda.
 */
public final class ConnectionBoundChunkReader implements BoundChunkReader {
  private final Connection connection;
  private final SourceChunkReader reader;

  public ConnectionBoundChunkReader(Connection connection, SourceChunkReader reader) {
    // connection is allowed to be null — in-memory test fakes frequently invoke chunk-reader
    // stubs that ignore the connection argument. Real runtime implementations always bind a
    // non-null connection via their adapter.
    this.connection = connection;
    this.reader = Objects.requireNonNull(reader, "reader");
  }

  @Override
  public Optional<PrimaryKeyTuple> tableScanUpperBoundPrimaryKeyTuple(TableSchema schema)
      throws SQLException {
    return reader.tableScanUpperBoundPrimaryKeyTuple(connection, schema);
  }

  @Override
  public Optional<Chunk> nextTableChunk(
      String jobId,
      TableSchema schema,
      PrimaryKeyTuple startAfterPrimaryKey,
      PrimaryKeyTuple stopAtPrimaryKey,
      int chunkSize)
      throws SQLException {
    return reader.nextTableChunk(
        connection, jobId, schema, startAfterPrimaryKey, stopAtPrimaryKey, chunkSize);
  }

  @Override
  public Optional<Chunk> targetedPrimaryKeyTuples(
      String jobId, TableSchema schema, List<PrimaryKeyTuple> requestedPrimaryKeys)
      throws SQLException {
    return reader.targetedPrimaryKeyTuples(connection, jobId, schema, requestedPrimaryKeys);
  }
}
