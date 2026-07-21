package io.github.aandreakis.dblog.adapter.api;

import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

/**
 * Adapter-owned reader for snapshot chunks selected during dump and targeted-repair work. Each
 * call runs against a caller-supplied {@link Connection}; ownership of the connection — opening,
 * transaction scope, closing — stays with the caller. The chunk-window methods are invoked inside
 * an open watermark window (paper Algorithm 1 step 3); the upper-bound capture runs outside.
 *
 * <h2>Bounds</h2>
 *
 * For full-table reads, {@code startAfterPrimaryKey} is <b>exclusive</b> (the primary key
 * strictly above this resumption point is the first candidate row) and {@code stopAtPrimaryKey}
 * is <b>inclusive</b> (the request-scoped upper bound captured once via
 * {@link #tableScanUpperBoundPrimaryKeyTuple} is itself a candidate). Either bound may be {@code
 * null} on the first chunk of a fresh dump or for unbounded scans.
 *
 * <h2>Empty results</h2>
 *
 * {@code Optional.empty()} from a chunk method means "no rows in the requested primary-key
 * range" — not "table missing" or "transient failure". The coordinator treats it as the cue to
 * run a drain-only watermark window and finalise the table.
 */
public interface SourceChunkReader {
  /**
   * Captures the table's largest primary key. Called once per dump request before any chunk is
   * selected; the captured value pins the request-scoped upper bound so subsequent INSERTs above
   * it are not part of this request's dump and are picked up via the live log instead. Returns
   * empty when the table has no rows.
   */
  Optional<PrimaryKeyTuple> tableScanUpperBoundPrimaryKeyTuple(
      Connection connection, TableSchema schema) throws SQLException;

  Optional<Chunk> nextTableChunk(
      Connection connection,
      String jobId,
      TableSchema schema,
      PrimaryKeyTuple startAfterPrimaryKey,
      PrimaryKeyTuple stopAtPrimaryKey,
      int chunkSize)
      throws SQLException;

  /**
   * Reads the rows for an explicit list of primary keys (targeted-repair path). Duplicates in the
   * input list are de-duplicated by the implementation; the returned chunk contains only rows
   * that exist on the source — missing keys do not raise an error. For source-defined equality
   * such as collated text, the chunk records the requested tuples that the source matched even
   * when their literals differ from the stored row keys. {@code Optional.empty()} when the input
   * list is empty after de-dup.
   */
  Optional<Chunk> targetedPrimaryKeyTuples(
      Connection connection,
      String jobId,
      TableSchema schema,
      List<PrimaryKeyTuple> requestedPrimaryKeys)
      throws SQLException;
}
