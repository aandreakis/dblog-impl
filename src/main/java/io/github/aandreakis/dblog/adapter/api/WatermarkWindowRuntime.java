package io.github.aandreakis.dblog.adapter.api;

import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.sql.SQLException;

/**
 * Runtime that exposes a chunk-read seam bound to its open {@link SourceConnections}. Core
 * coordinators interact with runtimes exclusively through {@link #executeChunkRead} and {@link
 * #executeChunkReadInWatermarkWindow} so they never see a raw {@code java.sql.Connection}.
 *
 * <p>Adapter-internal operations that genuinely need a raw {@code Connection} (metadata writes,
 * heartbeat writes, integration-test watermark probes) live as concrete methods on each adapter's
 * runtime class — they are not part of this port.
 */
public interface WatermarkWindowRuntime<TX extends SourceTransaction<?>> extends SourceRuntime<TX> {
  /** Run a chunk-read against a connection-bound {@link BoundChunkReader}. */
  <T> T executeChunkRead(SourceChunkReader reader, ChunkReadWork<T> work) throws SQLException;

  /**
   * Open a watermark window, run a chunk-read against a connection-bound {@link BoundChunkReader},
   * and close the window.
   */
  <T> WatermarkWindowResult<T> executeChunkReadInWatermarkWindow(
      SourceChunkReader reader, ChunkReadWithWindowWork<T> work) throws SQLException;

  default void updateCapturedSchema(TableSchema schema) {
    // Optional for runtimes that do not maintain a mutable decode schema.
  }

  /**
   * Returns the runtime's run identifier, or {@code null} when the runtime does not stamp its
   * watermark writes with a run id. A non-null value enables {@link
   * io.github.aandreakis.dblog.core.reconcile.WindowReconciler} to drain stale own-run
   * watermark tokens left in the source log by a prior failed attempt within the same process,
   * instead of failing closed and looping. Foreign tokens still fail closed regardless.
   */
  default String currentRunId() {
    return null;
  }
}
