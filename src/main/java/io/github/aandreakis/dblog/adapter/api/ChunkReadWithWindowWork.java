package io.github.aandreakis.dblog.adapter.api;

import io.github.aandreakis.dblog.core.reconcile.WatermarkWindow;
import java.sql.SQLException;

/**
 * Chunk-read work that runs inside an open watermark window, receiving a {@link
 * BoundChunkReader} plus the {@link WatermarkWindow} so it can correlate reads against the
 * low/high watermarks without ever touching a raw {@code Connection}.
 */
@FunctionalInterface
public interface ChunkReadWithWindowWork<T> {
  T execute(BoundChunkReader reader, WatermarkWindow window) throws SQLException;
}
