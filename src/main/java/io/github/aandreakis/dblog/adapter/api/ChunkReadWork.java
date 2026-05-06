package io.github.aandreakis.dblog.adapter.api;

import java.sql.SQLException;

/**
 * Unit of chunk-read work against a connection-bound {@link BoundChunkReader}. Used by the
 * semantic core's dump/repair coordinators so they never see a raw {@code Connection}.
 */
@FunctionalInterface
public interface ChunkReadWork<T> {
  T execute(BoundChunkReader reader) throws SQLException;
}
