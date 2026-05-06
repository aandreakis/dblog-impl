package io.github.aandreakis.dblog.state.api;

import io.github.aandreakis.dblog.core.request.DumpTableProgress;
import java.util.Optional;

/**
 * Durable per-table dump progress. Each row is keyed by {@code (jobId, tableName)} and pins one
 * request's chunk-boundary state so a restart can retry the active chunk from the last completed
 * primary key.
 *
 * <p>The persisted shape is intentionally chunk-boundary-only. {@link DumpTableProgress} enforces
 * the invariants; this interface only owns durability.
 */
public interface DumpProgressRepository {
  void save(DumpTableProgress progress);

  Optional<DumpTableProgress> load(String jobId, String tableName);

  /**
   * Removes one progress row. Destroys any recovery state captured for the row's active or
   * completed chunk.
   */
  void delete(String jobId, String tableName);

  /** Removes every progress row across all jobs and tables. Not part of normal request lifecycle. */
  void deleteAll();
}
