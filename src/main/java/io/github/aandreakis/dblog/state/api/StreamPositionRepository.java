package io.github.aandreakis.dblog.state.api;

import io.github.aandreakis.dblog.core.model.SourcePosition;
import java.util.Optional;

/**
 * Durable per-{@code sourceId} stream positions. Two distinct positions are stored:
 *
 * <ul>
 *   <li><b>checkpoint</b> — the latest source position the runtime has acknowledged through; on
 *       restart the runtime resumes the live log from here. Advances only after the corresponding
 *       events are durable in the sink's downstream invariants (or in local progress for dump
 *       work).
 *   <li><b>bootstrap position</b> — the source position captured at first startup, before any
 *       checkpoint exists. Used to anchor the live log when an initial dump runs against a fresh
 *       state store. Becomes meaningless once a real checkpoint exists; saving a checkpoint
 *       implicitly clears it.
 * </ul>
 */
public interface StreamPositionRepository {
  void saveCheckpoint(String sourceId, SourcePosition position);

  Optional<SourcePosition> loadCheckpoint(String sourceId);

  /**
   * Persists a bootstrap position only if no real checkpoint already exists for {@code sourceId}.
   * The guard is enforced atomically inside the underlying store; concurrent checkpoint advance
   * cannot be undone by a stale bootstrap write.
   */
  void saveBootstrapPosition(String sourceId, SourcePosition position);

  Optional<SourcePosition> loadBootstrapPosition(String sourceId);

  void clearBootstrapPosition(String sourceId);
}
