package io.github.aandreakis.dblog.state.api;

/**
 * Records which {@code sourceId} the local state store belongs to so a misconfigured restart
 * cannot silently reuse another source's checkpoints, dump progress, or schemas. The first claim
 * wins; a subsequent claim with a different {@code sourceId} must fail loudly.
 */
public interface SourceOwnershipRepository {
  void claimSourceOwnership(String sourceId);
}
