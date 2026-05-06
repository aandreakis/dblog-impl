package io.github.aandreakis.dblog.state.api;

/**
 * Aggregate root for the durable state DBLog needs across restarts. Exposes per-concern
 * repositories — stream positions, dump requests, dump progress, captured schemas, source
 * ownership — and one composite operation that must span them atomically.
 *
 * <p>Implementations are responsible for making each repository's writes durable on return; the
 * runtime relies on that durability for restart safety (see SPEC §14).
 */
public interface RuntimeStateStore {
  StreamPositionRepository streamPositions();

  DumpRequestRepository dumpRequests();

  DumpProgressRepository dumpProgress();

  SchemaStateRepository schemas();

  SourceOwnershipRepository ownership();

  /**
   * Atomically clears in-flight state that becomes meaningless when a captured table's primary
   * key changes shape: the bootstrap stream position, all dump-progress rows, and the schema
   * uncertainty signals for {@code sourceId}, then fails any non-terminal dump requests with
   * {@code reason}. Either every write commits or none does.
   */
  void invalidateRuntimeStateForPkChange(String sourceId, String reason);
}
