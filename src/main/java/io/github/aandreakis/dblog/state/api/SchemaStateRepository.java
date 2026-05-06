package io.github.aandreakis.dblog.state.api;

import io.github.aandreakis.dblog.core.schema.FullDumpRequiredSignal;
import io.github.aandreakis.dblog.core.schema.SchemaUncertaintySignal;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.util.List;
import java.util.Optional;

/**
 * Durable schema state for captured tables and the fail-closed signals raised against them.
 *
 * <h2>Two schema roles per table</h2>
 *
 * <ul>
 *   <li><b>contract schema</b> — the selected-column surface DBLog has committed to emit. Pinned
 *       at startup and used to fingerprint dump progress; mid-run drift against this contract is
 *       a stop-the-world boundary, not a feature.
 *   <li><b>observed schema</b> — the latest live source schema seen by the adapter. May contain
 *       extra unselected columns the contract ignores; tracked separately so policy decisions can
 *       compare contract vs. observed without overwriting the emitted-row shape.
 * </ul>
 *
 * <p>{@code save*} calls are upserts keyed by table display name; {@code load*} returns the most
 * recent stored value.
 *
 * <h2>Fail-closed signals</h2>
 *
 * <ul>
 *   <li>{@link FullDumpRequiredSignal} — destructive or unrecoverable conditions (TRUNCATE, live
 *       PK update, key-only old tuples) where only a fresh dump can restore correctness.
 *   <li>{@link SchemaUncertaintySignal} — DBLog cannot prove safety for the table; recorded with
 *       first/last seen timestamps so operators can distinguish a transient anomaly from a stuck
 *       condition.
 * </ul>
 */
public interface SchemaStateRepository {
  void saveContractSchema(TableSchema schema);

  Optional<TableSchema> loadContractSchema(String tableDisplayName);

  List<TableSchema> loadAllContractSchemas();

  void saveObservedSchema(TableSchema schema);

  Optional<TableSchema> loadObservedSchema(String tableDisplayName);

  List<TableSchema> loadAllObservedSchemas();

  void saveFullDumpRequiredSignal(FullDumpRequiredSignal signal);

  List<FullDumpRequiredSignal> loadFullDumpRequiredSignals();

  /**
   * Records or refreshes an uncertainty signal for one table. Implementations must merge against
   * any existing entry — preserving {@code firstDetectedAt}, advancing {@code lastDetectedAt},
   * and incrementing {@code occurrenceCount} — rather than overwriting it.
   */
  void saveSchemaUncertaintySignal(SchemaUncertaintySignal signal);

  void clearSchemaUncertaintySignal(String sourceId, String tableDisplayName);

  void deleteSchemaUncertaintySignals(String sourceId);

  List<SchemaUncertaintySignal> loadSchemaUncertaintySignals();
}
