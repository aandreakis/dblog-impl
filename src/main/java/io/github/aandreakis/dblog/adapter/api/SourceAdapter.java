package io.github.aandreakis.dblog.adapter.api;

import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import io.github.aandreakis.dblog.tap.NoopTap;
import io.github.aandreakis.dblog.tap.Tap;
import java.util.List;

public interface SourceAdapter {
  String key();

  String displayName();

  /**
   * Returns the adapter-owned {@link SourceDialect}. The dialect is the single place all
   * dialect-specific knowledge lives — JDBC URL shape, captured-table name layout, neutral type
   * mapping, and native-config validation — so the semantic core and generic runtime never need
   * to know which concrete source database is active.
   */
  SourceDialect dialect();

  default void validateSourceConfig(RelationalSourceConfig config) {
    dialect().validateNativeConfig(config);
  }

  /**
   * Returns the adapter-owned preflight port. Callers may invoke {@link SourcePreflight#ensure}
   * independently of {@link #openRuntime} — e.g. from startup-check or operator validation —
   * to verify source readiness without opening the long-lived runtime.
   */
  SourcePreflight preflight();

  List<TableSchema> inspectSchemas(RelationalSourceConfig config);

  /**
   * Open all JDBC connections this adapter's live runtime needs. Callers receive a {@link
   * SourceConnections} bundle whose named facets — {@code sql()}, {@code replication()}, and
   * {@code miner()} — make multi-connection ownership explicit and closable with one call. Single-
   * connection adapters populate only {@code sql()}; PostgreSQL populates {@code sql() +
   * replication()}; future Oracle LogMiner would populate {@code sql() + miner()}.
   *
   * <p>The default throws — adapters whose {@code openRuntime} delegates through this method must
   * override, but test fakes that implement {@code openRuntime(config, state, schemas)} directly
   * do not need to.
   */
  default SourceConnections openConnections(RelationalSourceConfig config) {
    throw new UnsupportedOperationException(
        "openConnections is not implemented for adapter " + key());
  }

  /**
   * Open the source runtime.
   *
   * <p>Implementations must re-run the adapter-owned source readiness checks required for live
   * streaming. The runtime retry path reopens through {@code openRuntime(...)} directly after a
   * source availability loss; it does not invoke {@link #preflight()} as a separate step.
   */
  OpenedSourceRuntime<? extends SourceTransaction<?>> openRuntime(
      RelationalSourceConfig config, RuntimeStateStore stateStore, List<TableSchema> contractSchemas);

  /**
   * Tap-aware overload. Production adapters override this to thread {@link Tap} into their live
   * runtime constructor so that watermark-related events surface on the educational tap stream.
   * The default discards the tap, keeping older adapters (and test doubles) working without
   * changes; they simply don't emit watermark.written events. The same readiness-check contract as
   * {@link #openRuntime(RelationalSourceConfig, RuntimeStateStore, List)} applies to this overload.
   */
  default OpenedSourceRuntime<? extends SourceTransaction<?>> openRuntime(
      RelationalSourceConfig config,
      RuntimeStateStore stateStore,
      List<TableSchema> contractSchemas,
      Tap tap) {
    return openRuntime(config, stateStore, contractSchemas);
  }
}
