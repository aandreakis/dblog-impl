package io.github.aandreakis.dblog.adapter.api;

import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.util.List;

/**
 * Adapter-owned preflight port. Each {@link SourceAdapter} exposes an implementation that
 * idempotently validates and (where applicable) ensures the source-side resources the adapter
 * requires before live streaming begins — e.g. PostgreSQL publication/logical-slot existence,
 * MySQL binary-logging configuration.
 *
 * <p>Preflight is invoked independently of {@link SourceAdapter#openRuntime} so that operator
 * tools (startup-check, dry-run validation) can verify source readiness without opening the
 * long-lived runtime. Implementations must be idempotent: adapters may invoke the same work
 * again internally when opening the runtime.
 */
public interface SourcePreflight {
  /**
   * Validate and, where applicable, ensure the source-side resources this adapter requires.
   *
   * @param config the source configuration for the current runtime
   * @param contractSchemas the captured contract schemas (required by some adapters to scope
   *     resource ensures such as publication membership)
   */
  void ensure(RelationalSourceConfig config, List<TableSchema> contractSchemas);
}
