package io.github.aandreakis.dblog.runtime.bootstrap;

import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.SourceAdapter;
import io.github.aandreakis.dblog.boot.SourceAdapterRegistry;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.sink.api.ChangeEventSink;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import java.util.List;
import java.util.Objects;

public final class StartupCheckRunner {
  private final SourceAdapterRegistry adapterRegistry;
  private final RelationalRuntimeBootstrap bootstrap;

  public StartupCheckRunner(SourceAdapterRegistry adapterRegistry) {
    this(adapterRegistry, new RelationalRuntimeBootstrap());
  }

  StartupCheckRunner(SourceAdapterRegistry adapterRegistry, RelationalRuntimeBootstrap bootstrap) {
    this.adapterRegistry = Objects.requireNonNull(adapterRegistry, "adapterRegistry");
    this.bootstrap = Objects.requireNonNull(bootstrap, "bootstrap");
  }

  public StartupCheckResult run(StartupCheckRequest request) throws Exception {
    Objects.requireNonNull(request, "request");
    SourceAdapter adapter = adapterRegistry.require(requireNonBlank(request.adapterKey(), "adapterKey"));
    // Exercise adapter-owned preflight as an explicit step so startup-check exposes source-
    // readiness failures (e.g. PostgreSQL publication/slot, MySQL binlog configuration) before
    // the long-lived runtime is opened. The adapter runs the same preflight again inside
    // openRuntime; both invocations are idempotent.
    adapter.validateSourceConfig(request.sourceConfig());
    List<TableSchema> contractSchemas = adapter.inspectSchemas(request.sourceConfig());
    adapter.preflight().ensure(request.sourceConfig(), contractSchemas);
    RelationalRuntimeBootstrap.BootstrappedSession session =
        bootstrap.open(
            new RelationalRuntimeBootstrap.BootstrapRequest(
                adapter, request.sourceConfig(), request.stateStore(), request.sink()));
    try {
      return new StartupCheckResult(
          adapter.key(),
          adapter.displayName(),
          request.sourceConfig().sourceId(),
          session.loadedCheckpointDisplayValue(),
          session.liveSchemas().size(),
          session.contractSchemas().size());
    } finally {
      session.session().close();
    }
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  public record StartupCheckRequest(
      String adapterKey,
      RelationalSourceConfig sourceConfig,
      RuntimeStateStore stateStore,
      ChangeEventSink sink) {
    public StartupCheckRequest {
      Objects.requireNonNull(adapterKey, "adapterKey");
      Objects.requireNonNull(sourceConfig, "sourceConfig");
      Objects.requireNonNull(stateStore, "stateStore");
      Objects.requireNonNull(sink, "sink");
    }
  }

  public record StartupCheckResult(
      String adapterKey,
      String adapterDisplayName,
      String sourceId,
      String loadedCheckpointDisplayValue,
      int liveSchemaCount,
      int contractSchemaCount) {
    public StartupCheckResult {
      Objects.requireNonNull(adapterKey, "adapterKey");
      Objects.requireNonNull(adapterDisplayName, "adapterDisplayName");
      Objects.requireNonNull(sourceId, "sourceId");
      if (liveSchemaCount < 0) {
        throw new IllegalArgumentException("liveSchemaCount must be >= 0");
      }
      if (contractSchemaCount < 0) {
        throw new IllegalArgumentException("contractSchemaCount must be >= 0");
      }
    }
  }
}
