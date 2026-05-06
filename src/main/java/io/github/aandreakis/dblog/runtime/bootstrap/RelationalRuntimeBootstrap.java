package io.github.aandreakis.dblog.runtime.bootstrap;

import io.github.aandreakis.dblog.adapter.api.OpenedSourceRuntime;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.SourceAdapter;
import io.github.aandreakis.dblog.adapter.api.SourceRuntime;
import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.adapter.api.WatermarkWindowRuntime;
import io.github.aandreakis.dblog.core.schema.FullDumpRequiredSignal;
import io.github.aandreakis.dblog.core.schema.SchemaDriftException;
import io.github.aandreakis.dblog.core.schema.SchemaPolicyEngine;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.runtime.host.RetryingWatermarkWindowRuntime;
import io.github.aandreakis.dblog.runtime.host.RuntimeSession;
import io.github.aandreakis.dblog.runtime.telemetry.RuntimeMeasurementWrappers;
import io.github.aandreakis.dblog.sink.api.ChangeEventSink;
import io.github.aandreakis.dblog.sink.api.SinkSchemaValidator;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import io.github.aandreakis.dblog.tap.NoopTap;
import io.github.aandreakis.dblog.tap.Tap;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class RelationalRuntimeBootstrap {
  private final SchemaPolicyEngine schemaPolicyEngine = new SchemaPolicyEngine();

  public BootstrappedSession open(BootstrapRequest request) throws Exception {
    Objects.requireNonNull(request, "request");
    request.adapter().validateSourceConfig(request.sourceConfig());
    SourceRetrySettings retrySettings =
        sourceRetrySettings(request.adapter().key(), request.sourceConfig());
    request.stateStore().ownership().claimSourceOwnership(request.sourceConfig().sourceId());

    List<TableSchema> liveSchemas = List.copyOf(request.adapter().inspectSchemas(request.sourceConfig()));
    List<TableSchema> contractSchemas =
        prepareContractSchemas(
            request.stateStore(), liveSchemas, request.sourceConfig().sourceId());
    validateSinkIfNeeded(request.sink(), contractSchemas);
    OpenedSourceRuntime<? extends SourceTransaction<?>> openedRuntime =
        request.adapter().openRuntime(
            request.sourceConfig(), request.stateStore(), contractSchemas, request.tap());
    openedRuntime =
        RuntimeMeasurementWrappers.instrumentOpenedRuntime(
            request.adapter().key(), request.meterRegistry(), openedRuntime);
    openedRuntime = wrapSourceRetryIfConfigured(request, contractSchemas, openedRuntime, retrySettings);
    RuntimeSession<? extends SourceTransaction<?>> session =
        new RuntimeSession<>(openedRuntime.runtime(), openedRuntime.chunkReader(), request.sink());
    return new BootstrappedSession(
        session, openedRuntime.loadedCheckpointDisplayValue(), liveSchemas, contractSchemas);
  }

  public List<TableSchema> prepareContractSchemas(
      RuntimeStateStore stateStore, List<TableSchema> liveSchemas) {
    return prepareContractSchemas(stateStore, liveSchemas, null);
  }

  public List<TableSchema> prepareContractSchemas(
      RuntimeStateStore stateStore, List<TableSchema> liveSchemas, String sourceId) {
    Objects.requireNonNull(stateStore, "stateStore");
    List<TableSchema> contractSchemas = new ArrayList<>(liveSchemas.size());
    for (TableSchema liveSchema : Objects.requireNonNull(liveSchemas, "liveSchemas")) {
      stateStore.schemas().saveObservedSchema(liveSchema);
      TableSchema acceptedSchema =
          stateStore.schemas().loadContractSchema(liveSchema.tableId().displayName())
              .map(contract -> reconcileStartupContract(stateStore, sourceId, contract, liveSchema))
              .orElse(liveSchema);
      stateStore.schemas().saveContractSchema(acceptedSchema);
      contractSchemas.add(acceptedSchema);
    }
    return List.copyOf(contractSchemas);
  }

  private TableSchema reconcileStartupContract(
      RuntimeStateStore stateStore, String sourceId, TableSchema contract, TableSchema liveSchema) {
    try {
      return schemaPolicyEngine.reconcileStrict(contract, liveSchema, "startup");
    } catch (SchemaDriftException drift) {
      if (sourceId != null && !sourceId.isBlank()) {
        stateStore
            .schemas()
            .saveFullDumpRequiredSignal(
                new FullDumpRequiredSignal(
                    sourceId, liveSchema.tableId(), drift.getMessage(), Instant.now()));
      }
      throw drift;
    }
  }

  private void validateSinkIfNeeded(ChangeEventSink sink, List<TableSchema> contractSchemas) {
    if (sink instanceof SinkSchemaValidator validator) {
      validator.validateCapturedSchemas(contractSchemas);
    }
  }

  private OpenedSourceRuntime<? extends SourceTransaction<?>> wrapSourceRetryIfConfigured(
      BootstrapRequest request,
      List<TableSchema> contractSchemas,
      OpenedSourceRuntime<? extends SourceTransaction<?>> openedRuntime,
      SourceRetrySettings retrySettings) {
    if (!retrySettings.enabled()) {
      return openedRuntime;
    }
    SourceRuntime<? extends SourceTransaction<?>> runtime = openedRuntime.runtime();
    if (!(runtime instanceof WatermarkWindowRuntime<?> watermarkRuntime)) {
      return openedRuntime;
    }
    @SuppressWarnings({"rawtypes", "unchecked"})
    OpenedSourceRuntime<? extends SourceTransaction<?>> retryingRuntime =
        new OpenedSourceRuntime(
            new RetryingWatermarkWindowRuntime(
                request.adapter().key(),
                request.meterRegistry(),
                (WatermarkWindowRuntime) watermarkRuntime,
                retrySettings.reconnectBackoff(),
                () -> reopenRetryHandle(request, contractSchemas)),
            openedRuntime.chunkReader(),
            openedRuntime.loadedCheckpointDisplayValue());
    return retryingRuntime;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private WatermarkWindowRuntime reopenRetryHandle(
      BootstrapRequest request, List<TableSchema> contractSchemas) throws SQLException {
    try {
      // Reopen keeps the startup contract. Schema drift must fail closed through the streaming
      // path rather than widening the accepted schema during an availability retry.
      OpenedSourceRuntime<? extends SourceTransaction<?>> reopenedRuntime =
          request.adapter().openRuntime(
              request.sourceConfig(), request.stateStore(), contractSchemas, request.tap());
      reopenedRuntime =
          RuntimeMeasurementWrappers.instrumentOpenedRuntime(
              request.adapter().key(), request.meterRegistry(), reopenedRuntime);
      SourceRuntime<? extends SourceTransaction<?>> runtime = reopenedRuntime.runtime();
      if (runtime instanceof WatermarkWindowRuntime<?> watermarkRuntime) {
        return (WatermarkWindowRuntime) watermarkRuntime;
      }
      closeQuietly(runtime);
      throw new SQLException(
          request.adapter().displayName()
              + " source runtime no longer exposes the watermark-window retry surface");
    } catch (RuntimeException failure) {
      throw sqlExceptionForReopenFailure(request.adapter().displayName(), failure);
    }
  }

  private static boolean retryLogConnectionLoss(
      String adapterKey, RelationalSourceConfig sourceConfig) {
    String raw = sourceConfig.options().get(adapterKey + ".retryLogConnectionLoss");
    return raw != null && Boolean.parseBoolean(raw.trim());
  }

  private static SourceRetrySettings sourceRetrySettings(
      String adapterKey, RelationalSourceConfig sourceConfig) {
    if (!retryLogConnectionLoss(adapterKey, sourceConfig)) {
      return new SourceRetrySettings(false, null);
    }
    return new SourceRetrySettings(true, reconnectBackoff(adapterKey, sourceConfig));
  }

  private static Duration reconnectBackoff(String adapterKey, RelationalSourceConfig sourceConfig) {
    String raw = sourceConfig.options().get(adapterKey + ".reconnectBackoff");
    Duration reconnectBackoff;
    if (raw == null || raw.isBlank()) {
      reconnectBackoff = Duration.ofSeconds(3);
    } else {
      String value = raw.trim();
      try {
        reconnectBackoff = Duration.parse(value);
      } catch (DateTimeParseException failure) {
        throw new IllegalArgumentException(
            adapterKey
                + ".reconnectBackoff must be an ISO-8601 duration, e.g. PT2S; got \""
                + value
                + "\"",
            failure);
      }
    }
    if (reconnectBackoff.isZero() || reconnectBackoff.isNegative()) {
      throw new IllegalArgumentException(adapterKey + ".reconnectBackoff must be > 0");
    }
    return reconnectBackoff;
  }

  private record SourceRetrySettings(boolean enabled, Duration reconnectBackoff) {
    private SourceRetrySettings {
      if (enabled) {
        Objects.requireNonNull(reconnectBackoff, "reconnectBackoff");
      }
    }
  }

  private static SQLException sqlExceptionForReopenFailure(
      String adapterDisplayName, RuntimeException failure) {
    Throwable current = failure;
    while (current != null) {
      if (current instanceof SQLException sqlException) {
        return sqlException;
      }
      current = current.getCause();
    }
    return new SQLException(adapterDisplayName + " source runtime reopen failed", failure);
  }

  private static void closeQuietly(SourceRuntime<? extends SourceTransaction<?>> runtime) {
    try {
      runtime.close();
    } catch (Exception ignored) {
      // The retry loop will surface the reopen failure; close failure is secondary cleanup noise.
    }
  }

  public record BootstrapRequest(
      SourceAdapter adapter,
      RelationalSourceConfig sourceConfig,
      RuntimeStateStore stateStore,
      ChangeEventSink sink,
      MeterRegistry meterRegistry,
      Tap tap) {
    public BootstrapRequest {
      Objects.requireNonNull(adapter, "adapter");
      Objects.requireNonNull(sourceConfig, "sourceConfig");
      Objects.requireNonNull(stateStore, "stateStore");
      Objects.requireNonNull(sink, "sink");
      tap = tap == null ? NoopTap.INSTANCE : tap;
    }

    public BootstrapRequest(
        SourceAdapter adapter,
        RelationalSourceConfig sourceConfig,
        RuntimeStateStore stateStore,
        ChangeEventSink sink) {
      this(adapter, sourceConfig, stateStore, sink, null, NoopTap.INSTANCE);
    }

    public BootstrapRequest(
        SourceAdapter adapter,
        RelationalSourceConfig sourceConfig,
        RuntimeStateStore stateStore,
        ChangeEventSink sink,
        MeterRegistry meterRegistry) {
      this(adapter, sourceConfig, stateStore, sink, meterRegistry, NoopTap.INSTANCE);
    }
  }

  public record BootstrappedSession(
      RuntimeSession<? extends SourceTransaction<?>> session,
      String loadedCheckpointDisplayValue,
      List<TableSchema> liveSchemas,
      List<TableSchema> contractSchemas) {
    public BootstrappedSession {
      Objects.requireNonNull(session, "session");
      liveSchemas = List.copyOf(Objects.requireNonNull(liveSchemas, "liveSchemas"));
      contractSchemas = List.copyOf(Objects.requireNonNull(contractSchemas, "contractSchemas"));
    }
  }
}
