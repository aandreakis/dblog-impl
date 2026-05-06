package io.github.aandreakis.dblog.runtime.bootstrap;

import io.github.aandreakis.dblog.adapter.api.CommittedTransactionIngress;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.RuntimeStatusInspectable;
import io.github.aandreakis.dblog.adapter.api.SourceAdapter;
import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.controlplane.http.ControlPlaneHttpServer;
import io.github.aandreakis.dblog.controlplane.service.AsyncControlPlaneEventStore;
import io.github.aandreakis.dblog.controlplane.service.ControlPlaneCommandService;
import io.github.aandreakis.dblog.controlplane.service.ControlPlaneEventStore;
import io.github.aandreakis.dblog.controlplane.service.ControlPlaneQueryService;
import io.github.aandreakis.dblog.controlplane.service.InMemoryControlPlaneEventStore;
import io.github.aandreakis.dblog.controlplane.service.RecordingChangeEventSink;
import io.github.aandreakis.dblog.controlplane.service.RequestSubmissionGateway;
import io.github.aandreakis.dblog.controlplane.service.RuntimeStatusProvider;
import io.github.aandreakis.dblog.core.checkpoint.CheckpointFlushPolicy;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.runtime.observer.RuntimeLoopObserver;
import io.github.aandreakis.dblog.runtime.sql.SourceFlowControlSnapshot;
import io.github.aandreakis.dblog.sink.api.ChangeEventSink;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import io.github.aandreakis.dblog.tap.NoopTap;
import io.github.aandreakis.dblog.tap.Tap;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reusable DBLog application surface over the bootstrap and control-plane layers.
 *
 * <p>This is intentionally narrow: it owns one bootstrapped runtime stack, exposes runtime
 * status, accepts request submissions into durable state, and can drive pending request processing
 * on demand. The same surface backs both inspection-only harnesses (tests) and the long-running
 * {@code --dblog.boot-mode=runtime} process — the {@code bootMode} label passed at construction
 * flows through {@code snapshot().mode()} so operators can tell which shape is live.
 */
public final class DbLogApplication<TX extends SourceTransaction<?>>
    implements AutoCloseable, RuntimeStatusProvider, RequestSubmissionGateway {
  /** Default boot-mode label used when callers (typically tests) don't pass one. */
  public static final String DEFAULT_BOOT_MODE = "inspection";

  private final String bootMode;
  private final String adapterLabel;
  private final String sourceId;
  private final RuntimeStateStore stateStore;
  private final RelationalRuntimeStack<TX> stack;
  private final AutoCloseable stateStoreCloseable;
  private final ControlPlaneEventStore eventStore;
  private final MeterRegistry meterRegistry;
  private final String measurementAdapterTag;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public DbLogApplication(
      String bootMode,
      String adapterLabel,
      String sourceId,
      RuntimeStateStore stateStore,
      RelationalRuntimeStack<TX> stack,
      AutoCloseable stateStoreCloseable,
      ControlPlaneEventStore eventStore,
      MeterRegistry meterRegistry,
      String measurementAdapterTag) {
    this.bootMode = requireNonBlank(bootMode, "bootMode");
    this.adapterLabel = requireNonBlank(adapterLabel, "adapterLabel");
    this.sourceId = requireNonBlank(sourceId, "sourceId");
    this.stateStore = Objects.requireNonNull(stateStore, "stateStore");
    this.stack = Objects.requireNonNull(stack, "stack");
    this.stateStoreCloseable = stateStoreCloseable;
    this.eventStore = Objects.requireNonNull(eventStore, "eventStore");
    this.meterRegistry = meterRegistry;
    this.measurementAdapterTag = measurementAdapterTag;
  }

  public static DbLogApplication<?> open(
      SourceAdapter adapter,
      RelationalSourceConfig config,
      RuntimeStateStore stateStore,
      AutoCloseable stateStoreCloseable,
      ChangeEventSink sink,
      int chunkSize) throws Exception {
    return open(
        DEFAULT_BOOT_MODE,
        adapter,
        config,
        stateStore,
        stateStoreCloseable,
        sink,
        chunkSize,
        null,
        ControlPlaneEventCaptureOptions.defaults());
  }

  public static DbLogApplication<?> open(
      SourceAdapter adapter,
      RelationalSourceConfig config,
      RuntimeStateStore stateStore,
      AutoCloseable stateStoreCloseable,
      ChangeEventSink sink,
      int chunkSize,
      MeterRegistry meterRegistry) throws Exception {
    return open(
        DEFAULT_BOOT_MODE,
        adapter,
        config,
        stateStore,
        stateStoreCloseable,
        sink,
        chunkSize,
        meterRegistry,
        ControlPlaneEventCaptureOptions.defaults(),
        NoopTap.INSTANCE);
  }

  public static DbLogApplication<?> open(
      SourceAdapter adapter,
      RelationalSourceConfig config,
      RuntimeStateStore stateStore,
      AutoCloseable stateStoreCloseable,
      ChangeEventSink sink,
      int chunkSize,
      MeterRegistry meterRegistry,
      ControlPlaneEventCaptureOptions eventCaptureOptions) throws Exception {
    return open(
        DEFAULT_BOOT_MODE,
        adapter,
        config,
        stateStore,
        stateStoreCloseable,
        sink,
        chunkSize,
        meterRegistry,
        eventCaptureOptions,
        NoopTap.INSTANCE);
  }

  public static DbLogApplication<?> open(
      String bootMode,
      SourceAdapter adapter,
      RelationalSourceConfig config,
      RuntimeStateStore stateStore,
      AutoCloseable stateStoreCloseable,
      ChangeEventSink sink,
      int chunkSize,
      MeterRegistry meterRegistry,
      ControlPlaneEventCaptureOptions eventCaptureOptions) throws Exception {
    return open(
        bootMode,
        adapter,
        config,
        stateStore,
        stateStoreCloseable,
        sink,
        chunkSize,
        meterRegistry,
        eventCaptureOptions,
        NoopTap.INSTANCE);
  }

  public static DbLogApplication<?> open(
      String bootMode,
      SourceAdapter adapter,
      RelationalSourceConfig config,
      RuntimeStateStore stateStore,
      AutoCloseable stateStoreCloseable,
      ChangeEventSink sink,
      int chunkSize,
      MeterRegistry meterRegistry,
      ControlPlaneEventCaptureOptions eventCaptureOptions,
      Tap tap) throws Exception {
    Objects.requireNonNull(adapter, "adapter");
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(stateStore, "stateStore");
    Objects.requireNonNull(sink, "sink");
    Objects.requireNonNull(tap, "tap");
    ControlPlaneEventCaptureOptions options =
        eventCaptureOptions == null
            ? ControlPlaneEventCaptureOptions.defaults()
            : eventCaptureOptions;
    ControlPlaneEventStore eventStore = controlPlaneEventStore(options);
    RelationalRuntimeBootstrap bootstrap = new RelationalRuntimeBootstrap();
    RelationalRuntimeBootstrap.BootstrappedSession bootstrapped =
        bootstrap.open(
            new RelationalRuntimeBootstrap.BootstrapRequest(
                adapter,
                config,
                stateStore,
                options.enabled() ? new RecordingChangeEventSink(sink, eventStore) : sink,
                meterRegistry,
                tap));
    @SuppressWarnings("unchecked")
    RelationalRuntimeStack<SourceTransaction<?>> stack =
        (RelationalRuntimeStack<SourceTransaction<?>>)
            RelationalRuntimeAssembly.forBootstrappedSession(
                bootstrapped,
                stateStore,
                adapter.key(),
                config.sourceId(),
                chunkSize,
                java.time.Duration.ofSeconds(1),
                java.time.Duration.ofMillis(1),
                CheckpointFlushPolicy.defaults(),
                RuntimeLoopObserver.noop(),
                meterRegistry,
                tap);
    return new DbLogApplication<>(
        bootMode,
        adapter.displayName(),
        config.sourceId(),
        stateStore,
        stack,
        stateStoreCloseable,
        eventStore,
        meterRegistry,
        adapter.key());
  }

  public RuntimeStateStore stateStore() {
    return stateStore;
  }

  public RelationalRuntimeStack<TX> stack() {
    return stack;
  }

  public void processPendingRequests(Duration idleDrainTimeout) throws Exception {
    ensureOpen();
    stack.processPendingRequests(idleDrainTimeout);
  }

  public int drainStreaming(Duration idleTimeout) throws Exception {
    ensureOpen();
    return stack.drainStreaming(bootMode + "-streaming", idleTimeout);
  }

  public void enqueueCommittedTransaction(TX transaction) {
    ensureOpen();
    if (!(stack.session().runtime() instanceof CommittedTransactionIngress<?> ingress)) {
      throw new IllegalStateException(
          "active runtime does not support committed transaction ingress");
    }
    @SuppressWarnings("unchecked")
    CommittedTransactionIngress<TX> typedIngress = (CommittedTransactionIngress<TX>) ingress;
    typedIngress.enqueueCommittedTransaction(transaction);
  }

  public ControlPlaneQueryService controlPlaneQueryService() {
    return new ControlPlaneQueryService(
        stateStore, this, eventStore, meterRegistry, measurementAdapterTag);
  }

  public ControlPlaneCommandService controlPlaneCommandService() {
    return new ControlPlaneCommandService(stateStore, this, this);
  }

  public ControlPlaneHttpServer controlPlaneServer(String host, int port) {
    return controlPlaneServer(host, port, false, 8, 64, 1_048_576);
  }

  public ControlPlaneHttpServer controlPlaneServer(
      String host,
      int port,
      int executorMaxThreads,
      int executorQueueCapacity,
      int maxRequestBodyBytes) {
    return controlPlaneServer(
        host, port, false, executorMaxThreads, executorQueueCapacity, maxRequestBodyBytes);
  }

  public ControlPlaneHttpServer controlPlaneServer(
      String host,
      int port,
      boolean allowNonLoopback,
      int executorMaxThreads,
      int executorQueueCapacity,
      int maxRequestBodyBytes) {
    return new ControlPlaneHttpServer(
        controlPlaneQueryService(),
        controlPlaneCommandService(),
        host,
        port,
        allowNonLoopback,
        executorMaxThreads,
        executorQueueCapacity,
        maxRequestBodyBytes);
  }

  @Override
  public DumpRequest submit(DumpScope scope, TableId tableId, List<PrimaryKeyTuple> primaryKeyTuples) {
    ensureOpen();
    return stateStore.dumpRequests().createGenerated(scope, tableId, primaryKeyTuples);
  }

  public DumpRequest submitFromLiterals(
      DumpScope scope, TableId tableId, List<String> primaryKeyLiterals) {
    ensureOpen();
    if (scope != DumpScope.PRIMARY_KEYS) {
      return submit(scope, tableId, List.<PrimaryKeyTuple>of());
    }
    TableSchema schema =
        stack.session().runtime().currentCapturedSchemas().stream()
            .filter(candidate -> candidate.tableId().equals(tableId))
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "PRIMARY_KEYS submission requires a captured schema for "
                            + tableId.displayName()));
    return submit(scope, tableId, schema.primaryKeyTuplesFromLiterals(primaryKeyLiterals));
  }

  @Override
  public RuntimeStatusSnapshot snapshot() {
    if (closed.get()) {
      return new RuntimeStatusSnapshot(
          bootMode,
          adapterLabel,
          "DOWN",
          false,
          "Request submission is unavailable because the DBLog application is closed.",
          sourceId,
          null,
          -1,
          -1,
          SourceFlowControlSnapshot.unavailable());
    }
    RuntimeStatusInspectable inspectable =
        stack.session().runtime() instanceof RuntimeStatusInspectable value ? value : null;
    return new RuntimeStatusSnapshot(
        bootMode,
        adapterLabel,
        "UP",
        true,
        null,
        sourceId,
        inspectable == null ? null : inspectable.lastAcknowledgedCheckpointDisplayValue(),
        inspectable == null ? -1 : inspectable.pendingTransactionCount(),
        inspectable == null ? -1 : inspectable.capturedTableCount(),
        inspectable == null
            ? SourceFlowControlSnapshot.unavailable()
            : inspectable.sourceFlowControlSnapshot());
  }

  @Override
  public void close() throws Exception {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    Exception firstFailure = null;
    try {
      stack.session().close();
    } catch (Exception failure) {
      firstFailure = failure;
    }
    if (stateStoreCloseable != null) {
      try {
        stateStoreCloseable.close();
      } catch (Exception failure) {
        if (firstFailure != null) {
          firstFailure.addSuppressed(failure);
        } else {
          firstFailure = failure;
        }
      }
    }
    if (eventStore instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (Exception failure) {
        if (firstFailure != null) {
          firstFailure.addSuppressed(failure);
        } else {
          firstFailure = failure;
        }
      }
    }
    if (firstFailure != null) {
      throw firstFailure;
    }
  }

  private void ensureOpen() {
    if (closed.get()) {
      throw new IllegalStateException("DBLog application is closed");
    }
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static ControlPlaneEventStore controlPlaneEventStore(
      ControlPlaneEventCaptureOptions options) {
    if (!options.enabled()) {
      return ControlPlaneEventStore.noop();
    }
    ControlPlaneEventStore eventStore = new InMemoryControlPlaneEventStore(options.capacity());
    return options.asynchronous() ? new AsyncControlPlaneEventStore(eventStore) : eventStore;
  }

  public record ControlPlaneEventCaptureOptions(
      boolean enabled, boolean asynchronous, int capacity) {
    public ControlPlaneEventCaptureOptions {
      if (enabled && capacity <= 0) {
        throw new IllegalArgumentException("capacity must be > 0 when event capture is enabled");
      }
    }

    public static ControlPlaneEventCaptureOptions defaults() {
      return new ControlPlaneEventCaptureOptions(true, true, 512);
    }

    public static ControlPlaneEventCaptureOptions disabled() {
      return new ControlPlaneEventCaptureOptions(false, false, 1);
    }
  }
}
