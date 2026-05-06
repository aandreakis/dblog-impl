package io.github.aandreakis.dblog.runtime.bootstrap;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.adapter.api.WatermarkWindowRuntime;
import io.github.aandreakis.dblog.core.checkpoint.CheckpointFlushPolicy;
import io.github.aandreakis.dblog.core.reconcile.WindowReconciler;
import io.github.aandreakis.dblog.core.request.DefaultDumpWindowCoordinator;
import io.github.aandreakis.dblog.core.request.DefaultTargetedRepairCoordinator;
import io.github.aandreakis.dblog.core.request.RuntimeStateDumpRequestCoordinator;
import io.github.aandreakis.dblog.runtime.host.RuntimeSession;
import io.github.aandreakis.dblog.runtime.loop.RuntimeRequestPump;
import io.github.aandreakis.dblog.runtime.loop.RuntimeStreamingPump;
import io.github.aandreakis.dblog.runtime.observer.RuntimeLoopObserver;
import io.github.aandreakis.dblog.runtime.telemetry.RuntimeMeasurementWrappers;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import io.github.aandreakis.dblog.tap.NoopTap;
import io.github.aandreakis.dblog.tap.Tap;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

public final class RelationalRuntimeAssembly {
  private RelationalRuntimeAssembly() {}

  public static RelationalRuntimeStack<?> forBootstrappedSession(
      RelationalRuntimeBootstrap.BootstrappedSession bootstrapped,
      RuntimeStateStore stateStore,
      String adapterLabel,
      String sourceId,
      int chunkSize) {
    return forBootstrappedSession(
        bootstrapped,
        stateStore,
        adapterLabel,
        sourceId,
        chunkSize,
        Duration.ofSeconds(1),
        Duration.ofMillis(1),
        CheckpointFlushPolicy.defaults(),
        RuntimeLoopObserver.noop(),
        null,
        NoopTap.INSTANCE);
  }

  public static <TX extends SourceTransaction<?>> RelationalRuntimeStack<TX> forBootstrappedSession(
      RelationalRuntimeBootstrap.BootstrappedSession bootstrapped,
      RuntimeStateStore stateStore,
      String adapterLabel,
      String sourceId,
      int chunkSize,
      Duration transactionWaitTimeout,
      Duration pollInterval,
      CheckpointFlushPolicy checkpointFlushPolicy,
      RuntimeLoopObserver<TX> observer,
      MeterRegistry meterRegistry,
      Tap tap) {
    Objects.requireNonNull(bootstrapped, "bootstrapped");
    Objects.requireNonNull(stateStore, "stateStore");
    Objects.requireNonNull(tap, "tap");
    RuntimeSession<?> session = bootstrapped.session();
    if (!(session.runtime() instanceof WatermarkWindowRuntime<?> watermarkRuntime)) {
      throw new IllegalArgumentException(
          "Bootstrapped session runtime does not support watermark-window coordination");
    }
    @SuppressWarnings("unchecked")
    RuntimeSession<TX> typedSession = (RuntimeSession<TX>) session;
    @SuppressWarnings("unchecked")
    WatermarkWindowRuntime<TX> typedRuntime = (WatermarkWindowRuntime<TX>) watermarkRuntime;

    DefaultDumpWindowCoordinator<TX> dumpCoordinator =
        new DefaultDumpWindowCoordinator<>(
            adapterLabel,
            "transaction",
            typedRuntime,
            stateStore.dumpProgress(),
            stateStore.schemas(),
            typedSession.chunkReader(),
            new WindowReconciler(tap),
            transactionWaitTimeout,
            pollInterval,
            tap);
    DefaultTargetedRepairCoordinator<TX> targetedRepairCoordinator =
        new DefaultTargetedRepairCoordinator<>(
            adapterLabel,
            "transaction",
            typedRuntime,
            typedSession.chunkReader(),
            new WindowReconciler(tap),
            transactionWaitTimeout,
            pollInterval,
            tap);
    RuntimeStateDumpRequestCoordinator<TX> requestCoordinator =
        new RuntimeStateDumpRequestCoordinator<>(
            adapterLabel,
            sourceId,
            stateStore.dumpRequests(),
            stateStore.schemas(),
            dumpCoordinator,
            targetedRepairCoordinator,
            () -> List.copyOf(bootstrapped.contractSchemas()),
            chunkSize,
            tap);
    var instrumentedRequestCoordinator =
        RuntimeMeasurementWrappers.instrumentCoordinator(
            adapterLabel, meterRegistry, requestCoordinator);
    RuntimeStreamingPump<TX> streamingPump =
        typedSession.streamingPump(checkpointFlushPolicy, observer, tap);
    RuntimeRequestPump<TX> requestPump =
        new RuntimeRequestPump<>(
            streamingPump,
            observer,
            Duration.ofMillis(250),
            new java.util.concurrent.atomic.AtomicBoolean(false));

    return new RelationalRuntimeStack<>(
        typedSession,
        streamingPump,
        dumpCoordinator,
        targetedRepairCoordinator,
        instrumentedRequestCoordinator,
        requestPump);
  }
}
