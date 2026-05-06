package io.github.aandreakis.dblog.boot;

import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.SourceAdapter;
import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.config.DbLogProperties;
import io.github.aandreakis.dblog.controlplane.http.ControlPlaneHttpServer;
import io.github.aandreakis.dblog.runtime.bootstrap.DbLogApplication;
import io.github.aandreakis.dblog.runtime.bootstrap.RelationalRuntimeStack;
import io.github.aandreakis.dblog.runtime.bootstrap.StartupCheckRunner;
import io.github.aandreakis.dblog.runtime.observer.DbLogRuntimeObservability;
import io.github.aandreakis.dblog.sink.api.ChangeEventSink;
import io.github.aandreakis.dblog.sink.api.ConfiguredChangeEventSinkFactory;
import io.github.aandreakis.dblog.tap.NoopTap;
import io.github.aandreakis.dblog.tap.Tap;
import io.github.aandreakis.dblog.tap.TapHttpHandler;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Objects;

final class NextPrimaryRuntimeSupport {
  private final SourceAdapterRegistry adapterRegistry;
  private final PrintStream out;
  private final Tap tap;

  NextPrimaryRuntimeSupport(SourceAdapterRegistry adapterRegistry, PrintStream out) {
    this(adapterRegistry, out, NoopTap.INSTANCE);
  }

  NextPrimaryRuntimeSupport(SourceAdapterRegistry adapterRegistry, PrintStream out, Tap tap) {
    this.adapterRegistry = Objects.requireNonNull(adapterRegistry, "adapterRegistry");
    this.out = Objects.requireNonNull(out, "out");
    this.tap = Objects.requireNonNull(tap, "tap");
  }

  NextPrimaryApplication.SinkResources buildSinkResources(
      DbLogProperties properties,
      RelationalSourceConfig sourceConfig) {
    var boundProperties = Objects.requireNonNull(properties, "properties");
    RelationalSourceConfig requiredSourceConfig =
        Objects.requireNonNull(sourceConfig, "sourceConfig");
    MeterRegistry meterRegistry =
        boundProperties.getObservability().isMetricsEnabled() ? new SimpleMeterRegistry() : null;
    DbLogRuntimeObservability observability = new DbLogRuntimeObservability();
    ChangeEventSink sink =
        ConfiguredChangeEventSinkFactory.configuredChangeEventSink(
            boundProperties, requiredSourceConfig, meterRegistry, observability);
    return new NextPrimaryApplication.SinkResources(sink, meterRegistry);
  }

  void runRuntimeDefault(NextPrimaryApplication.RuntimeLaunchContext context) throws Exception {
    SourceAdapter adapter = context.adapterRegistry().require(context.adapterKey());
    // ConfiguredChangeEventSinkFactory.withTap returns the sink unchanged when tap is NoopTap, so
    // no branching here. The single dispatch between NoopTap/ActiveTap lives in the Spring
    // @Bean that produced {@code tap}; everything below just uses the injected instance.
    ChangeEventSink effectiveSink = ConfiguredChangeEventSinkFactory.withTap(context.sink(), tap);
    try (DbLogApplication<?> application =
        DbLogApplication.open(
            "runtime",
            adapter,
            context.sourceConfig(),
            context.stateStore(),
            null,
            effectiveSink,
            context.chunkSize(),
            context.meterRegistry(),
            DbLogApplication.ControlPlaneEventCaptureOptions.defaults(),
            tap)) {
      ControlPlaneHttpServer server = null;
      Thread shutdownHook = null;
      try {
        if (context.controlPlane().enabled()) {
          server =
              application.controlPlaneServer(
                  context.controlPlane().host(),
                  context.controlPlane().port(),
                  context.controlPlane().allowNonLoopback(),
                  context.controlPlane().executorMaxThreads(),
                  context.controlPlane().executorQueueCapacity(),
                  context.controlPlane().maxRequestBodyBytes());
          server.attachTap(new TapHttpHandler(tap));
          server.start();
          publishBoundPortFile(context.controlPlane().portFile(), server.boundPort());
        }
        ControlPlaneHttpServer serverRef = server;
        shutdownHook =
            new Thread(
                () -> {
                  application.stack().session().sink().requestStop();
                  application.stack().requestPump().requestStop();
                  if (serverRef != null) {
                    serverRef.stop();
                  }
                },
                "dblog-next-runtime-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        runRequestLoop(application.stack());
      } finally {
        if (server != null) {
          server.stop();
        }
        deleteBoundPortFile(context.controlPlane().portFile());
        if (shutdownHook != null) {
          try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
          } catch (IllegalStateException ignored) {
            // JVM is already shutting down.
          }
        }
      }
    }
  }

  void runStartupCheckDefault(NextPrimaryApplication.StartupCheckLaunchContext context)
      throws Exception {
    StartupCheckRunner runner = new StartupCheckRunner(adapterRegistry);
    StartupCheckRunner.StartupCheckResult result =
        runner.run(
            new StartupCheckRunner.StartupCheckRequest(
                context.adapterKey(), context.sourceConfig(), context.stateStore(), context.sink()));
    out.println(
        "startup-check adapter="
            + result.adapterKey()
            + " sourceId="
            + result.sourceId()
            + " liveSchemas="
            + result.liveSchemaCount()
            + " contractSchemas="
            + result.contractSchemaCount()
            + " loadedCheckpoint="
            + (result.loadedCheckpointDisplayValue() == null
                ? ""
                : result.loadedCheckpointDisplayValue()));
  }

  private static <TX extends SourceTransaction<?>> void runRequestLoop(
      RelationalRuntimeStack<TX> stack)
      throws Exception {
    stack.requestPump().runUntilStopped(stack.coordinator(), "runtime-streaming");
  }

  /**
   * Writes the bound control-plane port to {@code portFile} so external supervisors can discover
   * an OS-assigned port (typical when {@code dblog.control-plane.port=0}). Writes to a sibling
   * {@code .tmp} file then atomically renames into place — a concurrent reader either sees the
   * old contents or the full new contents, never a partial value.
   */
  private static void publishBoundPortFile(Path portFile, int boundPort) throws Exception {
    if (portFile == null) {
      return;
    }
    Path absolute = portFile.toAbsolutePath().normalize();
    Path parent = absolute.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    Path temp = absolute.resolveSibling(absolute.getFileName() + ".tmp");
    Files.writeString(temp, Integer.toString(boundPort));
    try {
      Files.move(temp, absolute, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (Exception ex) {
      // ATOMIC_MOVE may not be supported on every filesystem; fall back to a non-atomic replace.
      Files.move(temp, absolute, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  private static void deleteBoundPortFile(Path portFile) {
    if (portFile == null) {
      return;
    }
    try {
      Files.deleteIfExists(portFile.toAbsolutePath().normalize());
    } catch (Exception ignored) {
      // Best-effort cleanup; a stale port file from this run will be overwritten by the next.
    }
  }
}
