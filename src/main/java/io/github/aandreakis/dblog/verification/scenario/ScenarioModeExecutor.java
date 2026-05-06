package io.github.aandreakis.dblog.verification.scenario;

import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.config.DbLogProperties;
import io.github.aandreakis.dblog.config.DbLogPropertiesBinder;
import io.github.aandreakis.dblog.controlplane.service.ControlPlaneEventCapture;
import io.github.aandreakis.dblog.core.checkpoint.CheckpointFlushPolicy;
import io.github.aandreakis.dblog.runtime.bootstrap.RuntimeTuningSupport;
import io.github.aandreakis.dblog.runtime.observer.DbLogRuntimeObservability;
import io.github.aandreakis.dblog.sink.api.ConfiguredChangeEventSinkFactory;
import io.github.aandreakis.dblog.verification.scenario.mysql.MySqlScenarioConfig;
import io.github.aandreakis.dblog.verification.scenario.mysql.MySqlScenarioRunner;
import io.github.aandreakis.dblog.verification.scenario.postgres.PostgresScenarioConfig;
import io.github.aandreakis.dblog.verification.scenario.postgres.PostgresScenarioRunner;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Scenario boot executor. */
public final class ScenarioModeExecutor {
  private static final Logger log = LoggerFactory.getLogger(ScenarioModeExecutor.class);

  private final ScenarioRunnerFactory scenarioRunnerFactory;
  private final Supplier<? extends MeterRegistry> meterRegistryFactory;
  private final Supplier<DbLogRuntimeObservability> observabilityFactory;
  private final Supplier<ControlPlaneEventCapture> eventCaptureFactory;

  public ScenarioModeExecutor() {
    this(
        ScenarioModeExecutor::createDefaultRunner,
        SimpleMeterRegistry::new,
        DbLogRuntimeObservability::new,
        ControlPlaneEventCapture::new);
  }

  ScenarioModeExecutor(
      ScenarioRunnerFactory scenarioRunnerFactory,
      Supplier<? extends MeterRegistry> meterRegistryFactory,
      Supplier<DbLogRuntimeObservability> observabilityFactory,
      Supplier<ControlPlaneEventCapture> eventCaptureFactory) {
    this.scenarioRunnerFactory =
        Objects.requireNonNull(scenarioRunnerFactory, "scenarioRunnerFactory");
    this.meterRegistryFactory =
        Objects.requireNonNull(meterRegistryFactory, "meterRegistryFactory");
    this.observabilityFactory =
        Objects.requireNonNull(observabilityFactory, "observabilityFactory");
    this.eventCaptureFactory =
        Objects.requireNonNull(eventCaptureFactory, "eventCaptureFactory");
  }

  public ScenarioModeResult execute(Map<String, String> properties) throws Exception {
    Objects.requireNonNull(properties, "properties");
    return execute(bindProperties(properties));
  }

  public ScenarioModeResult execute(DbLogProperties properties) throws Exception {
    DbLogProperties boundProperties = Objects.requireNonNull(properties, "properties");
    MeterRegistry meterRegistry =
        Objects.requireNonNull(meterRegistryFactory.get(), "meterRegistryFactory.get()");
    DbLogRuntimeObservability observability =
        Objects.requireNonNull(observabilityFactory.get(), "observabilityFactory.get()");
    ControlPlaneEventCapture eventCapture =
        Objects.requireNonNull(eventCaptureFactory.get(), "eventCaptureFactory.get()");
    String adapter =
        normalizeAdapter(
            requireNonBlank(boundProperties.getScenario().getAdapter(), "dblog.scenario.adapter"));
    String scenarioId =
        requireNonBlank(boundProperties.getScenario().getId(), "dblog.scenario.id");
    try {
      observability.runtimeStarted("scenario", adapter);
      observability.requestSubmissionAvailable();
      ScenarioRunnerHandle runner =
          scenarioRunnerFactory.create(
              new ScenarioExecutionContext(
                  scenarioConfig(boundProperties),
                  boundProperties,
                  meterRegistry,
                  observability,
                  eventCapture));
      log.info("Starting next-owned scenario executor adapter={} scenarioId={}", adapter, scenarioId);
      runner.run();
      observability.runtimeStopped("scenario", adapter);
      log.info("Scenario executor completed adapter={} scenarioId={}", adapter, scenarioId);
      return new ScenarioModeResult(scenarioId, adapter);
    } catch (Throwable failure) {
      observability.runtimeFailed("scenario", adapter, failure, meterRegistry);
      log.error("Scenario executor failed adapter={} scenarioId={}", adapter, scenarioId, failure);
      if (failure instanceof Exception exception) {
        throw exception;
      }
      throw new IllegalStateException("Scenario executor failed with a non-Exception throwable", failure);
    } finally {
      closeQuietly(meterRegistry);
    }
  }

  private static DbLogProperties bindProperties(Map<String, String> properties) {
    return DbLogPropertiesBinder.bind(properties);
  }

  private static ScenarioConfigView scenarioConfig(DbLogProperties properties) {
    String adapter =
        normalizeAdapter(
            requireNonBlank(properties.getScenario().getAdapter(), "dblog.scenario.adapter"));
    return switch (adapter) {
      case "mysql" -> MySqlScenarioConfig.from(properties);
      case "postgres" -> PostgresScenarioConfig.from(properties);
      default -> throw new IllegalArgumentException("Unsupported scenario adapter: " + adapter);
    };
  }

  private static String normalizeAdapter(String adapter) {
    String normalized = adapter.trim().toLowerCase(Locale.ROOT);
    return normalized.equals("postgresql") ? "postgres" : normalized;
  }

  private static String requireNonBlank(String value, String key) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(key + " must not be blank");
    }
    return value.trim();
  }

  private static ScenarioRunnerHandle createDefaultRunner(ScenarioExecutionContext context) {
    Objects.requireNonNull(context, "context");
    if (context.scenarioConfig() instanceof MySqlScenarioConfig config) {
      UnaryOperator<ScenarioStore> decorator =
          scenarioStoreDecorator(
              context.properties(),
              config.mirrorSourceConfig(),
              context.meterRegistry(),
              context.observability());
      return () ->
          new MySqlScenarioRunner(
                  config,
                  decorator,
                  checkpointFlushPolicy(context.properties()),
                  heartbeatInterval(context.properties()),
                  context.meterRegistry(),
                  context.observability(),
                  context.eventCapture())
              .run();
    }
    if (context.scenarioConfig() instanceof PostgresScenarioConfig config) {
      UnaryOperator<ScenarioStore> decorator =
          scenarioStoreDecorator(
              context.properties(),
              config.mirrorSourceConfig(),
              context.meterRegistry(),
              context.observability());
      return () ->
          new PostgresScenarioRunner(
                  config,
                  decorator,
                  checkpointFlushPolicy(context.properties()),
                  heartbeatInterval(context.properties()),
                  context.meterRegistry(),
                  context.observability(),
                  context.eventCapture())
              .run();
    }
    throw new IllegalArgumentException(
        "Unsupported scenario config type: " + context.scenarioConfig().getClass().getSimpleName());
  }

  private static UnaryOperator<ScenarioStore> scenarioStoreDecorator(
      DbLogProperties properties,
      RelationalSourceConfig sourceConfig,
      MeterRegistry meterRegistry,
      DbLogRuntimeObservability observability) {
    if (!hasConfiguredScenarioMirrorSink(properties)) {
      return UnaryOperator.identity();
    }
    return store ->
        new TeeingScenarioStore(
            store,
            ConfiguredChangeEventSinkFactory.configuredChangeEventSink(
                properties, sourceConfig, meterRegistry, observability));
  }

  private static boolean hasConfiguredScenarioMirrorSink(DbLogProperties properties) {
    DbLogProperties.Sink sink = Objects.requireNonNull(properties, "properties").getSink();
    return sink.getNdjson().isStdout()
        || sink.getNdjson().getPath() != null
        || sink.getTypedH2().getPath() != null
        || sink.getNoop().isEnabled()
        || properties.getTarget().isEnabled();
  }

  private static CheckpointFlushPolicy checkpointFlushPolicy(
      DbLogProperties properties) {
    return RuntimeTuningSupport.checkpointFlushPolicy(properties);
  }

  private static Duration heartbeatInterval(DbLogProperties properties) {
    return RuntimeTuningSupport.heartbeatInterval(properties);
  }

  private static void closeQuietly(MeterRegistry meterRegistry) {
    if (meterRegistry instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (Exception ignored) {
        // Best effort only for test-scoped registries.
      }
    }
  }

  public record ScenarioModeResult(String scenarioId, String adapter) {
    public ScenarioModeResult {
      Objects.requireNonNull(scenarioId, "scenarioId");
      Objects.requireNonNull(adapter, "adapter");
    }
  }

  record ScenarioExecutionContext(
      ScenarioConfigView scenarioConfig,
      DbLogProperties properties,
      MeterRegistry meterRegistry,
      DbLogRuntimeObservability observability,
      ControlPlaneEventCapture eventCapture) {
    ScenarioExecutionContext {
      Objects.requireNonNull(scenarioConfig, "scenarioConfig");
      Objects.requireNonNull(properties, "properties");
      Objects.requireNonNull(meterRegistry, "meterRegistry");
      Objects.requireNonNull(observability, "observability");
      Objects.requireNonNull(eventCapture, "eventCapture");
    }
  }

  @FunctionalInterface
  interface ScenarioRunnerFactory {
    ScenarioRunnerHandle create(ScenarioExecutionContext context);
  }

  @FunctionalInterface
  interface ScenarioRunnerHandle {
    void run() throws Exception;
  }
}
