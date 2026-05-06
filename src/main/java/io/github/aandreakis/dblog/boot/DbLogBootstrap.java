package io.github.aandreakis.dblog.boot;

import io.github.aandreakis.dblog.config.DbLogBootstrapMode;
import io.github.aandreakis.dblog.runtime.bootstrap.StartupCheckRunner;
import io.github.aandreakis.dblog.runtime.host.DbLogRuntimeHostLifecycle;
import io.github.aandreakis.dblog.verification.scenario.ScenarioHarness;
import java.time.Duration;
import java.util.Objects;

public final class DbLogBootstrap {
  private final SourceAdapterRegistry adapterRegistry;

  public DbLogBootstrap(SourceAdapterRegistry adapterRegistry) {
    this.adapterRegistry = Objects.requireNonNull(adapterRegistry, "adapterRegistry");
  }

  public SourceAdapterRegistry adapterRegistry() {
    return adapterRegistry;
  }

  public ResolvedBootPlan resolve(BootstrapRequest request) {
    Objects.requireNonNull(request, "request");
    return switch (request.mode()) {
      case RUNTIME -> new ResolvedBootPlan(
          request.mode(),
          requireAdapterKey(request.adapterKey()),
          BootTarget.RUNTIME_HOST);
      case SCENARIO -> new ResolvedBootPlan(
          request.mode(),
          requireAdapterKey(request.adapterKey()),
          BootTarget.SCENARIO_HARNESS);
      case STARTUP_CHECK -> new ResolvedBootPlan(
          request.mode(),
          requireAdapterKey(request.adapterKey()),
          BootTarget.STARTUP_CHECK);
    };
  }

  public BootPlan select(
      BootstrapRequest request,
      DbLogRuntimeHostLifecycle<?> runtimeHostLifecycle,
      ScenarioHarness scenarioHarness,
      StartupCheckRunner startupCheckRunner) {
    ResolvedBootPlan resolved = resolve(request);
    return switch (resolved.target()) {
      case RUNTIME_HOST -> new BootPlan(
          resolved.mode(),
          resolved.adapterKey(),
          BootTarget.RUNTIME_HOST,
          () -> Objects.requireNonNull(runtimeHostLifecycle, "runtimeHostLifecycle"));
      case SCENARIO_HARNESS -> new BootPlan(
          resolved.mode(),
          resolved.adapterKey(),
          BootTarget.SCENARIO_HARNESS,
          () -> Objects.requireNonNull(scenarioHarness, "scenarioHarness"));
      case STARTUP_CHECK -> new BootPlan(
          resolved.mode(),
          resolved.adapterKey(),
          BootTarget.STARTUP_CHECK,
          () -> Objects.requireNonNull(startupCheckRunner, "startupCheckRunner"));
    };
  }

  public BootExecutionResult execute(
      BootExecutionRequest request,
      DbLogRuntimeHostLifecycle<?> runtimeHostLifecycle,
      ScenarioHarness scenarioHarness,
      StartupCheckRunner startupCheckRunner)
      throws Exception {
    Objects.requireNonNull(request, "request");
    BootPlan plan =
        select(
            request.bootstrapRequest(),
            runtimeHostLifecycle,
            scenarioHarness,
            startupCheckRunner);
    return switch (plan.target()) {
      case RUNTIME_HOST -> new BootExecutionResult(
          plan.target(),
          Objects.requireNonNull(runtimeHostLifecycle, "runtimeHostLifecycle")
              .runStreamingDrain(
                  requireNonBlank(request.runtimeStageLabel(), "runtimeStageLabel"),
                  Objects.requireNonNull(request.runtimeIdleTimeout(), "runtimeIdleTimeout")));
      case SCENARIO_HARNESS -> new BootExecutionResult(
          plan.target(),
          Objects.requireNonNull(scenarioHarness, "scenarioHarness")
              .run(
                  Objects.requireNonNull(
                      request.scenarioRequest(), "scenarioRequest")));
      case STARTUP_CHECK -> new BootExecutionResult(
          plan.target(),
          Objects.requireNonNull(startupCheckRunner, "startupCheckRunner")
              .run(
                  Objects.requireNonNull(
                      request.startupCheckRequest(), "startupCheckRequest")));
    };
  }

  private String requireAdapterKey(String adapterKey) {
    return adapterRegistry.require(adapterKey).key();
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  public record BootstrapRequest(DbLogBootstrapMode mode, String adapterKey) {
    public BootstrapRequest {
      Objects.requireNonNull(mode, "mode");
    }
  }

  public record BootPlan(
      DbLogBootstrapMode mode, String adapterKey, BootTarget target, ComponentSupplier component) {
    public BootPlan {
      Objects.requireNonNull(mode, "mode");
      Objects.requireNonNull(adapterKey, "adapterKey");
      Objects.requireNonNull(target, "target");
      Objects.requireNonNull(component, "component");
    }
  }

  public record ResolvedBootPlan(
      DbLogBootstrapMode mode, String adapterKey, BootTarget target) {
    public ResolvedBootPlan {
      Objects.requireNonNull(mode, "mode");
      Objects.requireNonNull(adapterKey, "adapterKey");
      Objects.requireNonNull(target, "target");
    }
  }

  public record BootExecutionRequest(
      BootstrapRequest bootstrapRequest,
      String runtimeStageLabel,
      Duration runtimeIdleTimeout,
      ScenarioHarness.ScenarioRequest<?> scenarioRequest,
      StartupCheckRunner.StartupCheckRequest startupCheckRequest) {
    public BootExecutionRequest {
      Objects.requireNonNull(bootstrapRequest, "bootstrapRequest");
    }
  }

  public record BootExecutionResult(BootTarget target, Object outcome) {
    public BootExecutionResult {
      Objects.requireNonNull(target, "target");
      Objects.requireNonNull(outcome, "outcome");
    }
  }

  public enum BootTarget {
    RUNTIME_HOST,
    SCENARIO_HARNESS,
    STARTUP_CHECK
  }

  @FunctionalInterface
  public interface ComponentSupplier {
    Object get();
  }
}
