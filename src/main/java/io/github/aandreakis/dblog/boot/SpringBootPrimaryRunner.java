package io.github.aandreakis.dblog.boot;

import io.github.aandreakis.dblog.config.DbLogBootstrapMode;
import io.github.aandreakis.dblog.config.DbLogProperties;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import io.github.aandreakis.dblog.verification.scenario.ScenarioModeExecutor;
import java.util.Objects;

final class SpringBootPrimaryRunner {
  private final SourceAdapterRegistry adapterRegistry;
  private final NextPrimaryLaunchPlanner launchPlanner;
  private final NextPrimaryRuntimeSupport runtimeSupport;
  private final ScenarioModeExecutor scenarioModeExecutor;

  SpringBootPrimaryRunner(
      SourceAdapterRegistry adapterRegistry,
      NextPrimaryLaunchPlanner launchPlanner,
      NextPrimaryRuntimeSupport runtimeSupport,
      ScenarioModeExecutor scenarioModeExecutor) {
    this.adapterRegistry = Objects.requireNonNull(adapterRegistry, "adapterRegistry");
    this.launchPlanner = Objects.requireNonNull(launchPlanner, "launchPlanner");
    this.runtimeSupport = Objects.requireNonNull(runtimeSupport, "runtimeSupport");
    this.scenarioModeExecutor = Objects.requireNonNull(scenarioModeExecutor, "scenarioModeExecutor");
  }

  int run(DbLogProperties properties) throws Exception {
    DbLogProperties requiredProperties = Objects.requireNonNull(properties, "properties");
    DbLogBootstrapMode mode = resolveBootMode(requiredProperties);
    String adapterKey = NextPrimaryLaunchPlanner.bootstrapAdapterKey(mode, requiredProperties);
    return switch (mode) {
      case RUNTIME -> runRuntimeMode(requiredProperties, adapterKey);
      case STARTUP_CHECK -> runStartupCheckMode(requiredProperties, adapterKey);
      case SCENARIO -> runScenarioMode(requiredProperties);
    };
  }

  private int runRuntimeMode(DbLogProperties properties, String adapterKey) throws Exception {
    NextPrimaryApplication.RuntimeLaunchPlan plan = launchPlanner.runtimePlan(properties, adapterKey);
    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(plan.statePath());
        NextPrimaryApplication.SinkResources sinkResources =
            runtimeSupport.buildSinkResources(plan.properties(), plan.sourceConfig())) {
      runtimeSupport.runRuntimeDefault(
          new NextPrimaryApplication.RuntimeLaunchContext(
              adapterRegistry,
              plan.adapterKey(),
              plan.sourceConfig(),
              stateStore,
              sinkResources.sink(),
              sinkResources.meterRegistry(),
              plan.chunkSize(),
              plan.controlPlane()));
    }
    return 0;
  }

  private int runStartupCheckMode(DbLogProperties properties, String adapterKey) throws Exception {
    NextPrimaryApplication.StartupCheckLaunchPlan plan =
        launchPlanner.startupCheckPlan(properties, adapterKey);
    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(plan.statePath());
        NextPrimaryApplication.SinkResources sinkResources =
            runtimeSupport.buildSinkResources(plan.properties(), plan.sourceConfig())) {
      runtimeSupport.runStartupCheckDefault(
          new NextPrimaryApplication.StartupCheckLaunchContext(
              plan.adapterKey(),
              plan.sourceConfig(),
              stateStore,
              sinkResources.sink(),
              plan.statePath()));
    }
    return 0;
  }

  private int runScenarioMode(DbLogProperties properties) throws Exception {
    scenarioModeExecutor.execute(properties);
    return 0;
  }

  static DbLogBootstrapMode resolveBootMode(DbLogProperties properties) {
    DbLogProperties.BootMode configuredMode = Objects.requireNonNull(properties, "properties").getBootMode();
    if (configuredMode == null) {
      return DbLogBootstrapMode.RUNTIME;
    }
    return DbLogBootstrapMode.valueOf(configuredMode.name());
  }
}
