package io.github.aandreakis.dblog.boot;

import io.github.aandreakis.dblog.adapter.mysql.MySqlSourceAdapter;
import io.github.aandreakis.dblog.adapter.postgres.PostgresSourceAdapter;
import io.github.aandreakis.dblog.config.DbLogProperties;
import io.github.aandreakis.dblog.tap.ActiveTap;
import io.github.aandreakis.dblog.tap.NoopTap;
import io.github.aandreakis.dblog.tap.Tap;
import io.github.aandreakis.dblog.tap.TapConfig;
import io.github.aandreakis.dblog.verification.scenario.ScenarioModeExecutor;
import java.util.List;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@ConfigurationPropertiesScan(
    basePackages = {
      "io.github.aandreakis.dblog.config",
      "io.github.aandreakis.dblog.tap"
    })
public class DbLogImplApplication {
  public static void main(String[] args) {
    run(args, launcherArgs -> SpringApplication.run(DbLogImplApplication.class, launcherArgs));
  }

  static int run(String[] args, SpringApplicationLauncher springApplicationLauncher) {
    ConfigurableApplicationContext context = springApplicationLauncher.run(args);
    return SpringApplication.exit(context);
  }

  @Bean
  SourceAdapterRegistry sourceAdapterRegistry() {
    return new SourceAdapterRegistry(List.of(new MySqlSourceAdapter(), new PostgresSourceAdapter()));
  }

  @Bean
  NextPrimaryLaunchPlanner nextPrimaryLaunchPlanner() {
    return new NextPrimaryLaunchPlanner();
  }

  /**
   * Single place where the dispatch between {@link NoopTap} and {@link ActiveTap} happens.
   * Downstream components just depend on {@link Tap} and don't branch on configuration. When the
   * tap is off, the bean returns the shared {@link NoopTap} singleton whose empty-body methods
   * the JIT inlines to zero.
   */
  @Bean
  Tap tap(TapConfig tapConfig, DbLogProperties properties) {
    if (!tapConfig.isEnabled()) {
      return NoopTap.INSTANCE;
    }
    String sourceId = properties.getSource() == null ? null : properties.getSource().getId();
    if (sourceId == null || sourceId.isBlank()) {
      sourceId = "unknown";
    }
    return new ActiveTap(tapConfig, java.util.UUID.randomUUID().toString(), sourceId);
  }

  @Bean
  NextPrimaryRuntimeSupport nextPrimaryRuntimeSupport(
      SourceAdapterRegistry adapterRegistry, Tap tap) {
    return new NextPrimaryRuntimeSupport(adapterRegistry, System.out, tap);
  }

  @Bean
  ScenarioModeExecutor scenarioModeExecutor() {
    return new ScenarioModeExecutor();
  }

  @Bean
  SpringBootPrimaryRunner springBootPrimaryRunner(
      SourceAdapterRegistry adapterRegistry,
      NextPrimaryLaunchPlanner launchPlanner,
      NextPrimaryRuntimeSupport runtimeSupport,
      ScenarioModeExecutor scenarioModeExecutor) {
    return new SpringBootPrimaryRunner(
        adapterRegistry, launchPlanner, runtimeSupport, scenarioModeExecutor);
  }

  @Bean
  ApplicationRunner dbLogApplicationRunner(
      SpringBootPrimaryRunner runner, DbLogProperties properties) {
    return args -> runner.run(properties);
  }

  @FunctionalInterface
  interface SpringApplicationLauncher {
    ConfigurableApplicationContext run(String[] args);
  }
}
