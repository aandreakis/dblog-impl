package io.github.aandreakis.dblog.sink.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.config.DbLogProperties;
import io.github.aandreakis.dblog.config.DbLogTableMappingProperties;
import io.github.aandreakis.dblog.config.DbLogTargetProperties;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.runtime.observer.DbLogRuntimeObservability;
import io.github.aandreakis.dblog.sink.jdbc.TargetTableResolver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ConfiguredChangeEventSinkFactoryTests {
  @Test
  void rejectsRunsWithNoConfiguredSinks() {
    DbLogProperties properties = new DbLogProperties();

    assertThatThrownBy(
            () ->
                ConfiguredChangeEventSinkFactory.configuredChangeEventSink(
                    properties,
                    new RelationalSourceConfig(
                        "sourceA",
                        "jdbc:postgresql://localhost:5432/appdb",
                        "postgres",
                        "secret",
                        "appdb",
                        List.of("public.orders"),
                        Map.of(),
                        false),
                    new SimpleMeterRegistry(),
                    new DbLogRuntimeObservability()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("No DBLog output sink is configured")
        .hasMessageContaining("dblog.sink.noop.enabled=true");
  }

  @Test
  void allowsExplicitNoopSinkConfiguration() {
    DbLogProperties properties = new DbLogProperties();
    properties.getSink().getNoop().setEnabled(true);

    assertThat(
            ConfiguredChangeEventSinkFactory.configuredChangeEventSink(
                properties,
                new RelationalSourceConfig(
                    "sourceA",
                    "jdbc:postgresql://localhost:5432/appdb",
                    "postgres",
                    "secret",
                    "appdb",
                    List.of("public.orders"),
                    Map.of(),
                    false),
                new SimpleMeterRegistry(),
                new DbLogRuntimeObservability()))
        .isSameAs(NoOpChangeEventSink.instance());
  }

  @Test
  void retryingTargetSinkStopsPromptlyWhenShutdownIsRequested() throws Exception {
    CountDownLatch firstAttemptStarted = new CountDownLatch(1);
    AtomicInteger closeCalls = new AtomicInteger(0);
    AtomicReference<Throwable> appendFailure = new AtomicReference<>();
    ChangeEventSink delegate =
        new ChangeEventSink() {
          @Override
          public void appendEvents(List<io.github.aandreakis.dblog.core.model.ChangeEvent> events) {
            firstAttemptStarted.countDown();
            throw new IllegalStateException(
                "transient sink outage",
                new SQLTransientConnectionException("target temporarily unavailable"));
          }

          @Override
          public void close() {
            closeCalls.incrementAndGet();
          }
        };

    ChangeEventSink retryingSink =
        ConfiguredChangeEventSinkFactory.retryingTargetChangeEventSink(
            new DbLogRuntimeObservability(),
            delegate,
            Duration.ofMinutes(1),
            "postgres");

    Thread appendThread =
        new Thread(
            () -> {
              try {
                retryingSink.appendEvents(List.of());
              } catch (Throwable failure) {
                appendFailure.set(failure);
              }
            },
            "retrying-target-sink-test");
    appendThread.start();

    assertThat(firstAttemptStarted.await(5, TimeUnit.SECONDS)).isTrue();

    retryingSink.requestStop();
    appendThread.join(5_000L);

    assertThat(appendThread.isAlive()).isFalse();
    assertThat(appendFailure.get()).isInstanceOf(IllegalStateException.class);
    assertThat(appendFailure.get().getMessage()).contains("Sink retry loop was stopped");

    retryingSink.close();
    assertThat(closeCalls.get()).isEqualTo(1);
  }

  @Test
  void resolvesTargetMappingsFromSourceConfigWhenSchemasAreNotYetLoaded() {
    DbLogTargetProperties target = new DbLogTargetProperties();
    DbLogTableMappingProperties mapping = new DbLogTableMappingProperties();
    mapping.setSourceSchema("public");
    mapping.setSourceTable("orders");
    mapping.setTargetSchema("mirror");
    mapping.setTargetTable("orders_copy");
    target.getTableMappings().add(mapping);

    TargetTableResolver resolver =
        ConfiguredChangeEventSinkFactory.configuredTargetTableResolver(
            target,
            new RelationalSourceConfig(
                "sourceA",
                "jdbc:postgresql://localhost:5432/appdb",
                "postgres",
                "secret",
                null,
                List.of("public.orders"),
                Map.of(),
                false));

    assertThat(resolver.resolve(new TableId("appdb", "public", "orders")))
        .isEqualTo(new TableId("appdb", "mirror", "orders_copy"));
  }

  @Test
  void resolvesMySqlTargetMappingsAgainstRuntimeTableIdWhenDatabaseIsInferredFromJdbcUrl() {
    DbLogTargetProperties target = targetWithSampleOrdersMapping();

    TargetTableResolver resolver =
        ConfiguredChangeEventSinkFactory.configuredTargetTableResolver(
            target,
            new RelationalSourceConfig(
                "mysql-source",
                "jdbc:mysql://localhost:3306/app",
                "dblog",
                "secret",
                null,
                List.of("app.sample_orders"),
                Map.of(),
                false));

    assertThat(resolver.resolve(new TableId("mysql-source", "app", "sample_orders")))
        .isEqualTo(new TableId("mysql-source", "mirror", "sample_orders_copy"));
  }

  @Test
  void resolvesMySqlTargetMappingsAgainstRuntimeTableIdWhenDatabaseIsConfiguredExplicitly() {
    DbLogTargetProperties target = targetWithSampleOrdersMapping();

    TargetTableResolver resolver =
        ConfiguredChangeEventSinkFactory.configuredTargetTableResolver(
            target,
            new RelationalSourceConfig(
                "mysql-source",
                "jdbc:mysql://localhost:3306/ignored",
                "dblog",
                "secret",
                "app",
                List.of("app.sample_orders"),
                Map.of(),
                false));

    assertThat(resolver.resolve(new TableId("mysql-source", "app", "sample_orders")))
        .isEqualTo(new TableId("mysql-source", "mirror", "sample_orders_copy"));
  }

  @Test
  void rejectsMappingsThatReferenceUncapturedSourceTables() {
    DbLogTargetProperties target = new DbLogTargetProperties();
    DbLogTableMappingProperties mapping = new DbLogTableMappingProperties();
    mapping.setSourceSchema("public");
    mapping.setSourceTable("missing_orders");
    target.getTableMappings().add(mapping);

    assertThatThrownBy(
            () ->
                ConfiguredChangeEventSinkFactory.configuredTargetTableResolver(
                    target,
                    new RelationalSourceConfig(
                        "sourceA",
                        "jdbc:postgresql://localhost:5432/appdb",
                        "postgres",
                        "secret",
                        "appdb",
                        List.of("public.orders"),
                        Map.of(),
                        false)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("not captured");
  }

  private static DbLogTargetProperties targetWithSampleOrdersMapping() {
    DbLogTargetProperties target = new DbLogTargetProperties();
    DbLogTableMappingProperties mapping = new DbLogTableMappingProperties();
    mapping.setSourceSchema("app");
    mapping.setSourceTable("sample_orders");
    mapping.setTargetSchema("mirror");
    mapping.setTargetTable("sample_orders_copy");
    target.getTableMappings().add(mapping);
    return target;
  }
}
