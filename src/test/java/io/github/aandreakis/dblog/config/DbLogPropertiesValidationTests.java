package io.github.aandreakis.dblog.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

class DbLogPropertiesValidationTests {
  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner().withUserConfiguration(PropertiesConfiguration.class);

  @Test
  void failsBindingWhenTargetTableMappingIsMissingSourceSchema() {
    contextRunner
        .withPropertyValues(
            "dblog.boot-mode=scenario",
            "dblog.target.enabled=true",
            "dblog.target.dialect=POSTGRES",
            "dblog.target.jdbc-url=jdbc:postgresql://target:5432/targetdb",
            "dblog.target.username=target-user",
            "dblog.target.table-mappings[0].source-table=widgets")
        .run(
            context -> {
              assertThat(context).hasFailed();
              assertThat(context.getStartupFailure())
                  .hasStackTraceContaining("sourceSchema")
                  .hasStackTraceContaining("must not be blank");
            });
  }

  @Test
  void failsBindingWhenTargetTableMappingIsMissingSourceTable() {
    contextRunner
        .withPropertyValues(
            "dblog.boot-mode=scenario",
            "dblog.target.enabled=true",
            "dblog.target.dialect=POSTGRES",
            "dblog.target.jdbc-url=jdbc:postgresql://target:5432/targetdb",
            "dblog.target.username=target-user",
            "dblog.target.table-mappings[0].source-schema=public",
            "dblog.target.table-mappings[0].source-table= ")
        .run(
            context -> {
              assertThat(context).hasFailed();
              assertThat(context.getStartupFailure())
                  .hasStackTraceContaining("sourceTable")
                  .hasStackTraceContaining("must not be blank");
            });
  }

  @Test
  void failsBindingWhenRuntimeModeHasNoExplicitOutputSink() {
    contextRunner
        .withPropertyValues(
            "dblog.boot-mode=runtime",
            "dblog.runtime.state-path=/tmp/runtime-state",
            "dblog.source.adapter=mysql",
            "dblog.source.id=source-1",
            "dblog.source.tables[0]=app.widgets",
            "dblog.source.mysql.jdbc-url=jdbc:mysql://localhost:3306/app",
            "dblog.source.mysql.username=dblog")
        .run(
            context -> {
              assertThat(context).hasFailed();
              assertThat(context.getStartupFailure())
                  .hasStackTraceContaining("At least one explicit output sink must be configured")
                  .hasStackTraceContaining("dblog.sink.noop.enabled=true");
            });
  }

  @Configuration(proxyBeanMethods = false)
  @EnableConfigurationProperties(DbLogProperties.class)
  static class PropertiesConfiguration {}
}
