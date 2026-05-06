package io.github.aandreakis.dblog.config.validation;

import io.github.aandreakis.dblog.config.DbLogProperties;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

public final class ScenarioConfigurationValidator
    implements ConstraintValidator<ValidScenarioConfiguration, DbLogProperties> {
  @Override
  public boolean isValid(DbLogProperties properties, ConstraintValidatorContext context) {
    if (properties == null || properties.getBootMode() != DbLogProperties.BootMode.SCENARIO) {
      return true;
    }

    boolean valid = true;
    context.disableDefaultConstraintViolation();

    if (!DbLogPropertiesValidationSupport.hasText(properties.getScenario().getAdapter())) {
      valid &=
          violation(
              context,
              "dblog.scenario.adapter must not be blank when dblog.boot-mode=scenario",
              "scenario",
              "adapter");
    } else if (!DbLogPropertiesValidationSupport.isSupportedAdapter(
        properties.getScenario().getAdapter())) {
      valid &=
          violation(
              context,
              "dblog.scenario.adapter must be one of mysql, postgres, or postgresql when dblog.boot-mode=scenario",
              "scenario",
              "adapter");
    }

    if (!DbLogPropertiesValidationSupport.hasText(properties.getScenario().getId())) {
      valid &=
          violation(
              context,
              "dblog.scenario.id must not be blank when dblog.boot-mode=scenario",
              "scenario",
              "id");
    }

    if (properties.getScenario().getStatePath() == null) {
      valid &=
          violation(
              context,
              "dblog.scenario.state-path must be configured when dblog.boot-mode=scenario",
              "scenario",
              "statePath");
    }

    if (properties.getScenario().getSinkPath() == null) {
      valid &=
          violation(
              context,
              "dblog.scenario.sink-path must be configured when dblog.boot-mode=scenario",
              "scenario",
              "sinkPath");
    }

    String adapter = DbLogPropertiesValidationSupport.normalizedAdapter(properties.getScenario().getAdapter());
    switch (adapter) {
      case "mysql" -> {
        if (!DbLogPropertiesValidationSupport.hasText(properties.getScenario().getMysql().getJdbcUrl())) {
          valid &=
              violation(
                  context,
                  "dblog.scenario.mysql.jdbc-url must not be blank when dblog.scenario.adapter=mysql and dblog.boot-mode=scenario",
                  "scenario",
                  "mysql",
                  "jdbcUrl");
        }
        if (!DbLogPropertiesValidationSupport.hasText(properties.getScenario().getMysql().getUsername())) {
          valid &=
              violation(
                  context,
                  "dblog.scenario.mysql.username must not be blank when dblog.scenario.adapter=mysql and dblog.boot-mode=scenario",
                  "scenario",
                  "mysql",
                  "username");
        }
      }
      case "postgres" -> {
        if (!DbLogPropertiesValidationSupport.hasText(properties.getScenario().getPostgres().getJdbcUrl())) {
          valid &=
              violation(
                  context,
                  "dblog.scenario.postgres.jdbc-url must not be blank when dblog.scenario.adapter=postgres and dblog.boot-mode=scenario",
                  "scenario",
                  "postgres",
                  "jdbcUrl");
        }
        if (!DbLogPropertiesValidationSupport.hasText(properties.getScenario().getPostgres().getUsername())) {
          valid &=
              violation(
                  context,
                  "dblog.scenario.postgres.username must not be blank when dblog.scenario.adapter=postgres and dblog.boot-mode=scenario",
                  "scenario",
                  "postgres",
                  "username");
        }
      }
      default -> {
        // adapter presence/support already handled above
      }
    }
    return valid;
  }

  private static boolean violation(
      ConstraintValidatorContext context, String message, String firstNode, String... otherNodes) {
    ConstraintValidatorContext.ConstraintViolationBuilder.NodeBuilderCustomizableContext builder =
        context.buildConstraintViolationWithTemplate(message).addPropertyNode(firstNode);
    for (int index = 0; index < otherNodes.length - 1; index++) {
      builder = builder.addPropertyNode(otherNodes[index]);
    }
    if (otherNodes.length > 0) {
      builder.addPropertyNode(otherNodes[otherNodes.length - 1]).addConstraintViolation();
    } else {
      builder.addConstraintViolation();
    }
    return false;
  }
}
