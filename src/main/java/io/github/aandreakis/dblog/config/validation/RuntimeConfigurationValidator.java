package io.github.aandreakis.dblog.config.validation;

import io.github.aandreakis.dblog.config.DbLogProperties;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

public final class RuntimeConfigurationValidator
    implements ConstraintValidator<ValidRuntimeConfiguration, DbLogProperties> {
  @Override
  public boolean isValid(DbLogProperties properties, ConstraintValidatorContext context) {
    if (properties == null) {
      return true;
    }
    if (properties.getBootMode() != DbLogProperties.BootMode.RUNTIME
        && properties.getBootMode() != DbLogProperties.BootMode.STARTUP_CHECK) {
      return true;
    }

    boolean valid = true;
    context.disableDefaultConstraintViolation();

    if (!DbLogPropertiesValidationSupport.hasText(properties.getSource().getAdapter())) {
      valid &=
          violation(
              context,
              "dblog.source.adapter must not be blank when dblog.boot-mode=runtime or startup-check",
              "source",
              "adapter");
    } else if (!DbLogPropertiesValidationSupport.isSupportedAdapter(
        properties.getSource().getAdapter())) {
      valid &=
          violation(
              context,
              "dblog.source.adapter must be one of mysql, postgres, or postgresql when dblog.boot-mode=runtime or startup-check",
              "source",
              "adapter");
    }

    if (!DbLogPropertiesValidationSupport.hasText(properties.getSource().getId())) {
      valid &=
          violation(
              context,
              "dblog.source.id must not be blank when dblog.boot-mode=runtime or startup-check",
              "source",
              "id");
    }

    if (properties.getRuntime().getStatePath() == null) {
      valid &=
          violation(
              context,
              "dblog.runtime.state-path must be configured when dblog.boot-mode=runtime or startup-check",
              "runtime",
              "statePath");
    }

    if (!DbLogPropertiesValidationSupport.hasExplicitOutputSink(properties)) {
      valid &=
          violation(
              context,
              "At least one explicit output sink must be configured when dblog.boot-mode=runtime or startup-check; "
                  + "configure NDJSON, typed H2, JDBC target apply, or set dblog.sink.noop.enabled=true to discard intentionally",
              "sink");
    }

    if (!DbLogPropertiesValidationSupport.hasNonBlankEntries(properties.getSource().getTables())) {
      valid &=
          violation(
              context,
              "dblog.source.tables must not be empty when dblog.boot-mode=runtime or startup-check",
              "source",
              "tables");
    } else if (!DbLogPropertiesValidationSupport.usesTwoPartNames(properties.getSource().getTables())) {
      valid &=
          violation(
              context,
              "dblog.source.tables must use adapter-native two-part names such as schema.table when dblog.boot-mode=runtime or startup-check",
              "source",
              "tables");
    }

    String adapter = DbLogPropertiesValidationSupport.normalizedAdapter(properties.getSource().getAdapter());
    switch (adapter) {
      case "mysql" -> {
        if (!DbLogPropertiesValidationSupport.hasText(properties.getSource().getMysql().getJdbcUrl())) {
          valid &=
              violation(
                  context,
                  "dblog.source.mysql.jdbc-url must not be blank when dblog.source.adapter=mysql and dblog.boot-mode=runtime or startup-check",
                  "source",
                  "mysql",
                  "jdbcUrl");
        }
        if (!DbLogPropertiesValidationSupport.hasText(properties.getSource().getMysql().getUsername())) {
          valid &=
              violation(
                  context,
                  "dblog.source.mysql.username must not be blank when dblog.source.adapter=mysql and dblog.boot-mode=runtime or startup-check",
                  "source",
                  "mysql",
                  "username");
        }
      }
      case "postgres" -> {
        if (!DbLogPropertiesValidationSupport.hasText(properties.getSource().getPostgres().getJdbcUrl())) {
          valid &=
              violation(
                  context,
                  "dblog.source.postgres.jdbc-url must not be blank when dblog.source.adapter=postgres and dblog.boot-mode=runtime or startup-check",
                  "source",
                  "postgres",
                  "jdbcUrl");
        }
        if (!DbLogPropertiesValidationSupport.hasText(properties.getSource().getPostgres().getUsername())) {
          valid &=
              violation(
                  context,
                  "dblog.source.postgres.username must not be blank when dblog.source.adapter=postgres and dblog.boot-mode=runtime or startup-check",
                  "source",
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
