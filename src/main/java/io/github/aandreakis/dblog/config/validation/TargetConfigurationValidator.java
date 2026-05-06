package io.github.aandreakis.dblog.config.validation;

import io.github.aandreakis.dblog.config.DbLogProperties;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

public final class TargetConfigurationValidator
    implements ConstraintValidator<ValidTargetConfiguration, DbLogProperties> {
  @Override
  public boolean isValid(DbLogProperties properties, ConstraintValidatorContext context) {
    if (properties == null || !properties.getTarget().isEnabled()) {
      return true;
    }

    boolean valid = true;
    context.disableDefaultConstraintViolation();

    if (properties.getTarget().getDialect() == null) {
      valid &=
          violation(
              context,
              "dblog.target.dialect must be configured when dblog.target.enabled=true",
              "target",
              "dialect");
    }
    if (!DbLogPropertiesValidationSupport.hasText(properties.getTarget().getJdbcUrl())) {
      valid &=
          violation(
              context,
              "dblog.target.jdbc-url must not be blank when dblog.target.enabled=true",
              "target",
              "jdbcUrl");
    }
    if (!DbLogPropertiesValidationSupport.hasText(properties.getTarget().getUsername())) {
      valid &=
          violation(
              context,
              "dblog.target.username must not be blank when dblog.target.enabled=true",
              "target",
              "username");
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
