package io.github.aandreakis.dblog.config.validation;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = ScenarioConfigurationValidator.class)
public @interface ValidScenarioConfiguration {
  String message() default "Invalid scenario configuration";

  Class<?>[] groups() default {};

  Class<? extends Payload>[] payload() default {};
}
