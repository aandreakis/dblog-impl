package io.github.aandreakis.dblog.config.validation;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = TargetConfigurationValidator.class)
public @interface ValidTargetConfiguration {
  String message() default "Invalid target configuration";

  Class<?>[] groups() default {};

  Class<? extends Payload>[] payload() default {};
}
