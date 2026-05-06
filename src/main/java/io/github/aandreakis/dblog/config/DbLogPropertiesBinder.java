package io.github.aandreakis.dblog.config;

import java.util.Map;
import java.util.Objects;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

/** Shared Spring Binder entrypoint for DBLog typed configuration. */
public final class DbLogPropertiesBinder {
  private DbLogPropertiesBinder() {}

  public static DbLogProperties bind(Map<String, String> properties) {
    Objects.requireNonNull(properties, "properties");
    return new Binder(new MapConfigurationPropertySource(properties))
        .bind("dblog", Bindable.of(DbLogProperties.class))
        .orElseGet(DbLogProperties::new);
  }
}
