package io.github.aandreakis.dblog.verification.scenario.mysql.internal;

import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import org.springframework.core.env.Environment;

/** Shared validation/parsing helpers for the MySQL scenario config records. */
public final class MySqlScenarioConfigSupport {
  private MySqlScenarioConfigSupport() {}

  public static String required(Environment environment, String key) {
    String value = environment.getProperty(key);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("Required scenario property is missing: " + key);
    }
    return value;
  }

  public static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  public static Duration positiveDuration(Duration value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isZero() || value.isNegative()) {
      throw new IllegalArgumentException(name + " must be > 0");
    }
    return value;
  }

  public static Duration nonNegativeDuration(Duration value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isNegative()) {
      throw new IllegalArgumentException(name + " must be >= 0");
    }
    return value;
  }

  public static void requirePositiveWhenPresent(Integer value, String name) {
    if (value != null && value <= 0) {
      throw new IllegalArgumentException(name + " must be > 0 when present");
    }
  }

  public static String databaseNameFromJdbcUrl(String jdbcUrl, String adapterLabel) {
    int slash = jdbcUrl.lastIndexOf('/');
    if (slash < 0 || slash == jdbcUrl.length() - 1) {
      throw new IllegalArgumentException(
          "Could not infer " + adapterLabel + " database name from JDBC URL");
    }
    String tail = jdbcUrl.substring(slash + 1);
    int params = tail.indexOf('?');
    return params >= 0 ? tail.substring(0, params) : tail;
  }

  public static HostPort parseHostPort(
      String jdbcUrl, String jdbcPrefix, String adapterLabel, int defaultPort) {
    try {
      String withoutPrefix =
          jdbcUrl.replaceFirst(
              "^" + jdbcPrefix, adapterLabel.toLowerCase(java.util.Locale.ROOT) + "://");
      URI uri = URI.create(withoutPrefix);
      return new HostPort(uri.getHost(), uri.getPort() > 0 ? uri.getPort() : defaultPort);
    } catch (RuntimeException ex) {
      throw new IllegalArgumentException(
          "Could not parse " + adapterLabel + " JDBC URL host/port: " + jdbcUrl, ex);
    }
  }

  public record HostPort(String host, int port) {}
}
