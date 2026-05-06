package io.github.aandreakis.dblog.adapter.api;

import java.util.Objects;

/**
 * Dialect-neutral validator helpers shared across adapters. Dialect-specific rules
 * (database-consistency invariants, column-type classification) live on each
 * {@link SourceDialect}, not here.
 */
public final class RelationalSourceConfigValidator {
  private RelationalSourceConfigValidator() {}

  public static void requireTablePartCount(
      RelationalSourceConfig config, int requiredParts, String label) {
    Objects.requireNonNull(config, "config");
    if (requiredParts < 2) {
      throw new IllegalArgumentException("requiredParts must be >= 2");
    }
    if (config.capturedTables().isEmpty()) {
      throw new IllegalArgumentException(label + " capturedTables must not be empty");
    }
    for (String table : config.capturedTables()) {
      if (table == null || table.isBlank()) {
        throw new IllegalArgumentException(label + " capturedTables entries must not be blank");
      }
      String[] parts = table.split("\\.");
      if (parts.length != requiredParts || hasBlankPart(parts)) {
        throw new IllegalArgumentException(label + capturedTablesShapeSuffix(requiredParts));
      }
    }
  }

  private static boolean hasBlankPart(String[] parts) {
    for (String part : parts) {
      if (part.isBlank()) {
        return true;
      }
    }
    return false;
  }

  private static String capturedTablesShapeSuffix(int requiredParts) {
    return " capturedTables must use adapter-native "
        + partsWord(requiredParts)
        + "-part names such as "
        + partsExample(requiredParts);
  }

  private static String partsWord(int requiredParts) {
    return switch (requiredParts) {
      case 2 -> "two";
      case 3 -> "three";
      default -> Integer.toString(requiredParts);
    };
  }

  private static String partsExample(int requiredParts) {
    return switch (requiredParts) {
      case 2 -> "schema.table";
      case 3 -> "catalog.schema.table";
      default -> "a.b.c";
    };
  }

  public static String requireJdbcPrefix(RelationalSourceConfig config, String prefix, String label) {
    Objects.requireNonNull(config, "config");
    String jdbcUrl = config.jdbcUrl();
    if (!jdbcUrl.startsWith(prefix)) {
      throw new IllegalArgumentException(label + " jdbcUrl must start with " + prefix);
    }
    return jdbcUrl;
  }

  public static String databaseNameFromJdbcUrl(String jdbcUrl, String prefix, String label) {
    Objects.requireNonNull(jdbcUrl, "jdbcUrl");
    if (!jdbcUrl.startsWith(prefix)) {
      throw new IllegalArgumentException(label + " jdbcUrl must start with " + prefix);
    }
    String suffix = jdbcUrl.substring(prefix.length());
    int slashIndex = suffix.indexOf('/');
    if (slashIndex < 0 || slashIndex == suffix.length() - 1) {
      throw new IllegalArgumentException(
          "Could not infer " + label + " database name from JDBC URL: " + jdbcUrl);
    }
    String dbPart = suffix.substring(slashIndex + 1);
    int queryIndex = dbPart.indexOf('?');
    if (queryIndex >= 0) {
      dbPart = dbPart.substring(0, queryIndex);
    }
    int paramIndex = dbPart.indexOf(';');
    if (paramIndex >= 0) {
      dbPart = dbPart.substring(0, paramIndex);
    }
    if (dbPart.isBlank()) {
      throw new IllegalArgumentException(
          "Could not infer " + label + " database name from JDBC URL: " + jdbcUrl);
    }
    return dbPart;
  }
}
