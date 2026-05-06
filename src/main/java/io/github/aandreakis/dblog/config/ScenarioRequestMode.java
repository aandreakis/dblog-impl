package io.github.aandreakis.dblog.config;

import java.util.Locale;

/** Shared scenario/benchmark request-mode selector. */
public enum ScenarioRequestMode {
  STREAMING_ONLY,
  ALL_TABLES,
  TABLE,
  PRIMARY_KEYS;

  public static ScenarioRequestMode parse(String raw) {
    if (raw == null || raw.isBlank()) {
      return ALL_TABLES;
    }
    return switch (raw.trim().toUpperCase(Locale.ROOT)) {
      case "STREAMING_ONLY", "STREAMING" -> STREAMING_ONLY;
      case "ALL_TABLES", "ALL" -> ALL_TABLES;
      case "TABLE" -> TABLE;
      case "PRIMARY_KEYS", "PRIMARY_KEY", "TARGETED_REPAIR", "REPAIR" -> PRIMARY_KEYS;
      default -> throw new IllegalArgumentException("Unsupported scenario request mode: " + raw);
    };
  }
}
