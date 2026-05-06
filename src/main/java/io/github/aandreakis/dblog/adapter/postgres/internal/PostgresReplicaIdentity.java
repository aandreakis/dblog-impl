package io.github.aandreakis.dblog.adapter.postgres.internal;

import java.util.Locale;
import java.util.Objects;

/** PostgreSQL replica-identity modes exposed through {@code pg_class.relreplident}. */
public enum PostgresReplicaIdentity {
  DEFAULT('d'),
  NOTHING('n'),
  FULL('f'),
  INDEX('i');

  private final char catalogCode;

  PostgresReplicaIdentity(char catalogCode) {
    this.catalogCode = catalogCode;
  }

  public char catalogCode() {
    return catalogCode;
  }

  public static PostgresReplicaIdentity fromCatalogCode(String catalogCode) {
    Objects.requireNonNull(catalogCode, "catalogCode");
    if (catalogCode.isBlank() || catalogCode.length() != 1) {
      throw new IllegalArgumentException(
          "PostgreSQL replica-identity catalog code must be exactly one character");
    }
    return switch (Character.toLowerCase(catalogCode.charAt(0))) {
      case 'd' -> DEFAULT;
      case 'n' -> NOTHING;
      case 'f' -> FULL;
      case 'i' -> INDEX;
      default -> throw new IllegalArgumentException(
          "unknown PostgreSQL replica-identity catalog code: "
              + catalogCode.toUpperCase(Locale.ROOT));
    };
  }
}
