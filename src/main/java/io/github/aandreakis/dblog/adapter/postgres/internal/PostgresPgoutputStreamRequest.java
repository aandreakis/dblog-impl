package io.github.aandreakis.dblog.adapter.postgres.internal;

import io.github.aandreakis.dblog.adapter.postgres.PostgresLsn;
import java.time.Duration;
import java.util.Objects;

/** Startup request for the PostgreSQL pgoutput stream wrapper. */
public record PostgresPgoutputStreamRequest(
    String slotName,
    String publicationName,
    PostgresLsn startLsn,
    Duration statusInterval) {
  public PostgresPgoutputStreamRequest {
    slotName = requireNonBlank(slotName, "slotName");
    publicationName = requireNonBlank(publicationName, "publicationName");
    if (statusInterval != null && (statusInterval.isZero() || statusInterval.isNegative())) {
      throw new IllegalArgumentException("statusInterval must be > 0 when present");
    }
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
