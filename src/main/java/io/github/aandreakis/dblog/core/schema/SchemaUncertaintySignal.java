package io.github.aandreakis.dblog.core.schema;

import io.github.aandreakis.dblog.core.model.TableId;
import java.time.Instant;
import java.util.Objects;

public record SchemaUncertaintySignal(
    String sourceId,
    TableId tableId,
    String reason,
    Instant firstDetectedAt,
    Instant lastDetectedAt,
    int occurrenceCount) {
  public SchemaUncertaintySignal {
    Objects.requireNonNull(sourceId, "sourceId");
    Objects.requireNonNull(reason, "reason");
    Objects.requireNonNull(firstDetectedAt, "firstDetectedAt");
    Objects.requireNonNull(lastDetectedAt, "lastDetectedAt");
    if (sourceId.isBlank()) {
      throw new IllegalArgumentException("sourceId must not be blank");
    }
    if (reason.isBlank()) {
      throw new IllegalArgumentException("reason must not be blank");
    }
    if (occurrenceCount <= 0) {
      throw new IllegalArgumentException("occurrenceCount must be > 0");
    }
  }
}
