package io.github.aandreakis.dblog.core.schema;

import io.github.aandreakis.dblog.core.model.TableId;
import java.time.Instant;
import java.util.Objects;

public record FullDumpRequiredSignal(
    String sourceId, TableId tableId, String reason, Instant detectedAt) {
  public FullDumpRequiredSignal {
    Objects.requireNonNull(sourceId, "sourceId");
    reason = Objects.requireNonNull(reason, "reason");
    detectedAt = Objects.requireNonNull(detectedAt, "detectedAt");
    if (sourceId.isBlank()) {
      throw new IllegalArgumentException("sourceId must not be blank");
    }
    if (reason.isBlank()) {
      throw new IllegalArgumentException("reason must not be blank");
    }
  }
}
