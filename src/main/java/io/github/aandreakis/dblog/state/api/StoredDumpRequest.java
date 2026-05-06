package io.github.aandreakis.dblog.state.api;

import io.github.aandreakis.dblog.core.request.DumpRequest;
import java.time.Instant;
import java.util.Objects;

/** Pairs a persisted {@link DumpRequest} with its store-side creation and update timestamps. */
public record StoredDumpRequest(DumpRequest request, Instant createdAt, Instant updatedAt) {
  public StoredDumpRequest {
    request = Objects.requireNonNull(request, "request");
    createdAt = Objects.requireNonNull(createdAt, "createdAt");
    updatedAt = Objects.requireNonNull(updatedAt, "updatedAt");
  }
}
