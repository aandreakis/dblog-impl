package io.github.aandreakis.dblog.state.api;

import io.github.aandreakis.dblog.core.request.DumpRequestStatus;
import java.util.Objects;

/**
 * A {@link StoredDumpRequest} joined with its current {@link DumpRequestStatus}. {@code status}
 * may be {@code null} when a request has been persisted but no status row has been written yet.
 */
public record StoredDumpRequestDetail(StoredDumpRequest storedRequest, DumpRequestStatus status) {
  public StoredDumpRequestDetail {
    storedRequest = Objects.requireNonNull(storedRequest, "storedRequest");
  }
}
