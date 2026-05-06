package io.github.aandreakis.dblog.core.request;

import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import java.util.List;
import java.util.Objects;

public record DumpRequestStatus(
    String requestId,
    DumpScope scope,
    TableId tableId,
    DumpRequestState state,
    List<PrimaryKeyTuple> missingPrimaryKeyTuples,
    String failureReason) {

  public DumpRequestStatus {
    Objects.requireNonNull(requestId, "requestId");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(state, "state");
    missingPrimaryKeyTuples =
        missingPrimaryKeyTuples == null ? List.of() : List.copyOf(missingPrimaryKeyTuples);

    if (requestId.isBlank()) {
      throw new IllegalArgumentException("requestId must not be blank");
    }

    if ((scope == DumpScope.TABLE || scope == DumpScope.PRIMARY_KEYS) && tableId == null) {
      throw new IllegalArgumentException("table-scoped dump request status requires tableId");
    }

    if (scope != DumpScope.PRIMARY_KEYS && !missingPrimaryKeyTuples.isEmpty()) {
      throw new IllegalArgumentException(
          "only primary-key repair request status may record missing primary keys");
    }

    if (state == DumpRequestState.FAILED) {
      if (failureReason == null || failureReason.isBlank()) {
        throw new IllegalArgumentException("failed dump request status requires failureReason");
      }
    } else if (failureReason != null) {
      throw new IllegalArgumentException(
          "only failed dump request status may include a failureReason");
    }
  }

  public static DumpRequestStatus active(DumpRequest request) {
    Objects.requireNonNull(request, "request");
    return new DumpRequestStatus(
        request.requestId(),
        request.scope(),
        request.tableId(),
        DumpRequestState.ACTIVE,
        List.of(),
        null);
  }

  public static DumpRequestStatus completed(
      DumpRequest request, List<PrimaryKeyTuple> missingPrimaryKeyTuples) {
    Objects.requireNonNull(request, "request");
    return new DumpRequestStatus(
        request.requestId(),
        request.scope(),
        request.tableId(),
        DumpRequestState.COMPLETED,
        missingPrimaryKeyTuples,
        null);
  }

  public static DumpRequestStatus failed(DumpRequest request, String failureReason) {
    Objects.requireNonNull(request, "request");
    return new DumpRequestStatus(
        request.requestId(),
        request.scope(),
        request.tableId(),
        DumpRequestState.FAILED,
        List.of(),
        Objects.requireNonNull(failureReason, "failureReason"));
  }

  public List<String> missingPrimaryKeyLiterals() {
    return missingPrimaryKeyTuples.stream().map(PrimaryKeyTuple::literal).toList();
  }
}
