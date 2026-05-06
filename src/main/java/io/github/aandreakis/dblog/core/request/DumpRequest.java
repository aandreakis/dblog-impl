package io.github.aandreakis.dblog.core.request;

import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.util.List;
import java.util.Objects;

public record DumpRequest(
    String requestId, DumpScope scope, TableId tableId, List<PrimaryKeyTuple> primaryKeyTuples) {
  public static final int MAX_PRIMARY_KEY_LITERALS = 500;

  public DumpRequest {
    Objects.requireNonNull(requestId, "requestId");
    Objects.requireNonNull(scope, "scope");
    primaryKeyTuples = primaryKeyTuples == null ? List.of() : List.copyOf(primaryKeyTuples);

    if (requestId.isBlank()) {
      throw new IllegalArgumentException("requestId must not be blank");
    }

    if (scope == DumpScope.TABLE && tableId == null) {
      throw new IllegalArgumentException("table scope requires tableId");
    }
    if (scope == DumpScope.TABLE && !primaryKeyTuples.isEmpty()) {
      throw new IllegalArgumentException("table scope must not carry primary-key literals");
    }

    if (scope == DumpScope.PRIMARY_KEYS && (tableId == null || primaryKeyTuples.isEmpty())) {
      throw new IllegalArgumentException("primary-key scope requires tableId and keys");
    }
    if (scope == DumpScope.PRIMARY_KEYS && primaryKeyTuples.size() > MAX_PRIMARY_KEY_LITERALS) {
      throw new IllegalArgumentException(
          "primary-key scope accepts at most " + MAX_PRIMARY_KEY_LITERALS + " keys per request");
    }
    if (scope == DumpScope.ALL_TABLES && tableId != null) {
      throw new IllegalArgumentException("all-tables scope must not carry tableId");
    }
    if (scope == DumpScope.ALL_TABLES && !primaryKeyTuples.isEmpty()) {
      throw new IllegalArgumentException("all-tables scope must not carry primary-key literals");
    }
  }

  public static DumpRequest fromPrimaryKeyLiterals(
      String requestId,
      DumpScope scope,
      TableId tableId,
      TableSchema schema,
      List<String> primaryKeyLiterals) {
    Objects.requireNonNull(schema, "schema");
    return new DumpRequest(
        requestId,
        scope,
        tableId,
        schema.primaryKeyTuplesFromLiterals(
            primaryKeyLiterals == null ? List.of() : List.copyOf(primaryKeyLiterals)));
  }

  public List<String> primaryKeyLiterals() {
    return primaryKeyTuples.stream().map(PrimaryKeyTuple::literal).toList();
  }
}
