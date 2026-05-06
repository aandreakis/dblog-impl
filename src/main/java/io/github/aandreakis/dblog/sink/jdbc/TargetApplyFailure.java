package io.github.aandreakis.dblog.sink.jdbc;

import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import java.util.List;
import java.util.Objects;

/** Structured details for one target-apply failure. */
public record TargetApplyFailure(
    TargetApplyFailureType type,
    TableId sourceTableId,
    TableId targetTableId,
    String columnName,
    List<String> expectedPrimaryKeyColumns,
    List<String> actualPrimaryKeyColumns,
    String sourceType,
    NeutralColumnType sourceNeutralType,
    String targetType,
    NeutralColumnType targetNeutralType,
    String detail,
    String operatorAction) {
  public TargetApplyFailure {
    type = Objects.requireNonNull(type, "type");
    expectedPrimaryKeyColumns =
        List.copyOf(expectedPrimaryKeyColumns == null ? List.of() : expectedPrimaryKeyColumns);
    actualPrimaryKeyColumns =
        List.copyOf(actualPrimaryKeyColumns == null ? List.of() : actualPrimaryKeyColumns);
    operatorAction = requireNonBlank(operatorAction, "operatorAction");
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
