package io.github.aandreakis.dblog.sink.jdbc;

import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import java.util.List;
import java.util.Objects;

/** Shared factories for structured target-apply failures. */
final class TargetApplyFailures {
  private TargetApplyFailures() {}

  static TargetApplyOperationException preflightFailed(
      JdbcApplyTargetDialect dialect, String jdbcUrl, Throwable cause) {
    TargetApplyFailure failure =
        new TargetApplyFailure(
            TargetApplyFailureType.TARGET_PREFLIGHT_FAILED,
            null,
            null,
            null,
            List.of(),
            List.of(),
            null,
            null,
            null,
            null,
            "dialect=" + dialect.name().toLowerCase(java.util.Locale.ROOT) + " jdbcUrl=" + jdbcUrl,
            "VERIFY_TARGET_CONNECTIVITY_AND_CREDENTIALS");
    return new TargetApplyOperationException(
        failure,
        "Target apply preflight failed for "
            + dialect.name().toLowerCase(java.util.Locale.ROOT)
            + " target "
            + jdbcUrl,
        cause);
  }

  static TargetApplyContractException missingTargetTable(TableId sourceTableId, TableId targetTableId) {
    TargetApplyFailure failure =
        new TargetApplyFailure(
            TargetApplyFailureType.TARGET_TABLE_MISSING,
            sourceTableId,
            targetTableId,
            null,
            List.of(),
            List.of(),
            null,
            null,
            null,
            null,
            null,
            "ALIGN_TARGET_TABLE");
    return new TargetApplyContractException(
        failure,
        "Target table does not exist for JDBC apply sink: "
            + targetTableId.displayName()
            + " for source "
            + sourceTableId.displayName()
            + ". If the source and target engines differ (e.g. MySQL source -> PostgreSQL"
            + " target), the default identity resolver will not map source (schema,table) to"
            + " target (schema,table) correctly. Configure dblog.target.table-mappings[] to"
            + " remap source (schema,table) to target (schema,table) — see"
            + " docs/OPERATION.md \u00a74.3.8.");
  }

  static TargetApplyContractException primaryKeyMismatch(
      TableId sourceTableId,
      TableId targetTableId,
      List<String> expectedPrimaryKeyColumns,
      List<String> actualPrimaryKeyColumns) {
    TargetApplyFailure failure =
        new TargetApplyFailure(
            TargetApplyFailureType.TARGET_PRIMARY_KEY_MISMATCH,
            sourceTableId,
            targetTableId,
            null,
            expectedPrimaryKeyColumns,
            actualPrimaryKeyColumns,
            null,
            null,
            null,
            null,
            null,
            "ALIGN_TARGET_PRIMARY_KEY");
    return new TargetApplyContractException(
        failure,
        "Target table must expose the matching primary-key columns for JDBC apply sink: "
            + targetTableId.displayName()
            + " for source "
            + sourceTableId.displayName()
            + " expected primary key "
            + expectedPrimaryKeyColumns
            + " but was "
            + actualPrimaryKeyColumns);
  }

  static TargetApplyContractException missingTargetColumn(
      TableId sourceTableId, TableId targetTableId, String columnName) {
    TargetApplyFailure failure =
        new TargetApplyFailure(
            TargetApplyFailureType.TARGET_COLUMN_MISSING,
            sourceTableId,
            targetTableId,
            columnName,
            List.of(),
            List.of(),
            null,
            null,
            null,
            null,
            null,
            "ALIGN_TARGET_COLUMNS");
    return new TargetApplyContractException(
        failure,
        "Target table is missing required column for JDBC apply sink: "
            + targetTableId.displayName()
            + " for source "
            + sourceTableId.displayName()
            + " column="
            + columnName);
  }

  static TargetApplyContractException incompatibleTargetColumnType(
      TableId sourceTableId,
      TableId targetTableId,
      ColumnDefinition sourceColumn,
      JdbcApplyTargetSchemaInspector.TargetColumnMetadata targetColumn) {
    TargetApplyFailure failure =
        new TargetApplyFailure(
            TargetApplyFailureType.TARGET_COLUMN_TYPE_INCOMPATIBLE,
            sourceTableId,
            targetTableId,
            sourceColumn.name(),
            List.of(),
            List.of(),
            sourceColumn.sourceType(),
            sourceColumn.neutralType(),
            targetColumn.sourceType(),
            targetColumn.neutralType(),
            null,
            "ALIGN_TARGET_COLUMN_TYPES");
    return new TargetApplyContractException(
        failure,
        "Target column type is incompatible with captured schema for JDBC apply sink: "
            + targetTableId.displayName()
            + " for source "
            + sourceTableId.displayName()
            + " column="
            + sourceColumn.name()
            + " sourceType="
            + sourceColumn.sourceType()
            + " sourceNeutralType="
            + sourceColumn.neutralType()
            + " targetType="
            + targetColumn.sourceType()
            + " targetNeutralType="
            + targetColumn.neutralType());
  }

  static TargetApplyContractException valueCoercionFailed(
      TableId sourceTableId,
      TableId targetTableId,
      String sourceColumnName,
      JdbcApplyTargetSchemaInspector.TargetColumnMetadata targetColumn,
      Object value,
      RuntimeException cause) {
    TargetApplyFailure failure =
        new TargetApplyFailure(
            TargetApplyFailureType.TARGET_VALUE_COERCION_FAILED,
            sourceTableId,
            targetTableId,
            sourceColumnName,
            List.of(),
            List.of(),
            null,
            null,
            targetColumn.sourceType(),
            targetColumn.neutralType(),
            "valueClass=" + value.getClass().getName() + " value=" + previewValue(value),
            "ALIGN_TARGET_SCHEMA_OR_CONVERSION_POLICY");
    return new TargetApplyContractException(
        failure,
        "Target sink cannot coerce source value for "
            + targetTableId.displayName()
            + " column="
            + sourceColumnName
            + " targetType="
            + targetColumn.sourceType()
            + " targetNeutralType="
            + targetColumn.neutralType()
            + " valueClass="
            + value.getClass().getName()
            + " value="
            + previewValue(value)
            + "; sink operator must align the target schema or conversion policy",
        cause);
  }

  static TargetApplyOperationException metadataLookupFailed(
      TableId sourceTableId, TableId targetTableId, Throwable cause) {
    TargetApplyFailure failure =
        new TargetApplyFailure(
            TargetApplyFailureType.TARGET_METADATA_LOOKUP_FAILED,
            sourceTableId,
            targetTableId,
            null,
            List.of(),
            List.of(),
            null,
            null,
            null,
            null,
            null,
            "VERIFY_TARGET_SCHEMA_ACCESS");
    return new TargetApplyOperationException(
        failure,
        "Failed to inspect target table metadata for JDBC apply sink: "
            + targetTableId.displayName()
            + " for source "
            + sourceTableId.displayName(),
        cause);
  }

  static TargetApplyOperationException operationFailed(
      JdbcApplyTargetDialect dialect, String jdbcUrl, TableId targetTableId, Throwable cause) {
    String detail =
        targetTableId == null ? "jdbcUrl=" + jdbcUrl : "targetTable=" + targetTableId.displayName();
    TargetApplyFailure failure =
        new TargetApplyFailure(
            TargetApplyFailureType.TARGET_OPERATION_FAILED,
            null,
            targetTableId,
            null,
            List.of(),
            List.of(),
            null,
            null,
            null,
            null,
            detail,
            "VERIFY_TARGET_AVAILABILITY_PERMISSIONS_AND_CONSTRAINTS");
    String message =
        "JDBC apply sink operation failed for "
            + dialect.name().toLowerCase(java.util.Locale.ROOT)
            + " target "
            + jdbcUrl;
    if (targetTableId != null) {
      message += " table=" + targetTableId.displayName();
    }
    return new TargetApplyOperationException(failure, message, cause);
  }

  private static String previewValue(Object value) {
    if (value == null) {
      return "null";
    }
    String rendered = String.valueOf(value);
    if (rendered.length() <= 120) {
      return rendered;
    }
    return rendered.substring(0, 117) + "...";
  }
}
