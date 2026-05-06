package io.github.aandreakis.dblog.core.schema;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Safety rules for updating shared decode-schema assumptions for primary-key columns. */
public final class PrimaryKeyCompatibility {
  private PrimaryKeyCompatibility() {}

  public static boolean isSafeDecodeUpdate(TableSchema currentSchema, TableSchema candidateSchema) {
    Objects.requireNonNull(currentSchema, "currentSchema");
    Objects.requireNonNull(candidateSchema, "candidateSchema");
    if (!currentSchema.tableId().equals(candidateSchema.tableId())) {
      return false;
    }
    if (!currentSchema.primaryKeyColumns().equals(candidateSchema.primaryKeyColumns())) {
      return false;
    }
    Map<String, ColumnDefinition> currentPrimaryKeys = primaryKeysByName(currentSchema);
    Map<String, ColumnDefinition> candidatePrimaryKeys = primaryKeysByName(candidateSchema);
    for (String primaryKeyColumn : currentSchema.primaryKeyColumns()) {
      ColumnDefinition current = currentPrimaryKeys.get(primaryKeyColumn);
      ColumnDefinition candidate = candidatePrimaryKeys.get(primaryKeyColumn);
      if (current == null || candidate == null) {
        return false;
      }
      if (!current.sourceType().equals(candidate.sourceType())) {
        return false;
      }
      if (current.neutralType() != candidate.neutralType()) {
        return false;
      }
    }
    return true;
  }

  private static Map<String, ColumnDefinition> primaryKeysByName(TableSchema schema) {
    LinkedHashMap<String, ColumnDefinition> byName = new LinkedHashMap<>();
    for (ColumnDefinition column : schema.primaryKeyDefinitions()) {
      byName.put(column.name(), column);
    }
    return Map.copyOf(byName);
  }
}
