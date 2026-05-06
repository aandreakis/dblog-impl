package io.github.aandreakis.dblog.core.schema;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public final class SchemaPolicyEngine {
  public SchemaDecision evaluate(TableSchema contract, TableSchema live) {
    ReconciledSchema reconciled = reconcile(contract, live);
    if (!reconciled.schemaChanged()) {
      return SchemaDecision.UNCHANGED;
    }
    return SchemaDecision.CHANGED;
  }

  public ReconciledSchema reconcile(TableSchema contract, TableSchema live) {
    Objects.requireNonNull(contract, "contract");
    Objects.requireNonNull(live, "live");

    if (!contract.tableId().equals(live.tableId())) {
      throw new SchemaDriftException(
          "table identity changed from %s to %s"
              .formatted(contract.tableId().displayName(), live.tableId().displayName()));
    }

    LinkedHashMap<String, ColumnDefinition> liveColumnsByName = new LinkedHashMap<>();
    for (ColumnDefinition column : live.columns()) {
      liveColumnsByName.put(column.name(), column);
    }

    for (String configuredColumn : contract.selectedColumnNames()) {
      if (!liveColumnsByName.containsKey(configuredColumn)) {
        throw new SchemaDriftException(
            "configured column "
                + configuredColumn
                + " no longer exists for table "
                + contract.tableId().displayName());
      }
    }

    Set<String> includedColumnNames = new LinkedHashSet<>(contract.selectedColumnNames());
    includedColumnNames.addAll(live.primaryKeyColumns());

    List<String> ignoredColumns = new ArrayList<>();
    for (ColumnDefinition liveColumn : live.columns()) {
      if (!includedColumnNames.contains(liveColumn.name())) {
        ignoredColumns.add(liveColumn.name());
      }
    }

    TableSchema reconciled =
        new TableSchema(
            live.tableId(),
            live.columns(),
            live.primaryKeyColumns(),
            null,
            live.refreshedAt().equals(Instant.EPOCH) ? contract.refreshedAt() : live.refreshedAt(),
            ignoredColumns);

    boolean primaryKeyChanged =
        !contract.primaryKeyFingerprint().equals(reconciled.primaryKeyFingerprint());
    boolean schemaChanged = !contract.fingerprint().equals(reconciled.fingerprint());
    return new ReconciledSchema(reconciled, primaryKeyChanged, schemaChanged);
  }

  public TableSchema reconcileStrict(TableSchema contract, TableSchema live, String context) {
    ReconciledSchema reconciled = reconcile(contract, live);
    if (reconciled.primaryKeyChanged()) {
      throw new SchemaDriftException(
          driftMessage(context, live, "primary-key contract changed"));
    }
    if (reconciled.schemaChanged()) {
      throw new SchemaDriftException(
          driftMessage(context, live, "selected-column contract changed"));
    }
    return reconciled.schema();
  }

  private static String driftMessage(String context, TableSchema live, String reason) {
    String prefix = context == null || context.isBlank() ? "schema reconciliation" : context;
    return prefix
        + " detected "
        + reason
        + " for table "
        + live.tableId().displayName()
        + "; full dump required because schema continuity is uncertain";
  }

  public record ReconciledSchema(
      TableSchema schema, boolean primaryKeyChanged, boolean schemaChanged) {
    public ReconciledSchema {
      schema = Objects.requireNonNull(schema, "schema");
    }
  }

  public enum SchemaDecision {
    UNCHANGED,
    CHANGED
  }
}
