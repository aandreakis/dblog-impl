package io.github.aandreakis.dblog.verification.scenario.mysql.internal;

import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Shared scenario-table schema fixture for the MySQL harness. */
public record MySqlScenarioSchema(
    String databaseName, TableSchema widgets, TableSchema gadgets) {
  public static MySqlScenarioSchema forScenario(String sourceId, String databaseName) {
    String widgetNameType = "varchar(255)";
    TableSchema widgets =
        TableSchema.create(
            new TableId(sourceId, databaseName, "widgets"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", widgetNameType, NeutralColumnType.STRING, false, true),
                new ColumnDefinition("enabled", "tinyint", NeutralColumnType.BOOLEAN, false, true),
                new ColumnDefinition(
                    "payload", "varbinary(255)", NeutralColumnType.BINARY, false, true)),
            Instant.parse("2026-03-22T00:00:00Z"));
    TableSchema gadgets =
        TableSchema.create(
            new TableId(sourceId, databaseName, "gadgets"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-03-22T00:00:00Z"));
    return new MySqlScenarioSchema(databaseName, widgets, gadgets);
  }

  public List<TableSchema> capturedSchemas() {
    return List.of(widgets, gadgets);
  }

  public Set<String> capturedTableNames() {
    return new LinkedHashSet<>(List.of(widgets.tableId().displayName(), gadgets.tableId().displayName()));
  }
}
