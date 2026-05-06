package io.github.aandreakis.dblog.verification.scenario.postgres;

import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Scenario-table schema fixture plus publication/slot names for PostgreSQL runs. */
public record PostgresScenarioSchema(
    String schemaName,
    TableSchema widgets,
    TableSchema gadgets,
    String publicationName,
    String slotName) {
  private static final int MAX_NORMALIZED_IDENTIFIER_LENGTH = 48;

  public static PostgresScenarioSchema forScenario(PostgresScenarioConfig config) {
    String normalized = sanitize(config.scenarioId());
    String schemaName = "scn_" + normalized;
    String widgetNameType = "text";
    TableSchema widgets =
        TableSchema.create(
            new TableId(config.databaseName(), schemaName, "widgets"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", widgetNameType, NeutralColumnType.STRING, false, true),
                new ColumnDefinition("enabled", "boolean", NeutralColumnType.BOOLEAN, false, true),
                new ColumnDefinition("payload", "bytea", NeutralColumnType.BINARY, false, true)),
            Instant.parse("2026-03-22T00:00:00Z"));
    TableSchema gadgets =
        TableSchema.create(
            new TableId(config.databaseName(), schemaName, "gadgets"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-03-22T00:00:00Z"));
    return new PostgresScenarioSchema(
        schemaName, widgets, gadgets, "pub_" + normalized, "slot_" + normalized);
  }

  public List<TableSchema> capturedSchemas() {
    return List.of(widgets, gadgets);
  }

  public Set<String> capturedTableNames() {
    return new LinkedHashSet<>(
        List.of(widgets.tableId().displayName(), gadgets.tableId().displayName()));
  }

  private static String sanitize(String value) {
    String sanitized =
        value.trim().toLowerCase(java.util.Locale.ROOT).replaceAll("[^a-z0-9_]", "_");
    if (sanitized.isBlank()) {
      return "scenario";
    }
    if (sanitized.length() > MAX_NORMALIZED_IDENTIFIER_LENGTH) {
      return sanitized.substring(0, MAX_NORMALIZED_IDENTIFIER_LENGTH);
    }
    return sanitized;
  }
}
