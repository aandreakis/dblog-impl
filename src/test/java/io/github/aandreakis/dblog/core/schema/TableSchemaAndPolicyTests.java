package io.github.aandreakis.dblog.core.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.core.model.TableId;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TableSchemaAndPolicyTests {
  private static final Instant REFRESHED_AT = Instant.parse("2026-03-25T00:00:00Z");
  private static final TableId TABLE_ID = new TableId("source", "appdb", "widgets");

  @Test
  void createTracksIgnoredColumnsAndPrimaryKeyUtilitiesUseCanonicalOrdering() {
    TableSchema schema =
        TableSchema.create(
            TABLE_ID,
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true),
                new ColumnDefinition("payload", "json", NeutralColumnType.UNSUPPORTED, false, true)),
            REFRESHED_AT);

    assertThat(schema.primaryKeyColumn()).isEqualTo("id");
    assertThat(schema.ignoredColumns()).containsExactly("payload");
    assertThat(schema.selectedColumns()).extracting(ColumnDefinition::name).containsExactly("id", "name");
    assertThat(schema.primaryKeyLiteralFor(Map.of("id", "42", "name", "widget"))).isEqualTo("42");
    assertThat(schema.canonicalPrimaryKeyLiteral("00042")).isEqualTo("42");
    assertThat(schema.comparePrimaryKeyRows(Map.of("id", "2"), Map.of("id", "10"))).isLessThan(0);
    assertThat(schema.comparePrimaryKeyLiterals("2", "10")).isLessThan(0);
  }

  @Test
  void supportsCompositePrimaryKeysAndSchemaReconciliation() {
    TableSchema contract =
        TableSchema.create(
            TABLE_ID,
            List.of(
                new ColumnDefinition("tenant_id", "varchar(255)", NeutralColumnType.STRING, true, false),
                new ColumnDefinition("widget_id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true),
                new ColumnDefinition("payload", "json", NeutralColumnType.UNSUPPORTED, false, true)),
            REFRESHED_AT);
    TableSchema live =
        TableSchema.create(
            TABLE_ID,
            List.of(
                new ColumnDefinition("tenant_id", "varchar(255)", NeutralColumnType.STRING, true, false),
                new ColumnDefinition("widget_id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true),
                new ColumnDefinition("payload", "xml", NeutralColumnType.UNSUPPORTED, false, true),
                new ColumnDefinition("audit_note", "varchar(255)", NeutralColumnType.STRING, false, true)),
            REFRESHED_AT.plusSeconds(60));

    SchemaPolicyEngine.ReconciledSchema reconciled = new SchemaPolicyEngine().reconcile(contract, live);

    assertThat(contract.primaryKeyColumns()).containsExactly("tenant_id", "widget_id");
    assertThat(contract.canonicalPrimaryKeyLiteral("(north\\,west,00042)"))
        .isEqualTo("{tenant_id=north\\,west,widget_id=42}");
    assertThat(reconciled.schemaChanged()).isFalse();
    assertThat(reconciled.primaryKeyChanged()).isFalse();
    assertThat(reconciled.schema().ignoredColumns()).containsExactly("payload", "audit_note");
  }

  @Test
  void rejectsMissingConfiguredColumnsAndIdentityChanges() {
    TableSchema contract =
        TableSchema.create(
            TABLE_ID,
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            REFRESHED_AT);
    TableSchema missingConfiguredColumn =
        TableSchema.create(
            TABLE_ID,
            List.of(new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false)),
            REFRESHED_AT.plusSeconds(120));

    assertThatThrownBy(() -> new SchemaPolicyEngine().reconcile(contract, missingConfiguredColumn))
        .isInstanceOf(SchemaDriftException.class)
        .hasMessageContaining("configured column");
    assertThatThrownBy(
            () ->
                new SchemaPolicyEngine()
                    .reconcile(
                        contract,
                        new TableSchema(
                            new TableId("other", "appdb", "widgets"),
                            contract.columns(),
                            contract.primaryKeyColumns(),
                            contract.fingerprint(),
                            contract.refreshedAt(),
                            contract.ignoredColumns())))
        .isInstanceOf(SchemaDriftException.class)
        .hasMessageContaining("table identity changed");
  }

  @Test
  void strictReconciliationRejectsSelectedColumnFingerprintDrift() {
    TableSchema contract =
        TableSchema.create(
            TABLE_ID,
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            REFRESHED_AT);
    SchemaPolicyEngine policy = new SchemaPolicyEngine();

    assertThatThrownBy(
            () ->
                policy.reconcileStrict(
                    contract,
                    TableSchema.create(
                        TABLE_ID,
                        List.of(
                            new ColumnDefinition(
                                "id", "bigint", NeutralColumnType.INTEGER, true, false),
                            new ColumnDefinition(
                                "name", "boolean", NeutralColumnType.BOOLEAN, false, true)),
                        REFRESHED_AT.plusSeconds(60)),
                    "test"))
        .isInstanceOf(SchemaDriftException.class)
        .hasMessageContaining("selected-column contract changed")
        .hasMessageContaining("full dump required");
    assertThatThrownBy(
            () ->
                policy.reconcileStrict(
                    contract,
                    TableSchema.create(
                        TABLE_ID,
                        List.of(
                            new ColumnDefinition(
                                "id", "bigint", NeutralColumnType.INTEGER, true, false),
                            new ColumnDefinition(
                                "name", "text", NeutralColumnType.STRING, false, true)),
                        REFRESHED_AT.plusSeconds(60)),
                    "test"))
        .isInstanceOf(SchemaDriftException.class)
        .hasMessageContaining("selected-column contract changed");
    assertThatThrownBy(
            () ->
                policy.reconcileStrict(
                    contract,
                    TableSchema.create(
                        TABLE_ID,
                        List.of(
                            new ColumnDefinition(
                                "id", "bigint", NeutralColumnType.INTEGER, true, false),
                            new ColumnDefinition(
                                "name", "varchar(255)", NeutralColumnType.STRING, false, false)),
                        REFRESHED_AT.plusSeconds(60)),
                    "test"))
        .isInstanceOf(SchemaDriftException.class)
        .hasMessageContaining("selected-column contract changed");
  }
}
