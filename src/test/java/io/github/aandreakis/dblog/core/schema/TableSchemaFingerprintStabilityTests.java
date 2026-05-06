package io.github.aandreakis.dblog.core.schema;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.core.model.TableId;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Locks the {@link TableSchema#fingerprint()} stability invariants from AGENTS.md §5:
 * fingerprints are computed over the selected non-ignored column surface, not every live column
 * on the table. Without these tests, a future refactor that includes ignored columns in the
 * digest would silently re-fingerprint every table carrying unsupported columns and invalidate
 * progress across runs.
 */
class TableSchemaFingerprintStabilityTests {
  private static final TableId TABLE = new TableId("source", "appdb", "widgets");
  private static final Instant REFRESHED = Instant.parse("2026-04-16T00:00:00Z");

  @Test
  void addingAnIgnoredUnsupportedColumnDoesNotChangeFingerprint() {
    TableSchema base =
        TableSchema.create(
            TABLE,
            List.of(
                pk("id", NeutralColumnType.INTEGER),
                column("name", NeutralColumnType.STRING)),
            REFRESHED);

    TableSchema withIgnored =
        TableSchema.create(
            TABLE,
            List.of(
                pk("id", NeutralColumnType.INTEGER),
                column("name", NeutralColumnType.STRING),
                column("body", NeutralColumnType.UNSUPPORTED)),
            REFRESHED);

    assertThat(withIgnored.ignoredColumns()).contains("body");
    assertThat(withIgnored.fingerprint()).isEqualTo(base.fingerprint());
  }

  @Test
  void addingASelectedColumnChangesFingerprint() {
    TableSchema base =
        TableSchema.create(
            TABLE,
            List.of(
                pk("id", NeutralColumnType.INTEGER),
                column("name", NeutralColumnType.STRING)),
            REFRESHED);

    TableSchema withExtra =
        TableSchema.create(
            TABLE,
            List.of(
                pk("id", NeutralColumnType.INTEGER),
                column("name", NeutralColumnType.STRING),
                column("status", NeutralColumnType.STRING)),
            REFRESHED);

    assertThat(withExtra.fingerprint()).isNotEqualTo(base.fingerprint());
  }

  @Test
  void changingSelectedColumnNeutralTypeChangesFingerprint() {
    TableSchema integerName =
        TableSchema.create(
            TABLE,
            List.of(
                pk("id", NeutralColumnType.INTEGER),
                column("name", NeutralColumnType.STRING)),
            REFRESHED);

    TableSchema boolName =
        TableSchema.create(
            TABLE,
            List.of(
                pk("id", NeutralColumnType.INTEGER),
                column("name", NeutralColumnType.BOOLEAN)),
            REFRESHED);

    assertThat(boolName.fingerprint()).isNotEqualTo(integerName.fingerprint());
  }

  @Test
  void renamingSelectedColumnChangesFingerprint() {
    TableSchema original =
        TableSchema.create(
            TABLE,
            List.of(
                pk("id", NeutralColumnType.INTEGER),
                column("name", NeutralColumnType.STRING)),
            REFRESHED);

    TableSchema renamed =
        TableSchema.create(
            TABLE,
            List.of(
                pk("id", NeutralColumnType.INTEGER),
                column("customer_name", NeutralColumnType.STRING)),
            REFRESHED);

    assertThat(renamed.fingerprint()).isNotEqualTo(original.fingerprint());
  }

  @Test
  void reorderingSelectedColumnsChangesFingerprint() {
    // The digest walks columns in list order. A reorder is a schema change — different
    // declaration order produces different payload shape at emit time, so the fingerprint
    // must change.
    TableSchema asc =
        TableSchema.create(
            TABLE,
            List.of(
                pk("id", NeutralColumnType.INTEGER),
                column("name", NeutralColumnType.STRING),
                column("status", NeutralColumnType.STRING)),
            REFRESHED);

    TableSchema reordered =
        TableSchema.create(
            TABLE,
            List.of(
                pk("id", NeutralColumnType.INTEGER),
                column("status", NeutralColumnType.STRING),
                column("name", NeutralColumnType.STRING)),
            REFRESHED);

    assertThat(reordered.fingerprint()).isNotEqualTo(asc.fingerprint());
  }

  @Test
  void changingPrimaryKeyShapeChangesBothFingerprintAndPrimaryKeyFingerprint() {
    TableSchema singlePk =
        TableSchema.create(
            TABLE,
            List.of(
                pk("id", NeutralColumnType.INTEGER),
                column("name", NeutralColumnType.STRING)),
            REFRESHED);

    // Composite PK: "id" + "tenant_id"
    ColumnDefinition composite1 = new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, 1, false);
    ColumnDefinition composite2 =
        new ColumnDefinition("tenant_id", "bigint", NeutralColumnType.INTEGER, true, 2, false);
    TableSchema compositePk =
        TableSchema.create(
            TABLE,
            List.of(composite1, composite2, column("name", NeutralColumnType.STRING)),
            REFRESHED);

    assertThat(compositePk.fingerprint()).isNotEqualTo(singlePk.fingerprint());
    assertThat(compositePk.primaryKeyFingerprint())
        .isNotEqualTo(singlePk.primaryKeyFingerprint());
  }

  @Test
  void sameLogicalSchemaBuiltTwiceProducesIdenticalFingerprint() {
    // Determinism: no hidden salt, no time-dependent contribution.
    TableSchema first =
        TableSchema.create(
            TABLE,
            List.of(
                pk("id", NeutralColumnType.INTEGER),
                column("name", NeutralColumnType.STRING)),
            REFRESHED);

    TableSchema second =
        TableSchema.create(
            TABLE,
            List.of(
                pk("id", NeutralColumnType.INTEGER),
                column("name", NeutralColumnType.STRING)),
            REFRESHED);

    assertThat(first.fingerprint()).isEqualTo(second.fingerprint());
    assertThat(first.primaryKeyFingerprint()).isEqualTo(second.primaryKeyFingerprint());
  }

  @Test
  void growingIgnoredColumnsListLeavesFingerprintStable() {
    // Two schemas where the only difference is additional ignored columns: fingerprint must be
    // stable. This is the specific property that keeps progress valid when the underlying table
    // accumulates unsupported columns over time.
    List<ColumnDefinition> baseColumns =
        List.of(
            pk("id", NeutralColumnType.INTEGER), column("name", NeutralColumnType.STRING));
    TableSchema base = TableSchema.create(TABLE, baseColumns, REFRESHED);

    ArrayList<ColumnDefinition> withMany = new ArrayList<>(baseColumns);
    withMany.add(column("body", NeutralColumnType.UNSUPPORTED));
    withMany.add(column("metadata", NeutralColumnType.UNSUPPORTED));
    withMany.add(column("audit_json", NeutralColumnType.UNSUPPORTED));
    TableSchema augmented = TableSchema.create(TABLE, withMany, REFRESHED);

    assertThat(augmented.ignoredColumns())
        .containsExactlyInAnyOrder("body", "metadata", "audit_json");
    assertThat(augmented.fingerprint()).isEqualTo(base.fingerprint());
  }

  private static ColumnDefinition pk(String name, NeutralColumnType type) {
    return new ColumnDefinition(name, type.name().toLowerCase(), type, true, false);
  }

  private static ColumnDefinition column(String name, NeutralColumnType type) {
    return new ColumnDefinition(name, type.name().toLowerCase(), type, false, true);
  }
}
