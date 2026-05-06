package io.github.aandreakis.dblog.core.schema;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.core.model.TableId;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class PrimaryKeyCompatibilityTests {
  @Test
  void allowsDecodeUpdatesWhenPrimaryKeyFamiliesStayTheSame() {
    TableSchema current =
        schema(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true));
    TableSchema candidate =
        schema(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true),
            new ColumnDefinition("notes", "text", NeutralColumnType.STRING, false, true));

    assertThat(PrimaryKeyCompatibility.isSafeDecodeUpdate(current, candidate)).isTrue();
  }

  @Test
  void rejectsDecodeUpdatesWhenPrimaryKeyFamiliesChange() {
    TableSchema current =
        schema(new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false));
    TableSchema candidate =
        schema(new ColumnDefinition("id", "uuid", NeutralColumnType.UUID, true, false));

    assertThat(PrimaryKeyCompatibility.isSafeDecodeUpdate(current, candidate)).isFalse();
  }

  @Test
  void rejectsDecodeUpdatesWhenPrimaryKeySourceTypeChangesEvenWithinSameNeutralFamily() {
    TableSchema current =
        schema(new ColumnDefinition("id", "int", NeutralColumnType.INTEGER, true, false));
    TableSchema candidate =
        schema(new ColumnDefinition("id", "bigint unsigned", NeutralColumnType.INTEGER, true, false));

    assertThat(PrimaryKeyCompatibility.isSafeDecodeUpdate(current, candidate)).isFalse();
  }

  private static TableSchema schema(ColumnDefinition... columns) {
    return TableSchema.create(
        new TableId("sourceA", "appdb", "widgets"),
        List.of(columns),
        Instant.parse("2026-04-12T00:00:00Z"));
  }
}
