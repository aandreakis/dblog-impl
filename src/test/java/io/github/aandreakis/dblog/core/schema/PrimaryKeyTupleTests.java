package io.github.aandreakis.dblog.core.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PrimaryKeyTupleTests {
  @Test
  void equalityAndOrderingMatchForEquivalentSingleColumnTuples() {
    ColumnDefinition idColumn =
        new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false);

    PrimaryKeyTuple fromLiteral = PrimaryKeyTuple.fromLiteral(List.of(idColumn), "42");
    PrimaryKeyTuple fromRow = PrimaryKeyTuple.fromRow(List.of(idColumn), Map.of("id", 42L));

    assertThat(fromLiteral).isEqualTo(fromRow);
    assertThat(fromLiteral.hashCode()).isEqualTo(fromRow.hashCode());
    assertThat(fromLiteral.compareTo(fromRow)).isZero();
  }

  @Test
  void compositeTuplesCanonicalizeNamedAndPositionalForms() {
    ColumnDefinition accountId =
        new ColumnDefinition("account_id", "bigint", NeutralColumnType.INTEGER, true, 1, false);
    ColumnDefinition region =
        new ColumnDefinition("region", "varchar(32)", NeutralColumnType.STRING, true, 2, false);

    Map<String, Object> row = new LinkedHashMap<>();
    row.put("account_id", 7L);
    row.put("region", "apac");

    PrimaryKeyTuple fromLiteral =
        PrimaryKeyTuple.fromLiteral(List.of(accountId, region), "{account_id=7,region=apac}");
    PrimaryKeyTuple fromPositional =
        PrimaryKeyTuple.fromLiteral(List.of(accountId, region), "(7,apac)");
    PrimaryKeyTuple fromRow = PrimaryKeyTuple.fromRow(List.of(accountId, region), row);

    assertThat(fromLiteral).isEqualTo(fromRow);
    assertThat(fromPositional).isEqualTo(fromRow);
    assertThat(fromLiteral.compareTo(fromRow)).isZero();
  }

  @Test
  void canonicalCompositeLiteralRoundTripsBackslashes() {
    ColumnDefinition tenant =
        new ColumnDefinition("tenant", "varchar(64)", NeutralColumnType.STRING, true, 1, false);
    ColumnDefinition externalId =
        new ColumnDefinition(
            "external_id", "varchar(128)", NeutralColumnType.STRING, true, 2, false);
    List<ColumnDefinition> columns = List.of(tenant, externalId);
    PrimaryKeyTuple original =
        PrimaryKeyTuple.fromRow(
            columns, Map.of("tenant", "tenant-a", "external_id", "folder\\record"));

    PrimaryKeyTuple decoded = PrimaryKeyTuple.fromLiteral(columns, original.literal());

    assertThat(decoded).isEqualTo(original);
  }

  @Test
  void canonicalCompositeLiteralRoundTripsATrailingBackslash() {
    ColumnDefinition tenant =
        new ColumnDefinition("tenant", "varchar(64)", NeutralColumnType.STRING, true, 1, false);
    ColumnDefinition externalId =
        new ColumnDefinition(
            "external_id", "varchar(128)", NeutralColumnType.STRING, true, 2, false);
    List<ColumnDefinition> columns = List.of(tenant, externalId);
    PrimaryKeyTuple original =
        PrimaryKeyTuple.fromRow(
            columns, Map.of("tenant", "tenant-a", "external_id", "a\\"));

    PrimaryKeyTuple decoded = PrimaryKeyTuple.fromLiteral(columns, original.literal());

    assertThat(decoded).isEqualTo(original);
  }

  @Test
  void canonicalCompositeLiteralRoundTripsABackslashBeforeADelimiter() {
    ColumnDefinition tenant =
        new ColumnDefinition("tenant", "varchar(64)", NeutralColumnType.STRING, true, 1, false);
    ColumnDefinition externalId =
        new ColumnDefinition(
            "external_id", "varchar(128)", NeutralColumnType.STRING, true, 2, false);
    List<ColumnDefinition> columns = List.of(tenant, externalId);
    PrimaryKeyTuple original =
        PrimaryKeyTuple.fromRow(
            columns, Map.of("tenant", "tenant-a", "external_id", "x\\,y"));

    PrimaryKeyTuple decoded = PrimaryKeyTuple.fromLiteral(columns, original.literal());

    assertThat(decoded).isEqualTo(original);
  }
}
