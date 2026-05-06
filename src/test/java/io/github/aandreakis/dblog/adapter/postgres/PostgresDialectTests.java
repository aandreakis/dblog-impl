package io.github.aandreakis.dblog.adapter.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.api.TableNamingPolicy;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import org.junit.jupiter.api.Test;

class PostgresDialectTests {
  @Test
  void mapsRepresentativePostgresTypesToNeutralColumnTypes() {
    assertThat(PostgresDialect.INSTANCE.neutralType("bool", "b"))
        .isEqualTo(NeutralColumnType.BOOLEAN);
    assertThat(PostgresDialect.INSTANCE.neutralType("int8", "b"))
        .isEqualTo(NeutralColumnType.INTEGER);
    assertThat(PostgresDialect.INSTANCE.neutralType("text", "b"))
        .isEqualTo(NeutralColumnType.STRING);
    assertThat(PostgresDialect.INSTANCE.neutralType("widget_status", "e"))
        .isEqualTo(NeutralColumnType.ENUM_STRING);
    assertThat(PostgresDialect.INSTANCE.neutralType("jsonb", "b"))
        .isEqualTo(NeutralColumnType.JSON);
  }

  @Test
  void publishesTwoPartSchemaTableNamingPolicy() {
    assertThat(PostgresDialect.INSTANCE.tableNamingPolicy())
        .isEqualTo(TableNamingPolicy.TWO_PART_SCHEMA_TABLE);
    assertThat(PostgresDialect.INSTANCE.requiredTableNameParts()).isEqualTo(2);
  }
}
