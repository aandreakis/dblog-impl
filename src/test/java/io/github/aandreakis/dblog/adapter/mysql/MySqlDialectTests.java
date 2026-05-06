package io.github.aandreakis.dblog.adapter.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.api.TableNamingPolicy;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import org.junit.jupiter.api.Test;

class MySqlDialectTests {
  @Test
  void mapsRepresentativeMysqlTypesToNeutralColumnTypes() {
    assertThat(MySqlDialect.INSTANCE.neutralType("tinyint", "tinyint(1)"))
        .isEqualTo(NeutralColumnType.BOOLEAN);
    assertThat(MySqlDialect.INSTANCE.neutralType("tinyint", "tinyint(2)"))
        .isEqualTo(NeutralColumnType.INTEGER);
    assertThat(MySqlDialect.INSTANCE.neutralType("set", "set('a','b')"))
        .isEqualTo(NeutralColumnType.ENUM_STRING);
    assertThat(MySqlDialect.INSTANCE.neutralType("json", "json"))
        .isEqualTo(NeutralColumnType.JSON);
    assertThat(MySqlDialect.INSTANCE.neutralType("geometry", "geometry"))
        .isEqualTo(NeutralColumnType.UNSUPPORTED);
  }

  @Test
  void canonicalizesRepresentativeMysqlSourceTypes() {
    assertThat(MySqlDialect.INSTANCE.canonicalSourceType("integer", "INTEGER unsigned zerofill"))
        .isEqualTo("int unsigned zerofill");
    assertThat(MySqlDialect.INSTANCE.canonicalSourceType("bigint", "bigint unsigned"))
        .isEqualTo("bigint unsigned");
    assertThat(MySqlDialect.INSTANCE.canonicalSourceType("varchar", ""))
        .isEqualTo("varchar");
  }

  @Test
  void publishesTwoPartDatabaseTableNamingPolicy() {
    assertThat(MySqlDialect.INSTANCE.tableNamingPolicy())
        .isEqualTo(TableNamingPolicy.TWO_PART_DATABASE_TABLE);
    assertThat(MySqlDialect.INSTANCE.requiredTableNameParts()).isEqualTo(2);
  }
}
