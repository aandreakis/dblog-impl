package io.github.aandreakis.dblog.adapter.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class PostgresLsnTests {
  @Test
  void parsesFormatsAndOrdersLsnsDeterministically() {
    PostgresLsn first = PostgresLsn.parse("0/2A");
    PostgresLsn second = PostgresLsn.parse("0/2B");

    assertThat(first.displayValue()).isEqualTo("0/2A");
    assertThat(first.compareTo(second)).isLessThan(0);
    assertThat(PostgresLsn.fromLong(first.asLong())).isEqualTo(first);
  }

  @Test
  void rejectsMalformedLsnStrings() {
    assertThatThrownBy(() -> PostgresLsn.parse("0"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("HEX/HEX");
    assertThatThrownBy(() -> PostgresLsn.parse("0/XYZ"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("non-hex");
  }
}
