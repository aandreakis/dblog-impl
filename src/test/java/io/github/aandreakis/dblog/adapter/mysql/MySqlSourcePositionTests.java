package io.github.aandreakis.dblog.adapter.mysql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class MySqlSourcePositionTests {
  @Test
  void parsesFormatsAndOrdersPositionsDeterministically() {
    MySqlSourcePosition plain = MySqlSourcePosition.parse("mysql-bin.000001:42");
    MySqlSourcePosition withGtid = MySqlSourcePosition.parse("mysql-bin.000001:43;gtid=uuid:1-2");

    assertThat(plain.displayValue()).isEqualTo("mysql-bin.000001:42");
    assertThat(withGtid.displayValue()).isEqualTo("mysql-bin.000001:43;gtid=uuid:1-2");
    assertThat(plain.compareTo(withGtid)).isLessThan(0);
  }

  @Test
  void rejectsMalformedPositionStrings() {
    assertThatThrownBy(() -> MySqlSourcePosition.parse("mysql-bin.000001"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must use the form");
    assertThatThrownBy(() -> MySqlSourcePosition.parse("mysql-bin.000001:42;gtid="))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("gtidSet");
  }
}
