package io.github.aandreakis.dblog.adapter.mysql.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.mysql.MySqlSourcePosition;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class MySqlBinlogMessageTests {
  @Test
  void trustedWriteRowsReusesProvidedRowArrays() {
    Object[] row = new Object[] {1L, "before"};
    List<Object[]> rows = new ArrayList<>();
    rows.add(row);

    MySqlBinlogMessage.WriteRows message =
        MySqlBinlogMessage.trustedWriteRows(
            7L, rows, MySqlSourcePosition.parse("mysql-bin.000001:42"), Instant.EPOCH);

    row[1] = "after";

    assertThat(message.rows()).hasSize(1);
    assertThat(message.rows().getFirst()[1]).isEqualTo("after");
  }

  @Test
  void defensiveRowChangeConstructorStillClonesArrays() {
    Object[] before = new Object[] {1L, "before"};
    Object[] after = new Object[] {1L, "after"};

    MySqlBinlogMessage.RowChange rowChange = new MySqlBinlogMessage.RowChange(before, after);
    before[1] = "mutated-before";
    after[1] = "mutated-after";

    assertThat(rowChange.beforeValues()[1]).isEqualTo("before");
    assertThat(rowChange.afterValues()[1]).isEqualTo("after");
  }
}
