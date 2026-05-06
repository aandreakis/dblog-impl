package io.github.aandreakis.dblog.sink.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.core.model.TableId;
import java.util.List;
import org.junit.jupiter.api.Test;

class JdbcApplyTargetDialectTests {
  @Test
  void postgresBuildsOnConflictUpsertSqlForNonPrimaryKeyColumns() {
    String sql =
        JdbcApplyTargetDialect.POSTGRES.upsertSql(
            new TableId("sourceA", "public", "orders"),
            List.of("id", "status"),
            List.of("id"),
            List.of("?", "?"));

    assertThat(sql)
        .isEqualTo(
            "INSERT INTO \"public\".\"orders\" (\"id\", \"status\") VALUES (?, ?) "
                + "ON CONFLICT (\"id\") DO UPDATE SET \"status\" = EXCLUDED.\"status\"");
  }

  @Test
  void mysqlBuildsAliasedOnDuplicateKeyUpsertSqlForNonPrimaryKeyColumns() {
    String sql =
        JdbcApplyTargetDialect.MYSQL.upsertSql(
            new TableId("sourceA", "appdb", "orders"),
            List.of("id", "status"),
            List.of("id"),
            List.of("?", "?"));

    assertThat(sql)
        .isEqualTo(
            "INSERT INTO `appdb`.`orders` (`id`, `status`) VALUES (?, ?) AS new_row "
                + "ON DUPLICATE KEY UPDATE `status` = new_row.`status`");
  }
}
