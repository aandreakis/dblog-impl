package io.github.aandreakis.dblog.adapter.api;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RelationalSourceConfigValidatorTests {
  @Test
  void twoPartPolicyAcceptsSchemaTable() {
    RelationalSourceConfig config = config(List.of("appdb.widgets", "appdb.gadgets"));
    assertThatNoException()
        .isThrownBy(
            () -> RelationalSourceConfigValidator.requireTablePartCount(config, 2, "TestDialect"));
  }

  @Test
  void twoPartPolicyRejectsThreePartTable() {
    RelationalSourceConfig config = config(List.of("catalog.schema.table"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> RelationalSourceConfigValidator.requireTablePartCount(config, 2, "TestDialect"))
        .withMessageContaining("TestDialect")
        .withMessageContaining("two-part")
        .withMessageContaining("schema.table");
  }

  @Test
  void threePartPolicyAcceptsCatalogSchemaTable() {
    RelationalSourceConfig config = config(List.of("main.dbo.widgets", "main.dbo.gadgets"));
    assertThatNoException()
        .isThrownBy(
            () -> RelationalSourceConfigValidator.requireTablePartCount(config, 3, "TestDialect"));
  }

  @Test
  void threePartPolicyRejectsTwoPartTable() {
    RelationalSourceConfig config = config(List.of("schema.table"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> RelationalSourceConfigValidator.requireTablePartCount(config, 3, "TestDialect"))
        .withMessageContaining("TestDialect")
        .withMessageContaining("three-part")
        .withMessageContaining("catalog.schema.table");
  }

  @Test
  void rejectsBlankPartRegardlessOfCount() {
    RelationalSourceConfig config = config(List.of("appdb."));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> RelationalSourceConfigValidator.requireTablePartCount(config, 2, "TestDialect"));
  }

  @Test
  void rejectsEmptyCapturedTables() {
    RelationalSourceConfig config = config(List.of());
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> RelationalSourceConfigValidator.requireTablePartCount(config, 2, "TestDialect"))
        .withMessageContaining("must not be empty");
  }

  private static RelationalSourceConfig config(List<String> capturedTables) {
    return new RelationalSourceConfig(
        "test",
        "jdbc:test://localhost/appdb",
        "user",
        "pw",
        "appdb",
        capturedTables,
        Map.of(),
        false);
  }
}
