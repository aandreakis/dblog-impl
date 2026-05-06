package io.github.aandreakis.dblog.boot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.adapter.mysql.MySqlSourceAdapter;
import io.github.aandreakis.dblog.adapter.postgres.PostgresSourceAdapter;
import org.junit.jupiter.api.Test;

class SourceAdapterRegistryTests {
  @Test
  void resolvesCanonicalAndAliasedAdapterKeys() {
    SourceAdapterRegistry registry =
        new SourceAdapterRegistry(
            java.util.List.of(new MySqlSourceAdapter(), new PostgresSourceAdapter()));

    assertThat(registry.require("mysql").key()).isEqualTo("mysql");
    assertThat(registry.require("postgres").key()).isEqualTo("postgres");
    assertThat(registry.require("postgresql").key()).isEqualTo("postgres");
  }

  @Test
  void rejectsUnknownAdapters() {
    SourceAdapterRegistry registry =
        new SourceAdapterRegistry(
            java.util.List.of(new MySqlSourceAdapter(), new PostgresSourceAdapter()));

    assertThatThrownBy(() -> registry.require("oracle"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported source adapter");
  }
}
