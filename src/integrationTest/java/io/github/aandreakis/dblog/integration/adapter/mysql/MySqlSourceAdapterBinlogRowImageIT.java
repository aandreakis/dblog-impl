package io.github.aandreakis.dblog.integration.adapter.mysql;

import static io.github.aandreakis.dblog.integration.adapter.mysql.MySqlSourceAdapterIsolatedLiveRuntimeTestSupport.configureReplicationUser;
import static io.github.aandreakis.dblog.integration.adapter.mysql.MySqlSourceAdapterIsolatedLiveRuntimeTestSupport.mysqlContainer;
import static io.github.aandreakis.dblog.integration.adapter.mysql.MySqlSourceAdapterIsolatedLiveRuntimeTestSupport.sourceConfig;
import static io.github.aandreakis.dblog.integration.adapter.mysql.MySqlSourceAdapterIsolatedLiveRuntimeTestSupport.widgetSchema;
import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourceAdapter;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.mysql.MySQLContainer;

@Tag("integration-docker")
class MySqlSourceAdapterBinlogRowImageIT {
  @TempDir Path tempDir;

  @Test
  void failsClosedWhenBinlogRowImageIsNotFull() throws Exception {
    assumeDockerIsAvailable();

    try (MySQLContainer mysql =
        mysqlContainer(
            "--server-id=223344",
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            "--binlog-row-image=MINIMAL",
            "--mysql-native-password=ON")) {
      mysql.start();
      configureReplicationUser(mysql);

      TableSchema schema = widgetSchema();
      RelationalSourceConfig config = sourceConfig(mysql);
      MySqlSourceAdapter adapter = new MySqlSourceAdapter();

      try (H2RuntimeStateStore stateStore =
          new H2RuntimeStateStore(tempDir.resolve("mysql-live-adapter-preflight-row-image"))) {
        assertThatThrownBy(() -> adapter.openRuntime(config, stateStore, List.of(schema)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("FULL");
      }
    }
  }
}
