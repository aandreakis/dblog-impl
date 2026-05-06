package io.github.aandreakis.dblog.integration.adapter.mysql;

import static io.github.aandreakis.dblog.integration.adapter.LiveRuntimeTestSupport.TRANSACTION_AWAIT_TIMEOUT;
import static io.github.aandreakis.dblog.integration.adapter.LiveRuntimeTestSupport.awaitTransaction;
import static io.github.aandreakis.dblog.integration.adapter.mysql.MySqlSourceAdapterIsolatedLiveRuntimeTestSupport.configureReplicationUser;
import static io.github.aandreakis.dblog.integration.adapter.mysql.MySqlSourceAdapterIsolatedLiveRuntimeTestSupport.defaultMysqlContainer;
import static io.github.aandreakis.dblog.integration.adapter.mysql.MySqlSourceAdapterIsolatedLiveRuntimeTestSupport.insertWidget;
import static io.github.aandreakis.dblog.integration.adapter.mysql.MySqlSourceAdapterIsolatedLiveRuntimeTestSupport.purgeCheckpointHistory;
import static io.github.aandreakis.dblog.integration.adapter.mysql.MySqlSourceAdapterIsolatedLiveRuntimeTestSupport.sourceConfig;
import static io.github.aandreakis.dblog.integration.adapter.mysql.MySqlSourceAdapterIsolatedLiveRuntimeTestSupport.widgetSchema;
import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.api.OpenedSourceRuntime;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.mysql.MySqlBinlogTransaction;
import io.github.aandreakis.dblog.adapter.mysql.MySqlLiveStreamingRuntime;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourceAdapter;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourceCheckpointStore;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourcePosition;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.mysql.MySQLContainer;

@Tag("integration-docker")
class MySqlSourceAdapterGtidRecoveryIT {
  @TempDir Path tempDir;

  @Test
  void resumesFromGtidStateWhenCheckpointedBinlogHistoryIsPurgedBeforeRestart()
      throws Exception {
    assumeDockerIsAvailable();

    try (MySQLContainer mysql = defaultMysqlContainer()) {
      mysql.start();
      configureReplicationUser(mysql);

      try (Connection connection =
          DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
        try (Statement statement = connection.createStatement()) {
          statement.execute("CREATE TABLE appdb.widgets (id BIGINT PRIMARY KEY, name VARCHAR(255))");
        }
      }

      TableSchema schema = widgetSchema();
      RelationalSourceConfig config = sourceConfig(mysql);
      MySqlSourceAdapter adapter = new MySqlSourceAdapter();

      try (H2RuntimeStateStore stateStore =
              new H2RuntimeStateStore(tempDir.resolve("mysql-live-adapter-gtid-purge"));
          Connection sqlConnection =
              DriverManager.getConnection(
                  mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
        MySqlSourceCheckpointStore checkpointStore = new MySqlSourceCheckpointStore(stateStore);

        MySqlSourcePosition checkpoint;
        OpenedSourceRuntime<?> firstOpen = adapter.openRuntime(config, stateStore, List.of(schema));
        try {
          try (Connection writer =
              DriverManager.getConnection(
                  mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
            insertWidget(writer, 1L, "one");
          }

          MySqlLiveStreamingRuntime runtime = (MySqlLiveStreamingRuntime) firstOpen.runtime();
          MySqlBinlogTransaction transaction = awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
          runtime.acknowledge(transaction);
          checkpoint = transaction.checkpointPosition();
        } finally {
          firstOpen.runtime().close();
        }

        assertThat(checkpointStore.load("sourceA")).contains(checkpoint);
        assertThat(checkpoint.gtidSet()).isNotBlank();

        purgeCheckpointHistory(mysql, sqlConnection, checkpoint.binlogFilename());

        try (Connection writer =
            DriverManager.getConnection(
                mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
          insertWidget(writer, 2L, "two");
        }

        OpenedSourceRuntime<?> reopened = adapter.openRuntime(config, stateStore, List.of(schema));
        try {
          assertThat(reopened.loadedCheckpointDisplayValue()).isNotBlank();

          MySqlLiveStreamingRuntime runtime = (MySqlLiveStreamingRuntime) reopened.runtime();
          MySqlBinlogTransaction resumedTransaction =
              awaitTransaction(runtime, TRANSACTION_AWAIT_TIMEOUT);
          assertThat(resumedTransaction.events()).hasSize(1);
          assertThat(resumedTransaction.events().getFirst().afterRow().asMap())
              .containsEntry("id", 2L)
              .containsEntry("name", "two");
          runtime.acknowledge(resumedTransaction);
        } finally {
          reopened.runtime().close();
        }
      }
    }
  }
}
