package io.github.aandreakis.dblog.integration.adapter.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.testsupport.LiveMySqlTestContainers;
import io.github.aandreakis.dblog.testsupport.MySqlTestUserGrants;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

final class MySqlSourceAdapterIsolatedLiveRuntimeTestSupport {
  private MySqlSourceAdapterIsolatedLiveRuntimeTestSupport() {}

  static MySQLContainer defaultMysqlContainer() {
    return LiveMySqlTestContainers.newDefaultContainer();
  }

  static MySQLContainer mysqlContainer(String... command) {
    return LiveMySqlTestContainers.configureBaseContainer(
            new MySQLContainer(DockerImageName.parse(LiveMySqlTestContainers.DEFAULT_IMAGE)))
        .withCommand(command);
  }

  static void configureReplicationUser(MySQLContainer mysql) throws Exception {
    MySqlTestUserGrants.applyDblogUserGrants(mysql, "appdb");
  }

  static TableSchema widgetSchema() {
    return TableSchema.create(
        new TableId("sourceA", "appdb", "widgets"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
        Instant.parse("2026-04-11T00:00:00Z"));
  }

  static RelationalSourceConfig sourceConfig(MySQLContainer mysql) {
    return new RelationalSourceConfig(
        "sourceA",
        mysql.getJdbcUrl(),
        mysql.getUsername(),
        mysql.getPassword(),
        "appdb",
        List.of("appdb.widgets"),
        java.util.Map.of("mysql.serverId", "223355"),
        false);
  }

  static void insertWidget(Connection connection, long id, String name) throws Exception {
    try (Statement statement = connection.createStatement()) {
      statement.execute(
          "INSERT INTO appdb.widgets (id, name) VALUES (" + id + ", '" + name + "')");
    }
  }

  static void purgeCheckpointHistory(
      MySQLContainer mysql, Connection connection, String checkpointFile) throws Exception {
    List<String> logFiles = binaryLogFiles(connection);
    assertThat(logFiles).contains(checkpointFile);

    String activeFile = currentBinaryLogFile(connection);
    int rotations = 0;
    while (checkpointFile.equals(activeFile) && rotations < 8) {
      flushBinaryLogs(mysql);
      activeFile = currentBinaryLogFile(connection);
      rotations++;
    }

    assertThat(activeFile)
        .withFailMessage("Expected MySQL to rotate off checkpoint file %s", checkpointFile)
        .isNotEqualTo(checkpointFile);

    var purgeResult =
        mysql.execInContainer(
            "sh",
            "-c",
            "mysql -uroot -p"
                + mysql.getPassword()
                + " -e \"PURGE BINARY LOGS TO '"
                + activeFile
                + "'\"");
    assertThat(purgeResult.getExitCode())
        .withFailMessage(
            "Failed to purge MySQL binary logs before %s.%nstdout:%n%s%nstderr:%n%s",
            activeFile,
            purgeResult.getStdout(),
            purgeResult.getStderr())
        .isZero();

    assertThat(binaryLogFiles(connection)).doesNotContain(checkpointFile);
  }

  private static void flushBinaryLogs(MySQLContainer mysql) throws Exception {
    var flushResult =
        mysql.execInContainer(
            "sh", "-c", "mysql -uroot -p" + mysql.getPassword() + " -e \"FLUSH BINARY LOGS\"");
    assertThat(flushResult.getExitCode())
        .withFailMessage(
            "Failed to rotate MySQL binary logs.%nstdout:%n%s%nstderr:%n%s",
            flushResult.getStdout(),
            flushResult.getStderr())
        .isZero();
  }

  private static List<String> binaryLogFiles(Connection connection) throws Exception {
    List<String> files = new ArrayList<>();
    try (Statement statement = connection.createStatement();
        var resultSet = statement.executeQuery("SHOW BINARY LOGS")) {
      while (resultSet.next()) {
        files.add(resultSet.getString(1));
      }
    }
    return List.copyOf(files);
  }

  private static String currentBinaryLogFile(Connection connection) throws Exception {
    try (Statement statement = connection.createStatement()) {
      try (var resultSet = statement.executeQuery("SHOW BINARY LOG STATUS")) {
        if (resultSet.next()) {
          return resultSet.getString(1);
        }
      } catch (java.sql.SQLException ignored) {
        // Fall through to older syntax below.
      }
      try (var resultSet = statement.executeQuery("SHOW MASTER STATUS")) {
        assertThat(resultSet.next()).isTrue();
        return resultSet.getString(1);
      }
    }
  }
}
