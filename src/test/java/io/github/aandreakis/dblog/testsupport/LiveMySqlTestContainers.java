package io.github.aandreakis.dblog.testsupport;

import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/** Shared Testcontainers wiring for MySQL sources that must support live CDC. */
public final class LiveMySqlTestContainers {
  public static final String DEFAULT_IMAGE = "mysql:8.4";
  public static final String DEFAULT_DATABASE_NAME = "appdb";
  public static final String DEFAULT_USERNAME = "dblog";
  public static final String DEFAULT_PASSWORD = "dblog";
  public static final String DEFAULT_ROOT_PASSWORD = "root";
  public static final String DEFAULT_SERVER_ID = "223344";

  private LiveMySqlTestContainers() {}

  public static MySQLContainer newDefaultContainer() {
    return newContainer(DockerImageName.parse(DEFAULT_IMAGE), DEFAULT_SERVER_ID);
  }

  public static MySQLContainer newContainer(DockerImageName imageName, String serverId) {
    return configureLiveCdcContainer(new MySQLContainer(imageName), serverId);
  }

  public static MySQLContainer configureBaseContainer(MySQLContainer mysql) {
    return mysql
        .withDatabaseName(DEFAULT_DATABASE_NAME)
        .withUsername(DEFAULT_USERNAME)
        .withPassword(DEFAULT_PASSWORD)
        .withEnv("MYSQL_ROOT_PASSWORD", DEFAULT_ROOT_PASSWORD);
  }

  public static MySQLContainer configureLiveCdcContainer(MySQLContainer mysql) {
    return configureLiveCdcContainer(mysql, DEFAULT_SERVER_ID);
  }

  public static MySQLContainer configureLiveCdcContainer(MySQLContainer mysql, String serverId) {
    return configureBaseContainer(mysql).withCommand(liveCdcCommand(serverId));
  }

  public static String[] liveCdcCommand(String serverId) {
    return new String[] {
      "--server-id=" + serverId,
      "--log-bin=mysql-bin",
      "--binlog-format=ROW",
      "--binlog-row-image=FULL",
      "--binlog-row-metadata=FULL",
      "--gtid-mode=ON",
      "--enforce-gtid-consistency=ON"
    };
  }
}
