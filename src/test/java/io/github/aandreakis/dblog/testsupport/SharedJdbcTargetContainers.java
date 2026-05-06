package io.github.aandreakis.dblog.testsupport;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

public final class SharedJdbcTargetContainers {
  private static final Object LOCK = new Object();

  private static SharedMySQLContainer mysql;
  private static SharedPostgreSQLContainer postgres;
  private static boolean shutdownHookRegistered;

  private SharedJdbcTargetContainers() {}

  public static void start() {
    synchronized (LOCK) {
      if (mysql != null && postgres != null) {
        return;
      }
      mysql = new SharedMySQLContainer(DockerImageName.parse("mysql:8.4"));
      postgres = new SharedPostgreSQLContainer(DockerImageName.parse("postgres:18"));
      Startables.deepStart(mysql, postgres).join();
      registerShutdownHook();
    }
  }

  public static MySQLContainer mysql() {
    start();
    return mysql;
  }

  public static PostgreSQLContainer postgres() {
    start();
    return postgres;
  }

  public static void resetMySql() throws SQLException {
    start();
    try (Connection connection =
            DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
        Statement statement = connection.createStatement()) {
      List<String> tableNames = new ArrayList<>();
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE()")) {
        while (resultSet.next()) {
          tableNames.add(resultSet.getString(1));
        }
      }

      statement.execute("SET FOREIGN_KEY_CHECKS = 0");
      try {
        for (String tableName : tableNames) {
          statement.execute("DROP TABLE IF EXISTS " + quoteMySqlIdentifier(tableName));
        }
      } finally {
        statement.execute("SET FOREIGN_KEY_CHECKS = 1");
      }
    }
  }

  public static void resetPostgres() throws SQLException {
    start();
    try (Connection connection =
            DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        Statement statement = connection.createStatement()) {
      statement.execute("DROP SCHEMA IF EXISTS public CASCADE");
      statement.execute("CREATE SCHEMA public");
      statement.execute("GRANT ALL ON SCHEMA public TO postgres");
    }
  }

  private static void registerShutdownHook() {
    if (shutdownHookRegistered) {
      return;
    }
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(SharedJdbcTargetContainers::stopAll, "shared-jdbc-target-containers-stop"));
    shutdownHookRegistered = true;
  }

  private static void stopAll() {
    synchronized (LOCK) {
      if (postgres != null) {
        postgres.stopShared();
        postgres = null;
      }
      if (mysql != null) {
        mysql.stopShared();
        mysql = null;
      }
    }
  }

  private static String quoteMySqlIdentifier(String identifier) {
    return "`" + identifier.replace("`", "``") + "`";
  }

  private static final class SharedMySQLContainer extends MySQLContainer {
    private SharedMySQLContainer(DockerImageName imageName) {
      super(imageName);
      withDatabaseName("appdb")
          .withUsername("dblog")
          .withPassword("dblog")
          .withEnv("MYSQL_ROOT_PASSWORD", "root");
    }

    @Override
    public void close() {}

    private void stopShared() {
      super.close();
    }
  }

  private static final class SharedPostgreSQLContainer extends PostgreSQLContainer {
    private SharedPostgreSQLContainer(DockerImageName imageName) {
      super(imageName);
      withDatabaseName("appdb").withUsername("postgres").withPassword("postgres");
    }

    @Override
    public void close() {}

    private void stopShared() {
      super.close();
    }
  }
}
