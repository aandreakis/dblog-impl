package io.github.aandreakis.dblog.e2e;

import io.github.aandreakis.dblog.testsupport.LiveMySqlTestContainers;
import io.github.aandreakis.dblog.testsupport.LivePostgresTestContainers;
import io.github.aandreakis.dblog.testsupport.MySqlTestUserGrants;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

final class SharedLiveE2eContainers {
  private static final Object LOCK = new Object();

  private static SharedMySQLContainer mysql;
  private static SharedPostgreSQLContainer postgres;
  private static boolean shutdownHookRegistered;

  private SharedLiveE2eContainers() {}

  static void reset() throws Exception {
    synchronized (LOCK) {
      startLocked();
      resetMySql();
      resetPostgres();
    }
  }

  static MySQLContainer mysql() {
    synchronized (LOCK) {
      startLocked();
      return mysql;
    }
  }

  static PostgreSQLContainer postgres() {
    synchronized (LOCK) {
      startLocked();
      return postgres;
    }
  }

  private static void startLocked() {
    if (mysql != null && postgres != null) {
      return;
    }
    mysql = new SharedMySQLContainer(DockerImageName.parse("mysql:8.4"));
    postgres = new SharedPostgreSQLContainer(DockerImageName.parse("postgres:18"));
    Startables.deepStart(mysql, postgres).join();
    try {
      MySqlTestUserGrants.applyDblogUserGrants(mysql, "appdb");
    } catch (Exception failure) {
      throw new IllegalStateException("Failed to configure shared MySQL e2e user", failure);
    }
    registerShutdownHook();
  }

  private static void resetMySql() throws SQLException {
    try (Connection connection =
            DriverManager.getConnection(rootJdbcUrl(mysql), "root", mysql.getPassword());
        Statement statement = connection.createStatement()) {
      statement.execute("DROP DATABASE IF EXISTS appdb");
      statement.execute("CREATE DATABASE appdb");
      statement.execute("DROP DATABASE IF EXISTS dblog_meta");
      statement.execute("CREATE DATABASE dblog_meta");
    }
  }

  private static void resetPostgres() throws SQLException {
    try (Connection connection =
            DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        Statement statement = connection.createStatement()) {
      dropReplicationSlots(connection);
      for (String publicationName : scalarStrings(statement, "SELECT pubname FROM pg_publication")) {
        statement.execute("DROP PUBLICATION IF EXISTS " + quotePostgresIdentifier(publicationName));
      }
      statement.execute("DROP SCHEMA IF EXISTS dblog_meta CASCADE");
      statement.execute("DROP SCHEMA IF EXISTS appdb CASCADE");
      statement.execute("DROP SCHEMA IF EXISTS public CASCADE");
      statement.execute("CREATE SCHEMA public");
      statement.execute("GRANT USAGE, CREATE ON SCHEMA public TO postgres");
      if (runtimeRoleExists(statement)) {
        statement.execute("GRANT USAGE, CREATE ON SCHEMA public TO " + PostgresLiveE2eSupport.RUNTIME_USERNAME);
      }
    }
  }

  private static void dropReplicationSlots(Connection connection) throws SQLException {
    List<String> slots = new ArrayList<>();
    try (Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "SELECT slot_name FROM pg_catalog.pg_replication_slots WHERE database = current_database() ORDER BY slot_name")) {
      while (resultSet.next()) {
        slots.add(resultSet.getString(1));
      }
    }
    try (PreparedStatement statement =
        connection.prepareStatement("SELECT pg_drop_replication_slot(?)")) {
      for (String slot : slots) {
        statement.setString(1, slot);
        statement.execute();
      }
    }
  }

  private static List<String> scalarStrings(Statement statement, String query) throws SQLException {
    List<String> values = new ArrayList<>();
    try (ResultSet resultSet = statement.executeQuery(query)) {
      while (resultSet.next()) {
        values.add(resultSet.getString(1));
      }
    }
    return values;
  }

  private static boolean runtimeRoleExists(Statement statement) throws SQLException {
    try (ResultSet resultSet =
        statement.executeQuery(
            "SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = '"
                + PostgresLiveE2eSupport.RUNTIME_USERNAME
                + "'")) {
      return resultSet.next();
    }
  }

  private static void registerShutdownHook() {
    if (shutdownHookRegistered) {
      return;
    }
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(SharedLiveE2eContainers::stopAll, "shared-live-e2e-containers-stop"));
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

  private static String rootJdbcUrl(MySQLContainer mysql) {
    return "jdbc:mysql://"
        + mysql.getHost()
        + ":"
        + mysql.getMappedPort(MySQLContainer.MYSQL_PORT)
        + "/mysql";
  }

  private static String quotePostgresIdentifier(String identifier) {
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }

  private static final class SharedMySQLContainer extends MySQLContainer {
    private SharedMySQLContainer(DockerImageName imageName) {
      super(imageName);
      LiveMySqlTestContainers.configureLiveCdcContainer(this);
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
      LivePostgresTestContainers.configureLiveCdcContainer(this);
    }

    @Override
    public void close() {}

    private void stopShared() {
      super.close();
    }
  }
}
