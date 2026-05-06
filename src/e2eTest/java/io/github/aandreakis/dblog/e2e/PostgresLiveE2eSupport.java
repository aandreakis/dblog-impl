package io.github.aandreakis.dblog.e2e;

import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.testsupport.PostgresTestUserGrants;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.testcontainers.postgresql.PostgreSQLContainer;

final class PostgresLiveE2eSupport {
  static final String RUNTIME_USERNAME = PostgresTestUserGrants.RUNTIME_USERNAME;
  static final String RUNTIME_PASSWORD = PostgresTestUserGrants.RUNTIME_PASSWORD;

  private PostgresLiveE2eSupport() {}

  static RelationalSourceConfig sourceConfig(
      PostgreSQLContainer source,
      String sourceId,
      List<String> capturedTables,
      String publicationName,
      String slotName) {
    return new RelationalSourceConfig(
        sourceId,
        source.getJdbcUrl(),
        RUNTIME_USERNAME,
        RUNTIME_PASSWORD,
        source.getDatabaseName(),
        capturedTables,
        java.util.Map.of("postgres.publicationName", publicationName, "postgres.slotName", slotName),
        false);
  }

  static Connection openAdminConnection(PostgreSQLContainer source) throws SQLException {
    return DriverManager.getConnection(
        source.getJdbcUrl(), source.getUsername(), source.getPassword());
  }

  static Connection openRuntimeConnection(PostgreSQLContainer source) throws SQLException {
    return DriverManager.getConnection(source.getJdbcUrl(), RUNTIME_USERNAME, RUNTIME_PASSWORD);
  }

  static void createRuntimeRole(PostgreSQLContainer source, Statement statement)
      throws SQLException {
    PostgresTestUserGrants.applyDblogRuntimeRoleIdempotent(statement, source.getDatabaseName());
  }

  static void transferPublicTableOwnership(Statement statement, String... tableNames)
      throws SQLException {
    for (String tableName : tableNames) {
      statement.execute("ALTER TABLE public." + tableName + " OWNER TO " + RUNTIME_USERNAME);
    }
  }
}
