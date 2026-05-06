package io.github.aandreakis.dblog.testsupport;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Centralised setup of the {@code dblog} runtime role on a testcontainers-managed PostgreSQL
 * source. Single source of truth shared by the unit tests, integration tests, and e2e tests.
 * Mirrors the shipped {@code ops/docker/postgres/init/01-create-users.sql} docker-compose init
 * script so local/test paths agree on the privilege set.
 *
 * <p>The granted privileges are:
 *
 * <ul>
 *   <li>{@code LOGIN} and {@code REPLICATION} — for the logical replication session
 *   <li>{@code ALL PRIVILEGES ON DATABASE <databaseName>} — for connect and DBLog runtime access
 *   <li>{@code USAGE, CREATE ON SCHEMA public} — so tests can set up captured tables under the
 *       runtime role
 * </ul>
 *
 * <p>Table ownership and {@code dblog_meta} schema creation remain caller-specific and are left
 * to the individual tests; this helper covers only the role and its database/schema grants.
 */
public final class PostgresTestUserGrants {

  public static final String RUNTIME_USERNAME = "dblog";
  public static final String RUNTIME_PASSWORD = "dblog";

  private static final Pattern VALID_DATABASE_NAME = Pattern.compile("^[A-Za-z0-9_]+$");

  private PostgresTestUserGrants() {}

  /**
   * Creates the {@code dblog} runtime role and applies the shipped DBLog test-user grants on
   * {@code databaseName}. Intended to run on a fresh testcontainers-managed PostgreSQL instance;
   * the role creation is not idempotent by design (PostgreSQL has no {@code CREATE ROLE IF NOT
   * EXISTS}).
   *
   * @param statement open admin-privileged statement (e.g. from the container's superuser
   *     connection)
   * @param databaseName database name to grant on; must match {@code [A-Za-z0-9_]+} to prevent
   *     SQL injection via the GRANT statement
   * @throws SQLException if any of the DDL statements fail
   * @throws IllegalArgumentException if {@code databaseName} is blank or contains unsupported
   *     characters
   */
  public static void applyDblogRuntimeRole(Statement statement, String databaseName)
      throws SQLException {
    Objects.requireNonNull(statement, "statement");
    validateDatabaseName(databaseName);
    statement.execute(
        "CREATE ROLE "
            + RUNTIME_USERNAME
            + " WITH LOGIN PASSWORD '"
            + RUNTIME_PASSWORD
            + "' REPLICATION");
    applyDblogRuntimeRoleGrants(statement, databaseName);
  }

  /**
   * Creates or updates the {@code dblog} runtime role and applies the shipped DBLog test-user
   * grants on {@code databaseName}. Intended for shared-container tests that reset schema state
   * between methods but keep the PostgreSQL role catalog alive.
   *
   * @param statement open admin-privileged statement (e.g. from the container's superuser
   *     connection)
   * @param databaseName database name to grant on; must match {@code [A-Za-z0-9_]+} to prevent
   *     SQL injection via the GRANT statement
   * @throws SQLException if any of the DDL statements fail
   * @throws IllegalArgumentException if {@code databaseName} is blank or contains unsupported
   *     characters
   */
  public static void applyDblogRuntimeRoleIdempotent(Statement statement, String databaseName)
      throws SQLException {
    Objects.requireNonNull(statement, "statement");
    validateDatabaseName(databaseName);
    statement.execute(
        "DO $$ BEGIN "
            + "IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = '"
            + RUNTIME_USERNAME
            + "') THEN "
            + "CREATE ROLE "
            + RUNTIME_USERNAME
            + " WITH LOGIN PASSWORD '"
            + RUNTIME_PASSWORD
            + "' REPLICATION; "
            + "ELSE "
            + "ALTER ROLE "
            + RUNTIME_USERNAME
            + " WITH LOGIN PASSWORD '"
            + RUNTIME_PASSWORD
            + "' REPLICATION; "
            + "END IF; END $$");
    applyDblogRuntimeRoleGrants(statement, databaseName);
  }

  private static void validateDatabaseName(String databaseName) {
    if (databaseName == null || databaseName.isBlank()) {
      throw new IllegalArgumentException("databaseName must not be blank");
    }
    if (!VALID_DATABASE_NAME.matcher(databaseName).matches()) {
      throw new IllegalArgumentException(
          "databaseName must match [A-Za-z0-9_]+ but was: " + databaseName);
    }
  }

  private static void applyDblogRuntimeRoleGrants(Statement statement, String databaseName)
      throws SQLException {
    statement.execute(
        "GRANT ALL PRIVILEGES ON DATABASE " + databaseName + " TO " + RUNTIME_USERNAME);
    statement.execute("GRANT USAGE, CREATE ON SCHEMA public TO " + RUNTIME_USERNAME);
  }
}
