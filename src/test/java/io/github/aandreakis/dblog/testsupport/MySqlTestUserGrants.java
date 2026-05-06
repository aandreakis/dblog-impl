package io.github.aandreakis.dblog.testsupport;

import java.io.IOException;
import java.util.Objects;
import java.util.regex.Pattern;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.JdbcDatabaseContainer;

/**
 * Centralised setup of the {@code 'dblog'@'%'} test user on a testcontainers-managed MySQL
 * source. Single source of truth shared by the unit tests, integration tests,
 * and e2e tests. Mirrors the shipped {@code ops/docker/mysql/init/01-create-users.sql}
 * docker-compose init script so local/test paths agree on the privilege set.
 *
 * <p>The granted privileges are:
 *
 * <ul>
 *   <li>{@code ALL PRIVILEGES ON <applicationDatabaseName>.*} — for user-table CRUD + schema
 *       inspection
 *   <li>{@code ALL PRIVILEGES ON dblog_meta.*} — for DBLog's watermark/heartbeat metadata tables
 *   <li>{@code REPLICATION SLAVE, REPLICATION CLIENT} — for the binlog streaming session
 *   <li>{@code SESSION_VARIABLES_ADMIN} — so the adapter can {@code SET SESSION sql_log_bin=0}
 *       around its own metadata-table bootstrap DDL. Without this privilege the bootstrap
 *       still works (the streaming session skips its own DDL via SQL-pattern fallback), but
 *       downstream replicas would observe the leaked schema and the runtime emits three
 *       WARN lines per startup telling the operator to add this grant.
 * </ul>
 */
public final class MySqlTestUserGrants {

  private static final Pattern VALID_DATABASE_NAME = Pattern.compile("^[A-Za-z0-9_]+$");

  private MySqlTestUserGrants() {}

  /**
   * Applies the shipped DBLog test-user grants to {@code mysql}. Creates the {@code dblog_meta}
   * database if it does not yet exist. The {@code dblog} user is created with password
   * {@code dblog}; if it already exists, its password is re-asserted.
   *
   * @param mysql testcontainers MySQL container (either
   *     {@code org.testcontainers.containers.MySQLContainer} or
   *     {@code org.testcontainers.mysql.MySQLContainer}); must expose a {@code root} password
   * @param applicationDatabaseName user-table database name (e.g. {@code app} or {@code appdb});
   *     must match {@code [A-Za-z0-9_]+} to prevent SQL injection via the GRANT statement
   * @throws IOException if the in-container shell exec fails
   * @throws InterruptedException if interrupted while waiting for the shell exec
   * @throws IllegalStateException if the grant statements return a non-zero exit code
   * @throws IllegalArgumentException if {@code applicationDatabaseName} is blank or contains
   *     unsupported characters
   */
  public static void applyDblogUserGrants(
      JdbcDatabaseContainer<?> mysql, String applicationDatabaseName)
      throws IOException, InterruptedException {
    Objects.requireNonNull(mysql, "mysql");
    if (applicationDatabaseName == null || applicationDatabaseName.isBlank()) {
      throw new IllegalArgumentException("applicationDatabaseName must not be blank");
    }
    if (!VALID_DATABASE_NAME.matcher(applicationDatabaseName).matches()) {
      throw new IllegalArgumentException(
          "applicationDatabaseName must match [A-Za-z0-9_]+ but was: " + applicationDatabaseName);
    }
    Container.ExecResult result =
        mysql.execInContainer(
            "sh",
            "-c",
            "mysql -uroot -p"
                + mysql.getPassword()
                + " -e \""
                + "CREATE DATABASE IF NOT EXISTS dblog_meta; "
                + "CREATE USER IF NOT EXISTS 'dblog'@'%' IDENTIFIED BY 'dblog'; "
                + "ALTER USER 'dblog'@'%' IDENTIFIED BY 'dblog'; "
                + "GRANT ALL PRIVILEGES ON "
                + applicationDatabaseName
                + ".* TO 'dblog'@'%'; "
                + "GRANT ALL PRIVILEGES ON dblog_meta.* TO 'dblog'@'%'; "
                + "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'dblog'@'%'; "
                + "GRANT SESSION_VARIABLES_ADMIN ON *.* TO 'dblog'@'%'; "
                + "FLUSH PRIVILEGES;\"");
    if (result.getExitCode() != 0) {
      throw new IllegalStateException(
          "Failed to apply DBLog MySQL test-user grants. stdout="
              + result.getStdout()
              + " stderr="
              + result.getStderr());
    }
  }
}
