package io.github.aandreakis.dblog.adapter.mysql.internal;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;

/**
 * Helper that session-scopes {@code sql_log_bin = 0} around DBLog's own metadata DDL/INSERT
 * bootstrap so the statements do not enter the binlog and are not observed by the adapter's
 * own streaming session (or downstream MySQL replicas).
 *
 * <p>Debezium uses the same pattern for its signal and heartbeat tables. Disabling the
 * session flag requires the connection user to hold {@code SESSION_VARIABLES_ADMIN} on
 * MySQL 8+ (or {@code SUPER} on 5.7). If the session lacks that privilege the disable call
 * throws and the caller should treat the bypass as inactive: DDL then reaches the binlog
 * and must be skipped by {@link MySqlBinlogSession#shouldIgnoreQuery} via SQL-pattern match.
 *
 * <p>The restore-to-ON step is unconditional at the end of bootstrap because the same
 * connection is reused by the live streaming runtime for watermark UPDATEs, which MUST be
 * binlogged so the streaming session can decode low/high watermark events.
 */
final class MySqlBinlogBypass {
  private MySqlBinlogBypass() {}

  /**
   * Attempt to disable session-scoped binary logging for the given connection. Returns
   * {@code true} if the disable succeeded and the caller is now responsible for calling
   * {@link #restoreForSession(Connection, Logger)} in a {@code finally} block. Returns
   * {@code false} if the session lacks privilege; the caller should proceed and rely on
   * the defensive SQL-pattern filter in the binlog decoder to skip the DDL instead.
   */
  static boolean disableForSession(Connection connection, Logger log) {
    try (Statement statement = connection.createStatement()) {
      statement.execute("SET SESSION sql_log_bin = 0");
      return true;
    } catch (SQLException failure) {
      log.warn(
          "Could not disable session binary logging for DBLog metadata bootstrap (user likely"
              + " lacks SESSION_VARIABLES_ADMIN/SUPER); bootstrap DDL will enter the binlog and be"
              + " skipped by the decoder's SQL-pattern fallback, but downstream replicas will"
              + " still observe these statements: {}",
          failure.getMessage());
      return false;
    }
  }

  /**
   * Restore session-scoped binary logging to ON. Logs an ERROR on failure because the live
   * runtime requires {@code sql_log_bin=1} for its watermark UPDATEs to be decodable; the
   * caller should let the SQLException propagate if restoration fails.
   */
  static void restoreForSession(Connection connection, Logger log) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute("SET SESSION sql_log_bin = 1");
    } catch (SQLException failure) {
      log.error(
          "Failed to restore SESSION sql_log_bin=1 after DBLog metadata bootstrap;"
              + " watermark writes will not be decodable on this session: {}",
          failure.getMessage());
      throw failure;
    }
  }
}
