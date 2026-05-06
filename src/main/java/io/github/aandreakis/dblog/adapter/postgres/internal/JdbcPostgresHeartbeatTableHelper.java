package io.github.aandreakis.dblog.adapter.postgres.internal;

import io.github.aandreakis.dblog.adapter.api.HeartbeatMetadataWriter;
import io.github.aandreakis.dblog.adapter.api.HeartbeatRateLimiter;
import io.github.aandreakis.dblog.adapter.api.SingletonMetadataRowSupport;
import io.github.aandreakis.dblog.core.reconcile.HeartbeatMetadata;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

public final class JdbcPostgresHeartbeatTableHelper implements HeartbeatMetadataWriter {
  @Override
  public void ensureHeartbeatTable(Connection connection) throws SQLException {
    Objects.requireNonNull(connection, "connection");

    try (Statement statement = connection.createStatement()) {
      statement.execute("CREATE SCHEMA IF NOT EXISTS " + PostgresSql.quoteIdentifier(HeartbeatMetadata.SCHEMA_NAME));
      statement.execute(
          "CREATE TABLE IF NOT EXISTS "
              + PostgresSql.quoteIdentifier(HeartbeatMetadata.SCHEMA_NAME)
              + "."
              + PostgresSql.quoteIdentifier(HeartbeatMetadata.TABLE_NAME)
              + " ("
              + PostgresSql.quoteIdentifier(HeartbeatMetadata.PRIMARY_KEY_COLUMN)
              + " BIGINT PRIMARY KEY, "
              + PostgresSql.quoteIdentifier(HeartbeatMetadata.RUN_ID_COLUMN)
              + " TEXT, "
              + PostgresSql.quoteIdentifier(HeartbeatMetadata.SOURCE_STREAM_ID_COLUMN)
              + " TEXT, "
              + PostgresSql.quoteIdentifier(HeartbeatMetadata.TIMESTAMP_COLUMN)
              + " TIMESTAMP WITH TIME ZONE)");
    }

    try (PreparedStatement statement =
        connection.prepareStatement(
            "INSERT INTO "
                + PostgresSql.quoteIdentifier(HeartbeatMetadata.SCHEMA_NAME)
                + "."
                + PostgresSql.quoteIdentifier(HeartbeatMetadata.TABLE_NAME)
                + " ("
                + PostgresSql.quoteIdentifier(HeartbeatMetadata.PRIMARY_KEY_COLUMN)
                + ", "
                + PostgresSql.quoteIdentifier(HeartbeatMetadata.RUN_ID_COLUMN)
                + ", "
                + PostgresSql.quoteIdentifier(HeartbeatMetadata.SOURCE_STREAM_ID_COLUMN)
                + ", "
                + PostgresSql.quoteIdentifier(HeartbeatMetadata.TIMESTAMP_COLUMN)
                + ") VALUES (?, ?, ?, ?)")) {
      statement.setLong(1, HeartbeatMetadata.SINGLETON_ROW_ID);
      statement.setNull(2, Types.VARCHAR);
      statement.setNull(3, Types.VARCHAR);
      statement.setNull(4, Types.TIMESTAMP_WITH_TIMEZONE);
      try {
        statement.executeUpdate();
      } catch (SQLException duplicate) {
        if (!isDuplicateKey(duplicate)) {
          throw duplicate;
        }
      }
    }
  }

  @Override
  public boolean writeHeartbeatIfDue(
      Connection connection,
      String runId,
      String sourceStreamId,
      Instant heartbeatTime,
      Duration minimumInterval)
      throws SQLException {
    Objects.requireNonNull(connection, "connection");
    SingletonMetadataRowSupport.requireNonBlank(runId, "runId");
    SingletonMetadataRowSupport.requireNonBlank(sourceStreamId, "sourceStreamId");
    Objects.requireNonNull(heartbeatTime, "heartbeatTime");
    Objects.requireNonNull(minimumInterval, "minimumInterval");
    if (minimumInterval.isNegative() || minimumInterval.isZero()) {
      throw new IllegalArgumentException("minimumInterval must be > 0");
    }

    if (HeartbeatRateLimiter.shouldSkipWrite(
        loadLastHeartbeat(connection), heartbeatTime, minimumInterval)) {
      return false;
    }

    try (PreparedStatement statement =
        connection.prepareStatement(
            "UPDATE "
                + PostgresSql.quoteIdentifier(HeartbeatMetadata.SCHEMA_NAME)
                + "."
                + PostgresSql.quoteIdentifier(HeartbeatMetadata.TABLE_NAME)
                + " SET "
                + PostgresSql.quoteIdentifier(HeartbeatMetadata.RUN_ID_COLUMN)
                + " = ?, "
                + PostgresSql.quoteIdentifier(HeartbeatMetadata.SOURCE_STREAM_ID_COLUMN)
                + " = ?, "
                + PostgresSql.quoteIdentifier(HeartbeatMetadata.TIMESTAMP_COLUMN)
                + " = ? WHERE "
                + PostgresSql.quoteIdentifier(HeartbeatMetadata.PRIMARY_KEY_COLUMN)
                + " = ?")) {
      statement.setString(1, runId);
      statement.setString(2, sourceStreamId);
      statement.setTimestamp(3, Timestamp.from(heartbeatTime));
      statement.setLong(4, HeartbeatMetadata.SINGLETON_ROW_ID);
      SingletonMetadataRowSupport.executeSingletonUpdate(statement, "PostgreSQL", "heartbeat");
    }
    return true;
  }

  /**
   * Reads the persisted heartbeat timestamp regardless of which run wrote it.
   *
   * <p>Rate-limiting the heartbeat write is a global, timestamp-based concern — if any run
   * recently wrote a heartbeat on this source stream, this run should wait the minimum interval
   * before writing its own, even if its {@code runId} differs. Same-stream foreign-run detection
   * (the fail-closed guard) runs separately on the <em>read</em> side inside the pgoutput session
   * and is not weakened by this.
   */
  private Optional<Instant> loadLastHeartbeat(Connection connection) throws SQLException {
    try (PreparedStatement statement =
            connection.prepareStatement(
                "SELECT "
                    + PostgresSql.quoteIdentifier(HeartbeatMetadata.TIMESTAMP_COLUMN)
                    + " FROM "
                    + PostgresSql.quoteIdentifier(HeartbeatMetadata.SCHEMA_NAME)
                    + "."
                    + PostgresSql.quoteIdentifier(HeartbeatMetadata.TABLE_NAME)
                    + " WHERE "
                    + PostgresSql.quoteIdentifier(HeartbeatMetadata.PRIMARY_KEY_COLUMN)
                    + " = ?")) {
      statement.setLong(1, HeartbeatMetadata.SINGLETON_ROW_ID);
      try (ResultSet resultSet = statement.executeQuery()) {
        if (!resultSet.next()) {
          return Optional.empty();
        }
        Timestamp timestamp = resultSet.getTimestamp(1);
        return timestamp == null ? Optional.empty() : Optional.of(timestamp.toInstant());
      }
    }
  }

  private static boolean isDuplicateKey(SQLException exception) {
    return "23505".equals(exception.getSQLState())
        || (exception.getMessage() != null
            && exception.getMessage().toLowerCase(java.util.Locale.ROOT).contains("primary key"));
  }
}
