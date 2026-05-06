package io.github.aandreakis.dblog.adapter.mysql.internal;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcMySqlHeartbeatTableHelper implements HeartbeatMetadataWriter {
  private static final Logger log = LoggerFactory.getLogger(JdbcMySqlHeartbeatTableHelper.class);

  @Override
  public void ensureHeartbeatTable(Connection connection) throws SQLException {
    Objects.requireNonNull(connection, "connection");

    boolean binlogBypassActive = MySqlBinlogBypass.disableForSession(connection, log);
    try {
      try (Statement statement = connection.createStatement()) {
        statement.execute("CREATE DATABASE IF NOT EXISTS " + MySqlSql.quoteIdentifier(HeartbeatMetadata.SCHEMA_NAME));
        statement.execute(
            "CREATE TABLE IF NOT EXISTS "
                + MySqlSql.quoteIdentifier(HeartbeatMetadata.SCHEMA_NAME)
                + "."
                + MySqlSql.quoteIdentifier(HeartbeatMetadata.TABLE_NAME)
                + " ("
                + MySqlSql.quoteIdentifier(HeartbeatMetadata.PRIMARY_KEY_COLUMN)
                + " BIGINT PRIMARY KEY, "
                + MySqlSql.quoteIdentifier(HeartbeatMetadata.RUN_ID_COLUMN)
                + " VARCHAR(255) NULL, "
                + MySqlSql.quoteIdentifier(HeartbeatMetadata.SOURCE_STREAM_ID_COLUMN)
                + " VARCHAR(255) NULL, "
                + MySqlSql.quoteIdentifier(HeartbeatMetadata.TIMESTAMP_COLUMN)
                + " TIMESTAMP NULL)");
      }

      try (PreparedStatement statement =
          connection.prepareStatement(
              "INSERT INTO "
                  + MySqlSql.quoteIdentifier(HeartbeatMetadata.SCHEMA_NAME)
                  + "."
                  + MySqlSql.quoteIdentifier(HeartbeatMetadata.TABLE_NAME)
                  + " ("
                  + MySqlSql.quoteIdentifier(HeartbeatMetadata.PRIMARY_KEY_COLUMN)
                  + ", "
                  + MySqlSql.quoteIdentifier(HeartbeatMetadata.RUN_ID_COLUMN)
                  + ", "
                  + MySqlSql.quoteIdentifier(HeartbeatMetadata.SOURCE_STREAM_ID_COLUMN)
                  + ", "
                  + MySqlSql.quoteIdentifier(HeartbeatMetadata.TIMESTAMP_COLUMN)
                  + ") VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE "
                  + MySqlSql.quoteIdentifier(HeartbeatMetadata.PRIMARY_KEY_COLUMN)
                  + " = "
                  + MySqlSql.quoteIdentifier(HeartbeatMetadata.PRIMARY_KEY_COLUMN))) {
        statement.setLong(1, HeartbeatMetadata.SINGLETON_ROW_ID);
        statement.setNull(2, Types.VARCHAR);
        statement.setNull(3, Types.VARCHAR);
        statement.setNull(4, Types.TIMESTAMP);
        statement.executeUpdate();
      }
    } finally {
      if (binlogBypassActive) {
        MySqlBinlogBypass.restoreForSession(connection, log);
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
        loadLastHeartbeat(connection, runId), heartbeatTime, minimumInterval)) {
      return false;
    }

    try (PreparedStatement statement =
        connection.prepareStatement(
            "UPDATE "
                + MySqlSql.quoteIdentifier(HeartbeatMetadata.SCHEMA_NAME)
                + "."
                + MySqlSql.quoteIdentifier(HeartbeatMetadata.TABLE_NAME)
                + " SET "
                + MySqlSql.quoteIdentifier(HeartbeatMetadata.RUN_ID_COLUMN)
                + " = ?, "
                + MySqlSql.quoteIdentifier(HeartbeatMetadata.SOURCE_STREAM_ID_COLUMN)
                + " = ?, "
                + MySqlSql.quoteIdentifier(HeartbeatMetadata.TIMESTAMP_COLUMN)
                + " = ? WHERE "
                + MySqlSql.quoteIdentifier(HeartbeatMetadata.PRIMARY_KEY_COLUMN)
                + " = ?")) {
      statement.setString(1, runId);
      statement.setString(2, sourceStreamId);
      statement.setTimestamp(3, Timestamp.from(heartbeatTime));
      statement.setLong(4, HeartbeatMetadata.SINGLETON_ROW_ID);
      SingletonMetadataRowSupport.executeSingletonUpdate(statement, "MySQL", "heartbeat");
    }
    return true;
  }

  private Optional<Instant> loadLastHeartbeat(Connection connection, String runId) throws SQLException {
    try (PreparedStatement statement =
            connection.prepareStatement(
                "SELECT "
                    + MySqlSql.quoteIdentifier(HeartbeatMetadata.RUN_ID_COLUMN)
                    + ", "
                    + MySqlSql.quoteIdentifier(HeartbeatMetadata.TIMESTAMP_COLUMN)
                    + " FROM "
                    + MySqlSql.quoteIdentifier(HeartbeatMetadata.SCHEMA_NAME)
                    + "."
                    + MySqlSql.quoteIdentifier(HeartbeatMetadata.TABLE_NAME)
                    + " WHERE "
                    + MySqlSql.quoteIdentifier(HeartbeatMetadata.PRIMARY_KEY_COLUMN)
                    + " = ?");
        ) {
      statement.setLong(1, HeartbeatMetadata.SINGLETON_ROW_ID);
      try (ResultSet resultSet = statement.executeQuery()) {
        if (!resultSet.next()) {
          return Optional.empty();
        }
        String storedRunId = resultSet.getString(1);
        Timestamp timestamp = resultSet.getTimestamp(2);
        if (storedRunId == null || storedRunId.isBlank() || !runId.equals(storedRunId)) {
          return Optional.empty();
        }
        return timestamp == null ? Optional.empty() : Optional.of(timestamp.toInstant());
      }
    }
  }

}
