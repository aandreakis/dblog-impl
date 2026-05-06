package io.github.aandreakis.dblog.adapter.mysql.internal;

import io.github.aandreakis.dblog.adapter.api.SingletonMetadataRowSupport;
import io.github.aandreakis.dblog.adapter.api.WatermarkMetadataWriter;
import io.github.aandreakis.dblog.core.model.WatermarkToken;
import io.github.aandreakis.dblog.core.reconcile.WatermarkMetadata;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcMySqlWatermarkTableHelper implements WatermarkMetadataWriter {
  private static final Logger log = LoggerFactory.getLogger(JdbcMySqlWatermarkTableHelper.class);

  @Override
  public void ensureMetadataTable(Connection connection) throws SQLException {
    Objects.requireNonNull(connection, "connection");

    boolean binlogBypassActive = MySqlBinlogBypass.disableForSession(connection, log);
    try {
      try (Statement statement = connection.createStatement()) {
        if (!metadataDatabaseExists(connection)) {
          statement.execute("CREATE DATABASE IF NOT EXISTS " + MySqlSql.quoteIdentifier(WatermarkMetadata.SCHEMA_NAME));
        }
        if (!watermarkTableExists(connection)) {
          statement.execute(
              "CREATE TABLE IF NOT EXISTS "
                  + MySqlSql.quoteIdentifier(WatermarkMetadata.SCHEMA_NAME)
                  + "."
                  + MySqlSql.quoteIdentifier(WatermarkMetadata.TABLE_NAME)
                  + " ("
                  + MySqlSql.quoteIdentifier(WatermarkMetadata.PRIMARY_KEY_COLUMN)
                  + " BIGINT PRIMARY KEY, "
                  + MySqlSql.quoteIdentifier(WatermarkMetadata.RUN_ID_COLUMN)
                  + " VARCHAR(255) NULL, "
                  + MySqlSql.quoteIdentifier(WatermarkMetadata.TOKEN_COLUMN)
                  + " VARCHAR(512) NULL)");
        }
      }

      try (PreparedStatement statement =
          connection.prepareStatement(
              "INSERT INTO "
                  + MySqlSql.quoteIdentifier(WatermarkMetadata.SCHEMA_NAME)
                  + "."
                  + MySqlSql.quoteIdentifier(WatermarkMetadata.TABLE_NAME)
                  + " ("
                  + MySqlSql.quoteIdentifier(WatermarkMetadata.PRIMARY_KEY_COLUMN)
                  + ", "
                  + MySqlSql.quoteIdentifier(WatermarkMetadata.RUN_ID_COLUMN)
                  + ", "
                  + MySqlSql.quoteIdentifier(WatermarkMetadata.TOKEN_COLUMN)
                  + ") VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE "
                  + MySqlSql.quoteIdentifier(WatermarkMetadata.PRIMARY_KEY_COLUMN)
                  + " = "
                  + MySqlSql.quoteIdentifier(WatermarkMetadata.PRIMARY_KEY_COLUMN))) {
        statement.setLong(1, WatermarkMetadata.SINGLETON_ROW_ID);
        statement.setNull(2, Types.VARCHAR);
        statement.setNull(3, Types.VARCHAR);
        statement.executeUpdate();
      }
    } finally {
      if (binlogBypassActive) {
        MySqlBinlogBypass.restoreForSession(connection, log);
      }
    }
  }

  @Override
  public void writeWatermark(Connection connection, String runId, WatermarkToken token)
      throws SQLException {
    Objects.requireNonNull(connection, "connection");
    SingletonMetadataRowSupport.requireNonBlank(runId, "runId");
    Objects.requireNonNull(token, "token");

    try (PreparedStatement statement =
        connection.prepareStatement(
            "UPDATE "
                + MySqlSql.quoteIdentifier(WatermarkMetadata.SCHEMA_NAME)
                + "."
                + MySqlSql.quoteIdentifier(WatermarkMetadata.TABLE_NAME)
                + " SET "
                + MySqlSql.quoteIdentifier(WatermarkMetadata.RUN_ID_COLUMN)
                + " = ?, "
                + MySqlSql.quoteIdentifier(WatermarkMetadata.TOKEN_COLUMN)
                + " = ? WHERE "
                + MySqlSql.quoteIdentifier(WatermarkMetadata.PRIMARY_KEY_COLUMN)
                + " = ?")) {
      statement.setString(1, runId);
      statement.setString(2, token.value());
      statement.setLong(3, WatermarkMetadata.SINGLETON_ROW_ID);
      SingletonMetadataRowSupport.executeSingletonUpdate(statement, "MySQL", "watermark");
    }
  }

  private static boolean metadataDatabaseExists(Connection connection) throws SQLException {
    try (PreparedStatement statement =
            connection.prepareStatement(
                "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'dblog_meta'");
        ResultSet resultSet = statement.executeQuery()) {
      resultSet.next();
      return resultSet.getLong(1) == 1L;
    }
  }

  private static boolean watermarkTableExists(Connection connection) throws SQLException {
    try (PreparedStatement statement =
            connection.prepareStatement(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'dblog_meta' AND table_name = 'watermarks'");
        ResultSet resultSet = statement.executeQuery()) {
      resultSet.next();
      return resultSet.getLong(1) == 1L;
    }
  }

}
