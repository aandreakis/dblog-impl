package io.github.aandreakis.dblog.adapter.postgres.internal;

import io.github.aandreakis.dblog.adapter.api.SingletonMetadataRowSupport;
import io.github.aandreakis.dblog.adapter.api.WatermarkMetadataWriter;
import io.github.aandreakis.dblog.core.model.WatermarkToken;
import io.github.aandreakis.dblog.core.reconcile.WatermarkMetadata;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Objects;

public final class JdbcPostgresWatermarkTableHelper implements WatermarkMetadataWriter {
  @Override
  public void ensureMetadataTable(Connection connection) throws SQLException {
    Objects.requireNonNull(connection, "connection");

    try (Statement statement = connection.createStatement()) {
      statement.execute("CREATE SCHEMA IF NOT EXISTS " + PostgresSql.quoteIdentifier(WatermarkMetadata.SCHEMA_NAME));
      statement.execute(
          "CREATE TABLE IF NOT EXISTS "
              + PostgresSql.quoteIdentifier(WatermarkMetadata.SCHEMA_NAME)
              + "."
              + PostgresSql.quoteIdentifier(WatermarkMetadata.TABLE_NAME)
              + " ("
              + PostgresSql.quoteIdentifier(WatermarkMetadata.PRIMARY_KEY_COLUMN)
              + " BIGINT PRIMARY KEY, "
              + PostgresSql.quoteIdentifier(WatermarkMetadata.RUN_ID_COLUMN)
              + " TEXT, "
              + PostgresSql.quoteIdentifier(WatermarkMetadata.TOKEN_COLUMN)
              + " TEXT)");
    }

    try (PreparedStatement statement =
        connection.prepareStatement(
            "INSERT INTO "
                + PostgresSql.quoteIdentifier(WatermarkMetadata.SCHEMA_NAME)
                + "."
                + PostgresSql.quoteIdentifier(WatermarkMetadata.TABLE_NAME)
                + " ("
                + PostgresSql.quoteIdentifier(WatermarkMetadata.PRIMARY_KEY_COLUMN)
                + ", "
                + PostgresSql.quoteIdentifier(WatermarkMetadata.RUN_ID_COLUMN)
                + ", "
                + PostgresSql.quoteIdentifier(WatermarkMetadata.TOKEN_COLUMN)
                + ") VALUES (?, ?, ?)")) {
      statement.setLong(1, WatermarkMetadata.SINGLETON_ROW_ID);
      statement.setNull(2, Types.VARCHAR);
      statement.setNull(3, Types.VARCHAR);
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
  public void writeWatermark(Connection connection, String runId, WatermarkToken token)
      throws SQLException {
    Objects.requireNonNull(connection, "connection");
    SingletonMetadataRowSupport.requireNonBlank(runId, "runId");
    Objects.requireNonNull(token, "token");

    try (PreparedStatement statement =
        connection.prepareStatement(
            "UPDATE "
                + PostgresSql.quoteIdentifier(WatermarkMetadata.SCHEMA_NAME)
                + "."
                + PostgresSql.quoteIdentifier(WatermarkMetadata.TABLE_NAME)
                + " SET "
                + PostgresSql.quoteIdentifier(WatermarkMetadata.RUN_ID_COLUMN)
                + " = ?, "
                + PostgresSql.quoteIdentifier(WatermarkMetadata.TOKEN_COLUMN)
                + " = ? WHERE "
                + PostgresSql.quoteIdentifier(WatermarkMetadata.PRIMARY_KEY_COLUMN)
                + " = ?")) {
      statement.setString(1, runId);
      statement.setString(2, token.value());
      statement.setLong(3, WatermarkMetadata.SINGLETON_ROW_ID);
      SingletonMetadataRowSupport.executeSingletonUpdate(statement, "PostgreSQL", "watermark");
    }
  }

  private static boolean isDuplicateKey(SQLException exception) {
    return "23505".equals(exception.getSQLState())
        || (exception.getMessage() != null
            && exception.getMessage().toLowerCase(java.util.Locale.ROOT).contains("primary key"));
  }
}
