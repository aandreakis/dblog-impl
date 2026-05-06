package io.github.aandreakis.dblog.adapter.postgres.internal;

import io.github.aandreakis.dblog.core.model.TableId;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;

/** JDBC inspector for PostgreSQL table replica identity. */
public class JdbcPostgresReplicaIdentityInspector {
  public Optional<PostgresReplicaIdentityState> readReplicaIdentity(
      Connection connection, TableId tableId) throws SQLException {
    Objects.requireNonNull(connection, "connection");
    Objects.requireNonNull(tableId, "tableId");

    try (PreparedStatement statement =
            connection.prepareStatement(PostgresSql.replicaIdentityStateSql())) {
      statement.setString(1, tableId.schemaName());
      statement.setString(2, tableId.tableName());
      try (ResultSet resultSet = statement.executeQuery()) {
        if (!resultSet.next()) {
          return Optional.empty();
        }
        String catalogCode = resultSet.getString("relreplident");
        if (catalogCode == null || catalogCode.isBlank()) {
          throw new IllegalStateException(
              "PostgreSQL replica-identity query returned blank relreplident for "
                  + tableId.displayName());
        }
        return Optional.of(
            new PostgresReplicaIdentityState(
                tableId, PostgresReplicaIdentity.fromCatalogCode(catalogCode)));
      }
    }
  }
}
