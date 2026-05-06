package io.github.aandreakis.dblog.state.jdbc;

import io.github.aandreakis.dblog.state.api.SourceOwnershipRepository;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;

public final class JdbcSourceOwnershipRepository implements SourceOwnershipRepository {
  private final JdbcStateStoreSupport jdbc;

  public JdbcSourceOwnershipRepository(JdbcStateStoreSupport jdbc) {
    this.jdbc = Objects.requireNonNull(jdbc, "jdbc");
  }

  @Override
  public void claimSourceOwnership(String sourceId) {
    Objects.requireNonNull(sourceId, "sourceId");
    if (sourceId.isBlank()) {
      throw new IllegalArgumentException("sourceId must not be blank");
    }
    jdbc.withTransaction(
        connection -> {
          Optional<String> existingOwner = readMetadata(connection, "owning_source_id");
          if (existingOwner.isPresent() && !existingOwner.orElseThrow().equals(sourceId)) {
            throw new IllegalStateException(
                "state store is already owned by sourceId "
                    + existingOwner.orElseThrow()
                    + " and cannot be reused for sourceId "
                    + sourceId);
          }
          upsertMetadata(connection, "owning_source_id", sourceId);
          return null;
        });
  }

  private void upsertMetadata(Connection connection, String key, String value) throws SQLException {
    int updated =
        jdbc.update(
            connection, "UPDATE STATE_STORE_META SET META_VALUE = ? WHERE META_KEY = ?", value, key);
    if (updated == 0) {
      jdbc.insert(
          connection,
          "INSERT INTO STATE_STORE_META (META_KEY, META_VALUE) VALUES (?, ?)",
          key,
          value);
    }
  }

  private Optional<String> readMetadata(Connection connection, String key) throws SQLException {
    return jdbc.queryOptional(
        connection,
        "SELECT META_VALUE FROM STATE_STORE_META WHERE META_KEY = ?",
        resultSet -> resultSet.getString(1),
        key);
  }
}
