package io.github.aandreakis.dblog.state.jdbc;

import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.state.api.StreamPositionRepository;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;

public final class JdbcStreamPositionRepository implements StreamPositionRepository {
  private static final String BOOTSTRAP_STREAM_POSITION_KEY_PREFIX = "bootstrap_stream_position::";

  private final JdbcStateStoreSupport jdbc;

  public JdbcStreamPositionRepository(JdbcStateStoreSupport jdbc) {
    this.jdbc = Objects.requireNonNull(jdbc, "jdbc");
  }

  @Override
  public void saveCheckpoint(String sourceId, SourcePosition position) {
    Objects.requireNonNull(sourceId, "sourceId");
    Objects.requireNonNull(position, "position");
    jdbc.withTransaction(
        connection -> {
          int updated =
              jdbc.update(
                  connection,
                  "UPDATE STREAM_CHECKPOINT SET POSITION_VALUE = ? WHERE SOURCE_ID = ?",
                  position.displayValue(),
                  sourceId);
          if (updated == 0) {
            jdbc.insert(
                connection,
                "INSERT INTO STREAM_CHECKPOINT (SOURCE_ID, POSITION_VALUE) VALUES (?, ?)",
                sourceId,
                position.displayValue());
          }
          deleteMetadata(connection, bootstrapStreamPositionKey(sourceId));
          return null;
        });
  }

  @Override
  public Optional<SourcePosition> loadCheckpoint(String sourceId) {
    Objects.requireNonNull(sourceId, "sourceId");
    return jdbc.withTransaction(
        connection ->
            jdbc.queryOptional(
                connection,
                "SELECT POSITION_VALUE FROM STREAM_CHECKPOINT WHERE SOURCE_ID = ?",
                resultSet -> new OpaqueSourcePosition(resultSet.getString(1)),
                sourceId)
                .map(position -> (SourcePosition) position));
  }

  @Override
  public void saveBootstrapPosition(String sourceId, SourcePosition position) {
    Objects.requireNonNull(sourceId, "sourceId");
    Objects.requireNonNull(position, "position");
    if (sourceId.isBlank()) {
      throw new IllegalArgumentException("sourceId must not be blank");
    }
    String metadataKey = bootstrapStreamPositionKey(sourceId);
    String value = position.displayValue();
    jdbc.withTransaction(
        connection -> {
          // Atomic "set bootstrap position only if no real checkpoint exists". The guard is
          // embedded in each SQL statement (rather than a preceding SELECT) so the decision is
          // not vulnerable to concurrent inserts between the check and the write. Today the
          // orchestrator is single-threaded so this is defensive hardening for future HA work
          // (see Task 18) and for any refactor that might move this call off the single-
          // consumer path.
          int updated =
              jdbc.update(
                  connection,
                  "UPDATE STATE_STORE_META SET META_VALUE = ? "
                      + "WHERE META_KEY = ? "
                      + "  AND NOT EXISTS (SELECT 1 FROM STREAM_CHECKPOINT WHERE SOURCE_ID = ?)",
                  value,
                  metadataKey,
                  sourceId);
          if (updated == 0) {
            jdbc.insert(
                connection,
                "INSERT INTO STATE_STORE_META (META_KEY, META_VALUE) "
                    + "SELECT ?, ? "
                    + "WHERE NOT EXISTS (SELECT 1 FROM STREAM_CHECKPOINT WHERE SOURCE_ID = ?) "
                    + "  AND NOT EXISTS (SELECT 1 FROM STATE_STORE_META WHERE META_KEY = ?)",
                metadataKey,
                value,
                sourceId,
                metadataKey);
          }
          return null;
        });
  }

  @Override
  public Optional<SourcePosition> loadBootstrapPosition(String sourceId) {
    Objects.requireNonNull(sourceId, "sourceId");
    if (sourceId.isBlank()) {
      throw new IllegalArgumentException("sourceId must not be blank");
    }
    return jdbc.withTransaction(
        connection ->
            readMetadata(connection, bootstrapStreamPositionKey(sourceId))
                .map(OpaqueSourcePosition::new)
                .map(position -> (SourcePosition) position));
  }

  @Override
  public void clearBootstrapPosition(String sourceId) {
    Objects.requireNonNull(sourceId, "sourceId");
    if (sourceId.isBlank()) {
      throw new IllegalArgumentException("sourceId must not be blank");
    }
    jdbc.withTransaction(
        connection -> {
          clearBootstrapPositionInTxn(connection, sourceId);
          return null;
        });
  }

  /**
   * In-transaction variant of {@link #clearBootstrapPosition(String)} for composite operations
   * (e.g. {@link io.github.aandreakis.dblog.state.api.RuntimeStateStore#invalidateRuntimeStateForPkChange})
   * that need to combine multiple repository writes into one atomic unit.
   */
  void clearBootstrapPositionInTxn(Connection connection, String sourceId) throws SQLException {
    Objects.requireNonNull(connection, "connection");
    Objects.requireNonNull(sourceId, "sourceId");
    if (sourceId.isBlank()) {
      throw new IllegalArgumentException("sourceId must not be blank");
    }
    deleteMetadata(connection, bootstrapStreamPositionKey(sourceId));
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

  private void deleteMetadata(Connection connection, String key) throws SQLException {
    jdbc.update(connection, "DELETE FROM STATE_STORE_META WHERE META_KEY = ?", key);
  }

  private static String bootstrapStreamPositionKey(String sourceId) {
    return BOOTSTRAP_STREAM_POSITION_KEY_PREFIX + sourceId;
  }
}
