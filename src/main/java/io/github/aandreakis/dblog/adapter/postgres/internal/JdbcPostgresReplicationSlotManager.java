package io.github.aandreakis.dblog.adapter.postgres.internal;

import io.github.aandreakis.dblog.adapter.postgres.PostgresLsn;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;

/** JDBC-backed manager for the narrow PostgreSQL logical replication-slot contract. */
public final class JdbcPostgresReplicationSlotManager implements PostgresReplicationSlotManager {
  @Override
  public Optional<PostgresReplicationSlotState> readSlot(Connection connection, String slotName)
      throws SQLException {
    Objects.requireNonNull(connection, "connection");
    slotName = requireNonBlank(slotName, "slotName");
    PostgresServerVersion serverVersion = PostgresServerVersion.from(connection);

    try (PreparedStatement statement =
        connection.prepareStatement(PostgresSql.replicationSlotStateSql(serverVersion))) {
      statement.setString(1, slotName);
      try (ResultSet resultSet = statement.executeQuery()) {
        if (!resultSet.next()) {
          return Optional.empty();
        }
        return Optional.of(
            new PostgresReplicationSlotState(
                resultSet.getString("slot_name"),
                resultSet.getString("slot_type"),
                resultSet.getString("database"),
                resultSet.getString("plugin"),
                resultSet.getBoolean("temporary"),
                resultSet.getBoolean("active"),
                resultSet.getBoolean("two_phase"),
                resultSet.getBoolean("failover"),
                parseOptionalLsn(resultSet.getString("restart_lsn")),
                parseOptionalLsn(resultSet.getString("confirmed_flush_lsn")),
                resultSet.getString("wal_status"),
                resultSet.getString("invalidation_reason")));
      }
    }
  }

  @Override
  public PostgresReplicationSlotState ensureLogicalSlot(
      Connection connection, PostgresReplicationSlotConfig config)
      throws SQLException {
    Objects.requireNonNull(connection, "connection");
    Objects.requireNonNull(config, "config");
    PostgresServerVersion serverVersion = PostgresServerVersion.from(connection);

    Optional<PostgresReplicationSlotState> state = readSlot(connection, config.slotName());
    if (state.isEmpty()) {
      if (!config.ownership().isDblogManaged()) {
        throw new IllegalStateException(
            "externally managed PostgreSQL logical slot is missing: " + config.slotName());
      }
      if (config.failover() && !serverVersion.supportsReplicationSlotCreationFailoverArgument()) {
        throw new IllegalStateException(
            "PostgreSQL logical slot failover is unsupported before PostgreSQL 17: "
                + config.slotName());
      }
      try (PreparedStatement statement =
          connection.prepareStatement(PostgresSql.createLogicalReplicationSlotSql(serverVersion))) {
        statement.setString(1, config.slotName());
        statement.setString(2, config.pluginName());
        statement.setBoolean(3, config.temporary());
        statement.setBoolean(4, config.twoPhase());
        if (serverVersion.supportsReplicationSlotCreationFailoverArgument()) {
          statement.setBoolean(5, config.failover());
        }
        try (ResultSet ignored = statement.executeQuery()) {
          if (!ignored.next()) {
            throw new IllegalStateException(
                "PostgreSQL logical slot creation returned no row for " + config.slotName());
          }
        }
      }
      state = readSlot(connection, config.slotName());
    }

    PostgresReplicationSlotState currentState =
        state.orElseThrow(
            () ->
                new IllegalStateException(
                    "PostgreSQL logical slot did not exist after creation: " + config.slotName()));
    validateSlotState(currentState, config, serverVersion);
    return currentState;
  }

  private static void validateSlotState(
      PostgresReplicationSlotState state,
      PostgresReplicationSlotConfig config,
      PostgresServerVersion serverVersion) {
    if (!state.isLogical()) {
      throw new IllegalStateException(
          "PostgreSQL slot must be logical, but was " + state.slotType() + ": " + state.slotName());
    }
    if (!config.databaseName().equals(state.databaseName())) {
      throw new IllegalStateException(
          "PostgreSQL logical slot database does not match expected database. expected="
              + config.databaseName()
              + " actual="
              + state.databaseName());
    }
    if (!config.pluginName().equals(state.pluginName())) {
      throw new IllegalStateException(
          "PostgreSQL logical slot plugin does not match expected plugin. expected="
              + config.pluginName()
              + " actual="
              + state.pluginName());
    }
    if (state.temporary() != config.temporary()) {
      throw new IllegalStateException(
          "PostgreSQL logical slot temporary flag does not match expected value for "
              + state.slotName());
    }
    if (state.twoPhase() != config.twoPhase()) {
      throw new IllegalStateException(
          "PostgreSQL logical slot two_phase flag does not match expected value for "
              + state.slotName());
    }
    if (serverVersion.supportsReplicationSlotFailover() && state.failover() != config.failover()) {
      throw new IllegalStateException(
          "PostgreSQL logical slot failover flag does not match expected value for "
              + state.slotName());
    }
    if (!state.isUsable()) {
      throw new IllegalStateException(
          "PostgreSQL logical slot is invalid or lost. wal_status="
              + state.walStatus()
              + " invalidation_reason="
              + state.invalidationReason());
    }
    if (state.active()) {
      throw new IllegalStateException(
          "PostgreSQL logical slot is already active and cannot be reused safely right now: "
              + state.slotName());
    }
  }

  private static Optional<PostgresLsn> parseOptionalLsn(String value) {
    return value == null || value.isBlank() ? Optional.empty() : Optional.of(PostgresLsn.parse(value));
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
