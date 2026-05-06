package io.github.aandreakis.dblog.adapter.postgres.internal;

import io.github.aandreakis.dblog.core.model.TableId;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** JDBC-backed manager for the narrow explicit-table PostgreSQL publication contract. */
public final class JdbcPostgresPublicationManager implements PostgresPublicationManager {
  private static final Comparator<TableId> TABLE_ID_COMPARATOR =
      Comparator.comparing(TableId::schemaName).thenComparing(TableId::tableName);

  @Override
  public Optional<PostgresPublicationState> readPublication(
      Connection connection, String databaseName, String publicationName)
      throws SQLException {
    Objects.requireNonNull(connection, "connection");
    databaseName = requireNonBlank(databaseName, "databaseName");
    publicationName = requireNonBlank(publicationName, "publicationName");

    PostgresServerVersion serverVersion = PostgresServerVersion.from(connection);
    PublicationSummary summary =
        readPublicationSummary(connection, publicationName, serverVersion).orElse(null);
    if (summary == null) {
      return Optional.empty();
    }
    return Optional.of(
        new PostgresPublicationState(
            databaseName,
            publicationName,
            summary.allTables(),
            summary.schemaScoped(),
            summary.publishesInsert(),
            summary.publishesUpdate(),
            summary.publishesDelete(),
            summary.publishesTruncate(),
            summary.publishViaPartitionRoot(),
            readPublicationTables(connection, databaseName, publicationName, serverVersion)));
  }

  @Override
  public PostgresPublicationState ensurePublication(Connection connection, PostgresPublicationConfig config)
      throws SQLException {
    Objects.requireNonNull(connection, "connection");
    Objects.requireNonNull(config, "config");

    Optional<PostgresPublicationState> state =
        readPublication(connection, config.databaseName(), config.publicationName());
    if (state.isEmpty()) {
      if (!config.ownership().isDblogManaged()) {
        throw new IllegalStateException(
            "externally managed PostgreSQL publication is missing: " + config.publicationName());
      }
      try (Statement statement = connection.createStatement()) {
        statement.execute(
            PostgresSql.createPublicationSql(
                config.publicationName(), normalizeTableIds(config.capturedTables())));
      }
      state = readPublication(connection, config.databaseName(), config.publicationName());
    }

    PostgresPublicationState currentState =
        state.orElseThrow(
            () ->
                new IllegalStateException(
                    "PostgreSQL publication did not exist after creation: "
                        + config.publicationName()));

    if (config.ownership().isDblogManaged() && needsManagedRepair(currentState, config)) {
      if (currentState.allTables()) {
        throw new IllegalStateException(
            "DBLog-managed PostgreSQL publication uses FOR ALL TABLES and cannot be repaired safely in-place: "
                + config.publicationName());
      }
      if (currentState.schemaScoped()) {
        throw new IllegalStateException(
            "DBLog-managed PostgreSQL publication uses TABLES IN SCHEMA and cannot be repaired safely in-place: "
                + config.publicationName());
      }
      try (Statement statement = connection.createStatement()) {
        statement.execute(
            PostgresSql.alterPublicationSetTablesSql(
                config.publicationName(), normalizeTableIds(config.capturedTables())));
        statement.execute(PostgresSql.alterPublicationPublishOperationsSql(config.publicationName()));
      }
      currentState =
          readPublication(connection, config.databaseName(), config.publicationName())
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "PostgreSQL publication disappeared while being repaired: "
                              + config.publicationName()));
    }

    validatePublicationState(currentState, config);
    return currentState;
  }

  private Optional<PublicationSummary> readPublicationSummary(
      Connection connection, String publicationName, PostgresServerVersion serverVersion)
      throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(PostgresSql.publicationStateSql(serverVersion))) {
      statement.setString(1, publicationName);
      try (ResultSet resultSet = statement.executeQuery()) {
        if (!resultSet.next()) {
          return Optional.empty();
        }
        return Optional.of(
            new PublicationSummary(
                resultSet.getBoolean("puballtables"),
                resultSet.getBoolean("schema_scoped"),
                resultSet.getBoolean("pubinsert"),
                resultSet.getBoolean("pubupdate"),
                resultSet.getBoolean("pubdelete"),
                resultSet.getBoolean("pubtruncate"),
                resultSet.getBoolean("pubviaroot")));
      }
    }
  }

  private List<PostgresPublicationTableState> readPublicationTables(
      Connection connection,
      String databaseName,
      String publicationName,
      PostgresServerVersion serverVersion)
      throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(PostgresSql.publicationRelationStateSql(serverVersion))) {
      statement.setString(1, publicationName);
      try (ResultSet resultSet = statement.executeQuery()) {
        java.util.ArrayList<PostgresPublicationTableState> tables = new java.util.ArrayList<>();
        while (resultSet.next()) {
          tables.add(
              new PostgresPublicationTableState(
                  new TableId(
                      databaseName,
                      resultSet.getString("schemaname"),
                      resultSet.getString("tablename")),
                  resultSet.getBoolean("row_filter_present"),
                  resultSet.getBoolean("column_list_present")));
        }
        tables.sort(Comparator.comparing(state -> state.tableId(), TABLE_ID_COMPARATOR));
        return List.copyOf(tables);
      }
    }
  }

  private static boolean needsManagedRepair(
      PostgresPublicationState state, PostgresPublicationConfig config) {
    if (state.allTables() || state.schemaScoped()) {
      return false;
    }
    if (!hasRequiredPublishOperations(state)) {
      return true;
    }
    if (state.tables().stream().anyMatch(table -> table.rowFilterPresent() || table.columnListPresent())) {
      return true;
    }
    return !normalizeTableIds(state.tableIds()).equals(normalizeTableIds(config.capturedTables()));
  }

  private static void validatePublicationState(
      PostgresPublicationState state, PostgresPublicationConfig config) {
    if (state.allTables()) {
      throw new IllegalStateException(
          "PostgreSQL publication must not use FOR ALL TABLES for the narrow DBLog slice: "
              + state.publicationName());
    }
    if (state.schemaScoped()) {
      throw new IllegalStateException(
          "PostgreSQL publication must not use TABLES IN SCHEMA for the narrow DBLog slice: "
              + state.publicationName());
    }
    if (!hasRequiredPublishOperations(state)) {
      throw new IllegalStateException(
          "PostgreSQL publication must publish only insert, update, and delete operations for the narrow DBLog slice: "
              + state.publicationName());
    }
    if (state.publishViaPartitionRoot()) {
      throw new IllegalStateException(
          "PostgreSQL publication must not enable publish_via_partition_root for the narrow DBLog slice: "
              + state.publicationName());
    }
    List<PostgresPublicationTableState> filteredTables =
        state.tables().stream().filter(PostgresPublicationTableState::rowFilterPresent).toList();
    if (!filteredTables.isEmpty()) {
      throw new IllegalStateException(
          "PostgreSQL publication must not use row filters for the narrow DBLog slice: "
              + filteredTables);
    }
    List<PostgresPublicationTableState> projectedTables =
        state.tables().stream().filter(PostgresPublicationTableState::columnListPresent).toList();
    if (!projectedTables.isEmpty()) {
      throw new IllegalStateException(
          "PostgreSQL publication must not use column lists for the narrow DBLog slice: "
              + projectedTables);
    }

    List<TableId> expectedTables = normalizeTableIds(config.capturedTables());
    List<TableId> actualTables = normalizeTableIds(state.tableIds());
    if (!actualTables.equals(expectedTables)) {
      throw new IllegalStateException(
          "PostgreSQL publication table set does not match captured tables. expected="
              + expectedTables
              + " actual="
              + actualTables);
    }
  }

  private static boolean hasRequiredPublishOperations(PostgresPublicationState state) {
    return state.publishesInsert()
        && state.publishesUpdate()
        && state.publishesDelete()
        && !state.publishesTruncate();
  }

  private static List<TableId> normalizeTableIds(List<TableId> tableIds) {
    return tableIds.stream().sorted(TABLE_ID_COMPARATOR).toList();
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private record PublicationSummary(
      boolean allTables,
      boolean schemaScoped,
      boolean publishesInsert,
      boolean publishesUpdate,
      boolean publishesDelete,
      boolean publishesTruncate,
      boolean publishViaPartitionRoot) {}
}
