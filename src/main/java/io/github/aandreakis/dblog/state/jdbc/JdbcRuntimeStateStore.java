package io.github.aandreakis.dblog.state.jdbc;

import io.github.aandreakis.dblog.state.api.DumpProgressRepository;
import io.github.aandreakis.dblog.state.api.DumpRequestRepository;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import io.github.aandreakis.dblog.state.api.SchemaStateRepository;
import io.github.aandreakis.dblog.state.api.SourceOwnershipRepository;
import io.github.aandreakis.dblog.state.api.StreamPositionRepository;
import java.util.Objects;

/**
 * JDBC-backed aggregate {@link RuntimeStateStore}. Holds per-table repositories and the shared
 * {@link JdbcStateStoreSupport} so composite operations — notably
 * {@link #invalidateRuntimeStateForPkChange(String, String)} — can run inside a single
 * transaction that spans multiple repositories.
 */
public final class JdbcRuntimeStateStore implements RuntimeStateStore {
  private final JdbcStateStoreSupport jdbc;
  private final JdbcStreamPositionRepository streamPositions;
  private final JdbcDumpRequestRepository dumpRequests;
  private final JdbcDumpProgressRepository dumpProgress;
  private final JdbcSchemaStateRepository schemas;
  private final SourceOwnershipRepository ownership;

  public JdbcRuntimeStateStore(
      JdbcStateStoreSupport jdbc,
      JdbcStreamPositionRepository streamPositions,
      JdbcDumpRequestRepository dumpRequests,
      JdbcDumpProgressRepository dumpProgress,
      JdbcSchemaStateRepository schemas,
      SourceOwnershipRepository ownership) {
    this.jdbc = Objects.requireNonNull(jdbc, "jdbc");
    this.streamPositions = Objects.requireNonNull(streamPositions, "streamPositions");
    this.dumpRequests = Objects.requireNonNull(dumpRequests, "dumpRequests");
    this.dumpProgress = Objects.requireNonNull(dumpProgress, "dumpProgress");
    this.schemas = Objects.requireNonNull(schemas, "schemas");
    this.ownership = Objects.requireNonNull(ownership, "ownership");
  }

  @Override
  public StreamPositionRepository streamPositions() {
    return streamPositions;
  }

  @Override
  public DumpRequestRepository dumpRequests() {
    return dumpRequests;
  }

  @Override
  public DumpProgressRepository dumpProgress() {
    return dumpProgress;
  }

  @Override
  public SchemaStateRepository schemas() {
    return schemas;
  }

  @Override
  public SourceOwnershipRepository ownership() {
    return ownership;
  }

  @Override
  public void invalidateRuntimeStateForPkChange(String sourceId, String reason) {
    Objects.requireNonNull(sourceId, "sourceId");
    Objects.requireNonNull(reason, "reason");
    // Single-transaction wrapper: either all four invalidation writes commit, or none do. A
    // crash halfway through previously left checkpoint cleared but dump_requests still active,
    // which broke restart safety.
    jdbc.withTransaction(
        connection -> {
          streamPositions.clearBootstrapPositionInTxn(connection, sourceId);
          dumpProgress.deleteAllInTxn(connection);
          schemas.deleteSchemaUncertaintySignalsInTxn(connection, sourceId);
          dumpRequests.failNonTerminalRequestsInTxn(connection, reason);
          return null;
        });
  }
}
