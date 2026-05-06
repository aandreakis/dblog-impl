package io.github.aandreakis.dblog.verification.scenario;

import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.verification.scenario.support.ScenarioFailureSupport;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Scenario-only final chunk-reader wrapper for delay/failure injection. */
public final class FaultInjectingSourceChunkReader implements SourceChunkReader {
  private final SourceChunkReader delegate;
  private final String adapterLabel;
  private final ScenarioFaultPlan faultPlan;
  private final ScenarioFailureSupport support;

  public FaultInjectingSourceChunkReader(
      SourceChunkReader delegate,
      String adapterLabel,
      ScenarioStore scenarioStore,
      String scenarioId,
      ScenarioFaultPlan faultPlan) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.adapterLabel = Objects.requireNonNull(adapterLabel, "adapterLabel");
    this.faultPlan = Objects.requireNonNull(faultPlan, "faultPlan");
    this.support = new ScenarioFailureSupport(scenarioStore, scenarioId);
  }

  @Override
  public Optional<PrimaryKeyTuple> tableScanUpperBoundPrimaryKeyTuple(
      Connection connection, TableSchema schema) throws SQLException {
    before("tableScanUpperBoundPrimaryKey", schema.tableId().displayName());
    return delegate.tableScanUpperBoundPrimaryKeyTuple(connection, schema);
  }

  @Override
  public Optional<Chunk> nextTableChunk(
      Connection connection,
      String jobId,
      TableSchema schema,
      PrimaryKeyTuple startAfterPrimaryKey,
      PrimaryKeyTuple stopAtPrimaryKey,
      int chunkSize)
      throws SQLException {
    before("nextTableChunk", schema.tableId().displayName());
    return delegate.nextTableChunk(
        connection, jobId, schema, startAfterPrimaryKey, stopAtPrimaryKey, chunkSize);
  }

  @Override
  public Optional<Chunk> targetedPrimaryKeyTuples(
      Connection connection,
      String jobId,
      TableSchema schema,
      List<PrimaryKeyTuple> requestedPrimaryKeys)
      throws SQLException {
    before("targetedPrimaryKeys", schema.tableId().displayName());
    return delegate.targetedPrimaryKeyTuples(connection, jobId, schema, requestedPrimaryKeys);
  }

  private void before(String operation, String detail) {
    support.maybeDelay("chunk-read", adapterLabel + " " + operation, faultPlan.chunkReadDelay());
    int invocation = support.nextInvocation();
    if (faultPlan.failChunkReadAfterCount() != null
        && invocation >= faultPlan.failChunkReadAfterCount()) {
      support.fail(
          "failure-injection",
          "Injected " + adapterLabel + " chunk-read failure",
          detail + " invocation=" + invocation);
    }
  }
}
