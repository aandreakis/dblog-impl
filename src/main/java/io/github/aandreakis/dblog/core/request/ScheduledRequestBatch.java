package io.github.aandreakis.dblog.core.request;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.reconcile.WatermarkWindow;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import java.util.List;
import java.util.Objects;

/**
 * A coordinated batch a {@link DumpRequestCoordinator} hands off to the runtime request pump.
 *
 * <p>Exactly one of {@link #tableOutcome()} / {@link #targetedOutcome()} is populated. Each
 * outcome is a sealed sum type (see {@link DumpWindowOutcome}, {@link TargetedRepairOutcome}) so
 * callers can pattern-match on whether a chunk was completed or the window was drained without a
 * chunk.
 */
public record ScheduledRequestBatch<TX extends SourceTransaction<?>>(
    DumpRequest request,
    DumpWindowOutcome<TX> tableOutcome,
    TargetedRepairOutcome<TX> targetedOutcome,
    boolean finalRequestBatch) {
  public ScheduledRequestBatch {
    request = Objects.requireNonNull(request, "request");
    boolean hasTableOutcome = tableOutcome != null;
    boolean hasTargetedOutcome = targetedOutcome != null;
    if (hasTableOutcome == hasTargetedOutcome) {
      throw new IllegalArgumentException(
          "exactly one outcome delegate must be present for a scheduled dump request batch");
    }
    if (hasTableOutcome
        && request.scope() != DumpScope.TABLE
        && request.scope() != DumpScope.ALL_TABLES) {
      throw new IllegalArgumentException(
          "table dump request batches require a TABLE- or ALL_TABLES-scoped dump request");
    }
    if (hasTargetedOutcome && request.scope() != DumpScope.PRIMARY_KEYS) {
      throw new IllegalArgumentException(
          "targeted repair request batches require a PRIMARY_KEYS-scoped dump request");
    }
    if (request.scope() == DumpScope.TABLE && hasTableOutcome) {
      boolean tableOutcomeIsFinal =
          tableOutcome instanceof DumpWindowDrainBatch<TX>
              || ((DumpWindowBatch<TX>) tableOutcome).chunk().finalChunk();
      if (finalRequestBatch != tableOutcomeIsFinal) {
        throw new IllegalArgumentException(
            "TABLE-scoped dump request batches must mirror the outcome's final flag");
      }
    }
    if (request.scope() == DumpScope.ALL_TABLES
        && finalRequestBatch
        && tableOutcome instanceof DumpWindowBatch<TX> tableBatch
        && !tableBatch.chunk().finalChunk()) {
      throw new IllegalArgumentException(
          "ALL_TABLES request batches may only be marked final when the delegated table chunk is final");
    }
    if (hasTargetedOutcome && !finalRequestBatch) {
      throw new IllegalArgumentException(
          "targeted repair request batches must always be final for the request");
    }
  }

  public static <TX extends SourceTransaction<?>> ScheduledRequestBatch<TX> table(
      DumpRequest request, DumpWindowOutcome<TX> outcome) {
    boolean isFinal =
        outcome instanceof DumpWindowDrainBatch<TX>
            || ((DumpWindowBatch<TX>) outcome).chunk().finalChunk();
    return table(request, outcome, isFinal);
  }

  public static <TX extends SourceTransaction<?>> ScheduledRequestBatch<TX> table(
      DumpRequest request, DumpWindowOutcome<TX> outcome, boolean finalRequestBatch) {
    return new ScheduledRequestBatch<>(
        request, Objects.requireNonNull(outcome, "outcome"), null, finalRequestBatch);
  }

  public static <TX extends SourceTransaction<?>> ScheduledRequestBatch<TX> targetedRepair(
      DumpRequest request, TargetedRepairOutcome<TX> outcome) {
    return new ScheduledRequestBatch<>(
        request, null, Objects.requireNonNull(outcome, "outcome"), true);
  }

  public List<ChangeEvent> emittedEvents() {
    return tableOutcome != null ? tableOutcome.emittedEvents() : targetedOutcome.emittedEvents();
  }

  public TX checkpointTransaction() {
    return tableOutcome != null
        ? tableOutcome.checkpointTransaction()
        : targetedOutcome.checkpointTransaction();
  }

  public SourcePosition checkpointPosition() {
    return checkpointTransaction().checkpointPosition();
  }

  public WatermarkWindow window() {
    return tableOutcome != null ? tableOutcome.window() : targetedOutcome.window();
  }

  public TableId tableId() {
    return tableOutcome != null ? tableOutcome.tableId() : targetedOutcome.tableId();
  }

  /**
   * Returns the coordinated chunk, or {@code null} when this batch represents a drain-only
   * outcome (empty-chunk SELECT with log events reconciled across the window).
   */
  public Chunk chunk() {
    if (tableOutcome instanceof DumpWindowBatch<TX> tableBatch) {
      return tableBatch.chunk();
    }
    if (targetedOutcome instanceof TargetedRepairBatch<TX> targetedBatch) {
      return targetedBatch.chunk();
    }
    return null;
  }

  public List<String> missingPrimaryKeyLiterals() {
    if (targetedOutcome != null) {
      return targetedOutcome.missingPrimaryKeys();
    }
    return List.of();
  }

  public List<PrimaryKeyTuple> missingPrimaryKeyTuples() {
    if (targetedOutcome != null) {
      return targetedOutcome.missingPrimaryKeyTuples();
    }
    return List.of();
  }

  /**
   * Backward-compatible accessor for callers that specifically want the chunk-carrying table
   * batch; returns {@code null} for drain outcomes.
   */
  public DumpWindowBatch<TX> tableBatch() {
    return tableOutcome instanceof DumpWindowBatch<TX> tableBatch ? tableBatch : null;
  }

  /**
   * Backward-compatible accessor for callers that specifically want the chunk-carrying targeted
   * repair batch; returns {@code null} for drain outcomes.
   */
  public TargetedRepairBatch<TX> targetedRepairBatch() {
    return targetedOutcome instanceof TargetedRepairBatch<TX> targetedBatch ? targetedBatch : null;
  }
}
