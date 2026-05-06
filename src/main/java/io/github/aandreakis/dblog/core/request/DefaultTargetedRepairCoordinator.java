package io.github.aandreakis.dblog.core.request;

import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.adapter.api.WatermarkWindowResult;
import io.github.aandreakis.dblog.adapter.api.WatermarkWindowRuntime;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.reconcile.WatermarkWindow;
import io.github.aandreakis.dblog.core.reconcile.WindowReconciler;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import io.github.aandreakis.dblog.core.schema.SchemaPolicyEngine;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.tap.Tap;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Coordinates a targeted primary-key repair through one source watermark window.
 *
 * <p>Reuses the same Algorithm 1 machinery as {@link DefaultDumpWindowCoordinator}: opens a
 * watermark window, issues a single bounded SELECT for the requested keys, drives queued log
 * transactions through {@link WindowReconciler} until the matching high watermark is observed,
 * and emits the surviving rows. Unlike a dump request, there is no chunk loop — one
 * {@link #coordinate} call covers all requested keys at once.
 *
 * <p>Repair semantics:
 *
 * <ul>
 *   <li>Keys absent from the source at SELECT time are reported as missing in the result;
 *       in-window log events that committed between LW and HW still flow to the sink so a row
 *       that was deleted concurrently does not get destructively popped from the source queue.
 *   <li>Schema drift detected against the request's current selected-column contract
 *       short-circuits via {@link io.github.aandreakis.dblog.core.schema.SchemaPolicyEngine}
 *       in {@code acceptCompatibleChunk}, after the in-window SELECT returns and before any
 *       reconciler session is opened. The chunk is rejected with a
 *       {@link io.github.aandreakis.dblog.core.schema.SchemaDriftException}; the LW/HW
 *       already written to {@code dblog_meta.watermarks} become orphan tokens that
 *       {@link WindowReconciler}'s run-id stale-token mechanism drains on a subsequent request.
 *   <li>Per-request validation is narrow: scope must be {@code PRIMARY_KEYS} and the supplied
 *       schema's tableId must match the request. Literal canonicalization happens earlier at
 *       submission via
 *       {@link io.github.aandreakis.dblog.core.schema.TableSchema#primaryKeyTuplesFromLiterals}.
 * </ul>
 *
 * <p>The repair is single-shot: there is no persisted progress record to resume from. A failed
 * repair (timeout, schema drift, source error) does not modify user-table state; any LW/HW
 * already written to {@code dblog_meta.watermarks} become stale tokens drained on the next
 * request. The streaming checkpoint is not advanced through the failed window, so streaming
 * CDC resumes from the same position and the operator can resubmit.
 */
public final class DefaultTargetedRepairCoordinator<TX extends SourceTransaction<?>>
    implements TargetedRepairCoordinator<TX> {
  private final String adapterLabel;
  private final String transactionLabel;
  private final WatermarkWindowRuntime<TX> runtime;
  private final SourceChunkReader chunkReader;
  private final WindowReconciler windowReconciler;
  private final SchemaPolicyEngine schemaPolicyEngine;
  private final Duration transactionWaitTimeout;
  private final Duration pollInterval;
  private final Tap tap;

  public DefaultTargetedRepairCoordinator(
      String adapterLabel,
      String transactionLabel,
      WatermarkWindowRuntime<TX> runtime,
      SourceChunkReader chunkReader,
      WindowReconciler windowReconciler,
      Duration transactionWaitTimeout,
      Duration pollInterval,
      Tap tap) {
    this.adapterLabel = requireNonBlank(adapterLabel, "adapterLabel");
    this.transactionLabel = requireNonBlank(transactionLabel, "transactionLabel");
    this.runtime = Objects.requireNonNull(runtime, "runtime");
    this.chunkReader = Objects.requireNonNull(chunkReader, "chunkReader");
    this.windowReconciler = Objects.requireNonNull(windowReconciler, "windowReconciler");
    this.schemaPolicyEngine = new SchemaPolicyEngine();
    this.transactionWaitTimeout = positiveDuration(transactionWaitTimeout, "transactionWaitTimeout");
    this.pollInterval = positiveDuration(pollInterval, "pollInterval");
    this.tap = Objects.requireNonNull(tap, "tap");
  }

  @Override
  public TargetedRepairResult<TX> coordinate(DumpRequest request, TableSchema schema) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(schema, "schema");
    validateRequest(request, schema);

    List<PrimaryKeyTuple> canonicalRequestedKeys = canonicalRequestedKeys(request.primaryKeyTuples());

    // Single SELECT inside the watermark window (paper Algorithm 1 step 3). An empty result
    // means none of the requested primary keys currently exist; report all as missing. We
    // still drive the in-flight transactions through a reconciler session so log events that
    // committed between LW and HW reach the sink (they would otherwise be destructively popped
    // from the source queue by an HW-search loop).
    WatermarkWindowResult<Optional<Chunk>> windowResult =
        executeSql(
            () ->
                runtime.executeChunkReadInWatermarkWindow(
                    chunkReader,
                    (reader, window) ->
                        reader.targetedPrimaryKeyTuples(
                            request.requestId(), schema, canonicalRequestedKeys)),
            "coordinate targeted primary keys within watermark window");
    Optional<Chunk> coordinatedChunk = windowResult.value();
    if (coordinatedChunk.isEmpty()) {
      TargetedRepairDrainBatch<TX> drain =
          readUntilDrainComplete(request, schema, windowResult.window(), canonicalRequestedKeys);
      return new TargetedRepairResult<>(Optional.of(drain), canonicalRequestedKeys);
    }

    Chunk chunk = acceptCompatibleChunk(schema, coordinatedChunk.orElseThrow());
    runtime.updateCapturedSchema(chunk.schema());
    List<PrimaryKeyTuple> missingPrimaryKeys = missingRequestedKeys(canonicalRequestedKeys, chunk);
    tap.onChunkSelected(request.requestId(), chunk);
    TargetedRepairBatch<TX> batch =
        readUntilRepairComplete(request, chunk, windowResult.window(), missingPrimaryKeys);
    return new TargetedRepairResult<>(Optional.of(batch), missingPrimaryKeys);
  }

  @Override
  public void acknowledge(TargetedRepairOutcome<TX> outcome) {
    Objects.requireNonNull(outcome, "outcome");
    if (outcome instanceof TargetedRepairBatch<TX> batch) {
      executeSql(
          () -> {
            runtime.acknowledge(batch.checkpointTransaction());
            return null;
          },
          "acknowledge targeted repair batch");
      tap.onChunkCompleted(
          batch.request().requestId(),
          batch.chunk(),
          batch.emittedEvents(),
          batch.checkpointTransaction().checkpointPosition());
      return;
    }
    if (outcome instanceof TargetedRepairDrainBatch<TX> drain) {
      executeSql(
          () -> {
            runtime.acknowledge(drain.checkpointTransaction());
            return null;
          },
          "acknowledge drained targeted repair window");
      return;
    }
    throw new IllegalArgumentException(
        "Unsupported TargetedRepairOutcome subtype: " + outcome.getClass().getName());
  }

  /**
   * Drives a {@link WindowReconciler.ReconciliationSession} across an empty-chunk drain window.
   * The session handles LW/HW state transitions via the reconciler's strict state machine and
   * collects every non-watermark log event committed between LW and HW, so nothing is lost when
   * the targeted-primary-key SELECT returned zero rows.
   */
  private TargetedRepairDrainBatch<TX> readUntilDrainComplete(
      DumpRequest request,
      TableSchema schema,
      WatermarkWindow window,
      List<PrimaryKeyTuple> missingPrimaryKeys) {
    Chunk drainChunk = Chunk.drainOnly(request.requestId(), schema);
    WindowReconciler.ReconciliationSession session =
        windowReconciler.openSession(drainChunk, window, runtime.currentRunId());
    while (true) {
      TX transaction = awaitTransaction();
      session.apply(transaction.events());
      if (session.chunkCompleted()) {
        return new TargetedRepairDrainBatch<>(
            request, window, missingPrimaryKeys, session.emittedEvents(), transaction);
      }
    }
  }

  private TX awaitTransaction() {
    long deadline = System.nanoTime() + transactionWaitTimeout.toNanos();
    while (System.nanoTime() < deadline) {
      try {
        Optional<TX> transaction = runtime.readPendingTransaction();
        if (transaction.isPresent()) {
          return transaction.orElseThrow();
        }
      } catch (RuntimeException ex) {
        throw ex;
      } catch (Exception ex) {
        throw executionFailure("await " + transactionLabel, ex);
      }
      sleepPollInterval();
    }
    throw new IllegalStateException(
        "Timed out waiting for a committed "
            + transactionLabel
            + " while coordinating a targeted repair window");
  }

  private TargetedRepairBatch<TX> readUntilRepairComplete(
      DumpRequest request,
      Chunk chunk,
      WatermarkWindow window,
      List<PrimaryKeyTuple> missingPrimaryKeys) {
    return readUntilRepairComplete(request, chunk, window, missingPrimaryKeys, List.of());
  }

  private TargetedRepairBatch<TX> readUntilRepairComplete(
      DumpRequest request,
      Chunk chunk,
      WatermarkWindow window,
      List<PrimaryKeyTuple> missingPrimaryKeys,
      List<ChangeEvent> initialLogEvents) {
    WindowReconciler.ReconciliationSession reconciliationSession =
        windowReconciler.openSession(chunk, window, runtime.currentRunId());
    if (!initialLogEvents.isEmpty()) {
      reconciliationSession.apply(initialLogEvents);
      if (reconciliationSession.chunkCompleted()) {
        throw new IllegalStateException(
            adapterLabel
                + " completed targeted repair reconciliation before a checkpoint transaction was available for "
                + chunk.schema().tableId().displayName());
      }
    }
    while (true) {
      TX transaction = awaitTransaction();
      reconciliationSession.apply(transaction.events());
      if (reconciliationSession.chunkCompleted()) {
        return new TargetedRepairBatch<>(
            request,
            chunk,
            window,
            missingPrimaryKeys,
            reconciliationSession.emittedEvents(),
            transaction);
      }
    }
  }

  private void sleepPollInterval() {
    try {
      Thread.sleep(pollInterval.toMillis());
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Interrupted while waiting for "
              + transactionLabel
              + "s during targeted repair coordination",
          ex);
    }
  }

  private static List<PrimaryKeyTuple> canonicalRequestedKeys(List<PrimaryKeyTuple> requestedKeys) {
    LinkedHashSet<PrimaryKeyTuple> deduped = new LinkedHashSet<>(requestedKeys);
    return List.copyOf(deduped);
  }

  private static List<PrimaryKeyTuple> missingRequestedKeys(
      List<PrimaryKeyTuple> requestedKeys, Chunk chunk) {
    LinkedHashSet<PrimaryKeyTuple> foundKeys = new LinkedHashSet<>(chunk.rowPrimaryKeyTuples());
    List<PrimaryKeyTuple> missing = new ArrayList<>();
    for (PrimaryKeyTuple requestedKey : requestedKeys) {
      if (!foundKeys.contains(requestedKey)) {
        missing.add(requestedKey);
      }
    }
    return List.copyOf(missing);
  }

  private void validateRequest(DumpRequest request, TableSchema schema) {
    if (request.scope() != DumpScope.PRIMARY_KEYS) {
      throw new IllegalArgumentException(
          adapterLabel + " targeted repair requests must use PRIMARY_KEYS scope");
    }
    if (!Objects.equals(request.tableId(), schema.tableId())) {
      throw new IllegalArgumentException(
          adapterLabel
              + " targeted repair request table does not match the supplied schema: expected "
              + request.tableId()
              + " but was "
              + schema.tableId().displayName());
    }
  }

  private Chunk acceptCompatibleChunk(TableSchema currentSchema, Chunk chunk) {
    Objects.requireNonNull(currentSchema, "currentSchema");
    Objects.requireNonNull(chunk, "chunk");
    TableSchema reconciled =
        schemaPolicyEngine.reconcileStrict(currentSchema, chunk.schema(), "targeted repair read");
    return chunk.schema().equals(reconciled) ? chunk : chunk.withSchema(reconciled);
  }

  private static Duration positiveDuration(Duration duration, String name) {
    Objects.requireNonNull(duration, name);
    if (duration.isZero() || duration.isNegative()) {
      throw new IllegalArgumentException(name + " must be > 0");
    }
    return duration;
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  @FunctionalInterface
  private interface SqlAction<T> {
    T run() throws Exception;
  }

  private <T> T executeSql(SqlAction<T> action, String label) {
    try {
      return action.run();
    } catch (RuntimeException ex) {
      throw ex;
    } catch (Exception ex) {
      throw executionFailure(label, ex);
    }
  }

  private CoreRequestExecutionException executionFailure(String label, Exception cause) {
    return new CoreRequestExecutionException(
        adapterLabel + " failed to " + label + " during targeted repair coordination", cause);
  }
}
