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
import io.github.aandreakis.dblog.core.schema.SchemaDriftException;
import io.github.aandreakis.dblog.core.schema.SchemaPolicyEngine;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.state.api.DumpProgressRepository;
import io.github.aandreakis.dblog.state.api.SchemaStateRepository;
import io.github.aandreakis.dblog.tap.Tap;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Coordinates one range chunk of a dump request inside a source watermark window.
 *
 * <p>For each table/request it captures a request-scoped upper-bound primary key once, selects the
 * next chunk strictly above the last completed key and at or below that bound, and reconciles
 * queued log transactions through {@link WindowReconciler} until the matching high watermark is
 * observed.
 *
 * <p>Restart semantics are chunk-boundary based: if persisted progress still shows an active
 * chunk, the coordinator clears that in-flight window state and retries from the last completed
 * primary key. Source checkpoint advancement happens only after completed-chunk progress is
 * durable.
 */
public final class DefaultDumpWindowCoordinator<TX extends SourceTransaction<?>>
    implements DumpWindowCoordinator<TX> {
  private final String adapterLabel;
  private final String transactionLabel;
  private final WatermarkWindowRuntime<TX> runtime;
  private final DumpProgressRepository dumpProgress;
  private final SchemaStateRepository schemas;
  private final SourceChunkReader chunkReader;
  private final WindowReconciler windowReconciler;
  private final SchemaPolicyEngine schemaPolicyEngine;
  private final Duration transactionWaitTimeout;
  private final Duration pollInterval;
  private final Tap tap;

  public DefaultDumpWindowCoordinator(
      String adapterLabel,
      String transactionLabel,
      WatermarkWindowRuntime<TX> runtime,
      DumpProgressRepository dumpProgress,
      SchemaStateRepository schemas,
      SourceChunkReader chunkReader,
      WindowReconciler windowReconciler,
      Duration transactionWaitTimeout,
      Duration pollInterval,
      Tap tap) {
    this.adapterLabel = requireNonBlank(adapterLabel, "adapterLabel");
    this.transactionLabel = requireNonBlank(transactionLabel, "transactionLabel");
    this.runtime = Objects.requireNonNull(runtime, "runtime");
    this.dumpProgress = Objects.requireNonNull(dumpProgress, "dumpProgress");
    this.schemas = Objects.requireNonNull(schemas, "schemas");
    this.chunkReader = Objects.requireNonNull(chunkReader, "chunkReader");
    this.windowReconciler = Objects.requireNonNull(windowReconciler, "windowReconciler");
    this.schemaPolicyEngine = new SchemaPolicyEngine();
    this.transactionWaitTimeout = positiveDuration(transactionWaitTimeout, "transactionWaitTimeout");
    this.pollInterval = positiveDuration(pollInterval, "pollInterval");
    this.tap = Objects.requireNonNull(tap, "tap");
  }

  @Override
  public Optional<DumpWindowOutcome<TX>> coordinateNextTableChunk(
      String jobId, TableSchema schema, int chunkSize) {
    requireNonBlank(jobId, "jobId");
    Objects.requireNonNull(schema, "schema");
    if (chunkSize <= 0) {
      throw new IllegalArgumentException("chunkSize must be > 0");
    }

    LoadedDumpProgress loaded = loadProgress(jobId, schema);
    DumpTableProgress progress = loaded.progress();
    if (progress.hasActiveChunk()) {
      DumpTableProgress restarted = progress.restartFromLastCompletedBoundary();
      dumpProgress.save(restarted);
      progress = restarted;
    }
    progress = reconcileProgressSchema(progress, schema);
    if (!loaded.persisted()) {
      Optional<PrimaryKeyTuple> requestUpperBoundPrimaryKey =
          executeSql(
              () ->
                  runtime.executeChunkRead(
                      chunkReader, reader -> reader.tableScanUpperBoundPrimaryKeyTuple(schema)),
              "read request upper bound primary key");
      progress = progress.captureRequestUpperBound(requestUpperBoundPrimaryKey.orElse(null));
      dumpProgress.save(progress);
    }
    boolean activeProgressPersisted = false;
    try {
      if (hasReachedRequestUpperBound(progress, schema)) {
        return Optional.empty();
      }

      TableSchema currentSchema = schema;
      PrimaryKeyTuple resumeAfterPrimaryKey = progress.resumeAfterPrimaryKeyTuple();
      PrimaryKeyTuple requestUpperBoundPrimaryKey = progress.requestUpperBoundPrimaryKeyTuple();

      // Single SELECT inside the watermark window (paper Algorithm 1 step 3). An empty result
      // means either the table has been fully dumped or concurrent deletes drained the tail
      // after the last chunk's HW; either way this request has no remaining dump work for this
      // table. We still drive the in-flight transactions through a reconciler session so log
      // events that committed between LW and HW reach the sink (they would otherwise be
      // destructively popped from the source queue by an HW-search loop).
      WatermarkWindowResult<Optional<Chunk>> windowResult =
          executeSql(
              () ->
                  runtime.executeChunkReadInWatermarkWindow(
                      chunkReader,
                      (reader, window) ->
                          reader.nextTableChunk(
                              jobId,
                              schema,
                              resumeAfterPrimaryKey,
                              requestUpperBoundPrimaryKey,
                              chunkSize)),
              "coordinate next table chunk within watermark window");
      Optional<Chunk> coordinatedChunk = windowResult.value();
      if (coordinatedChunk.isEmpty()) {
        return Optional.of(readUntilDrainComplete(jobId, schema, windowResult.window()));
      }

      Chunk chunk = acceptCompatibleChunk(currentSchema, coordinatedChunk.orElseThrow());
      runtime.updateCapturedSchema(chunk.schema());
      progress = progress.withAcceptedSchema(chunk.schema());
      DumpTableProgress activeProgress = progress.beginChunk(chunk, windowResult.window());
      dumpProgress.save(activeProgress);
      activeProgressPersisted = true;
      tap.onChunkSelected(jobId, chunk);
      return Optional.of(readUntilChunkComplete(activeProgress, chunk, windowResult.window()));
    } catch (RuntimeException ex) {
      rollbackFreshProgressIfNeeded(
          loaded.persisted(),
          activeProgressPersisted,
          jobId,
          schema.tableId().displayName(),
          ex);
      throw ex;
    }
  }

  @Override
  public boolean hasRemainingTableWork(String jobId, TableSchema schema, int chunkSize) {
    requireNonBlank(jobId, "jobId");
    Objects.requireNonNull(schema, "schema");
    if (chunkSize <= 0) {
      throw new IllegalArgumentException("chunkSize must be > 0");
    }

    LoadedDumpProgress loaded = loadProgress(jobId, schema);
    DumpTableProgress progress = loaded.progress();
    if (progress.hasActiveChunk()) {
      return true;
    }
    progress = reconcileProgressSchema(progress, schema);
    if (!loaded.persisted()) {
      Optional<PrimaryKeyTuple> requestUpperBoundPrimaryKey =
          executeSql(
              () ->
                  runtime.executeChunkRead(
                      chunkReader, reader -> reader.tableScanUpperBoundPrimaryKeyTuple(schema)),
              "read request upper bound primary key");
      progress = progress.captureRequestUpperBound(requestUpperBoundPrimaryKey.orElse(null));
      dumpProgress.save(progress);
    }
    try {
      if (hasReachedRequestUpperBound(progress, schema)) {
        return false;
      }
      PrimaryKeyTuple resumeAfterPrimaryKey = progress.resumeAfterPrimaryKeyTuple();
      PrimaryKeyTuple requestUpperBoundPrimaryKey = progress.requestUpperBoundPrimaryKeyTuple();
      return executeSql(
              () ->
                  runtime.executeChunkRead(
                      chunkReader,
                      reader ->
                          reader.nextTableChunk(
                              jobId,
                              schema,
                              resumeAfterPrimaryKey,
                              requestUpperBoundPrimaryKey,
                              chunkSize)),
              "check remaining table work")
          .isPresent();
    } catch (RuntimeException ex) {
      rollbackFreshProgressIfNeeded(
          loaded.persisted(), false, jobId, schema.tableId().displayName(), ex);
      throw ex;
    }
  }

  /**
   * Persists completed-chunk progress before acknowledging the source transaction that carried the
   * matching high watermark. This ordering is load-bearing for restart safety: DBLog must never
   * advance the live checkpoint past work whose local recovery boundary was not yet durable.
   *
   * <p>Drain-only outcomes skip progress persistence (there was no chunk to complete) and jump
   * straight to the source-checkpoint advance.
   */
  @Override
  public void acknowledgeCompletedBatch(DumpWindowOutcome<TX> outcome) {
    Objects.requireNonNull(outcome, "outcome");
    if (outcome instanceof DumpWindowBatch<TX> batch) {
      dumpProgress.save(batch.activeProgress().completeChunk(batch.chunk()));
      executeSql(
          () -> {
            runtime.acknowledge(batch.checkpointTransaction());
            return null;
          },
          "acknowledge completed dump window batch");
      tap.onChunkCompleted(
          batch.chunk().jobId(),
          batch.chunk(),
          batch.emittedEvents(),
          batch.checkpointTransaction().checkpointPosition());
      return;
    }
    if (outcome instanceof DumpWindowDrainBatch<TX> drain) {
      // Empty-chunk drain still counts as "table is done for this request": mark progress at
      // the persisted upper bound so the next coordinator call short-circuits via
      // hasReachedRequestUpperBound. Without this, ALL_TABLES scheduling would re-open a drain
      // window on the same table forever (TABLE scope is protected by the finalRequestBatch
      // ack path, but this keeps both scopes idempotent on restart).
      String tableName = drain.tableId().displayName();
      dumpProgress
          .load(drain.jobId(), tableName)
          .map(DumpTableProgress::completeAtRequestUpperBound)
          .ifPresent(dumpProgress::save);
      executeSql(
          () -> {
            runtime.acknowledge(drain.checkpointTransaction());
            return null;
          },
          "acknowledge drained dump window");
      return;
    }
    throw new IllegalArgumentException(
        "Unsupported DumpWindowOutcome subtype: " + outcome.getClass().getName());
  }

  private LoadedDumpProgress loadProgress(String jobId, TableSchema schema) {
    String tableName = schema.tableId().displayName();
    Optional<DumpTableProgress> maybeProgress = dumpProgress.load(jobId, tableName);
    if (maybeProgress.isEmpty()) {
      return new LoadedDumpProgress(
          DumpTableProgress.initial(jobId, tableName, schema.fingerprint()), false);
    }
    return new LoadedDumpProgress(maybeProgress.orElseThrow(), true);
  }

  private void rollbackFreshProgressIfNeeded(
      boolean hadPersistedProgress,
      boolean activeProgressPersisted,
      String jobId,
      String tableName,
      Throwable cause) {
    if (hadPersistedProgress || activeProgressPersisted) {
      return;
    }
    try {
      dumpProgress.delete(jobId, tableName);
    } catch (RuntimeException rollbackFailure) {
      cause.addSuppressed(rollbackFailure);
    }
  }

  private DumpWindowBatch<TX> readUntilChunkComplete(
      DumpTableProgress activeProgress, Chunk chunk, WatermarkWindow window) {
    return readUntilChunkComplete(activeProgress, chunk, window, List.of());
  }

  private DumpWindowBatch<TX> readUntilChunkComplete(
      DumpTableProgress activeProgress,
      Chunk chunk,
      WatermarkWindow window,
      List<ChangeEvent> initialLogEvents) {
    WindowReconciler.ReconciliationSession reconciliationSession =
        windowReconciler.openSession(chunk, window, runtime.currentRunId());
    if (!initialLogEvents.isEmpty()) {
      reconciliationSession.apply(initialLogEvents);
      if (reconciliationSession.chunkCompleted()) {
        throw new IllegalStateException(
            adapterLabel
                + " completed dump/window reconciliation before a checkpoint transaction was available for "
                + chunk.schema().tableId().displayName());
      }
    }
    while (true) {
      TX transaction = awaitTransaction();
      reconciliationSession.apply(transaction.events());
      if (reconciliationSession.chunkCompleted()) {
        return new DumpWindowBatch<>(
            activeProgress,
            chunk,
            window,
            reconciliationSession.emittedEvents(),
            transaction);
      }
    }
  }

  /**
   * Drives a {@link WindowReconciler.ReconciliationSession} across an empty-chunk drain window.
   * The session handles LW/HW state transitions via the reconciler's strict state machine and
   * collects every non-watermark log event committed between LW and HW, so nothing is lost when
   * the chunk SELECT returned zero rows.
   */
  private DumpWindowDrainBatch<TX> readUntilDrainComplete(
      String jobId, TableSchema schema, WatermarkWindow window) {
    Chunk drainChunk = Chunk.drainOnly(jobId, schema);
    WindowReconciler.ReconciliationSession session =
        windowReconciler.openSession(drainChunk, window, runtime.currentRunId());
    while (true) {
      TX transaction = awaitTransaction();
      session.apply(transaction.events());
      if (session.chunkCompleted()) {
        return new DumpWindowDrainBatch<>(jobId, schema.tableId(), window, session.emittedEvents(), transaction);
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
            + " while coordinating a dump window");
  }

  private void sleepPollInterval() {
    try {
      Thread.sleep(pollInterval.toMillis());
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Interrupted while waiting for "
              + transactionLabel
              + "s during dump/window coordination",
          ex);
    }
  }

  private DumpTableProgress reconcileProgressSchema(
      DumpTableProgress progress, TableSchema currentSchema) {
    Objects.requireNonNull(progress, "progress");
    Objects.requireNonNull(currentSchema, "currentSchema");
    try {
      progress.requireCompatibleSchema(currentSchema);
      return progress;
    } catch (SchemaDriftException incompatibleProgress) {
      Optional<TableSchema> contractSchema =
          schemas.loadContractSchema(currentSchema.tableId().displayName());
      if (contractSchema.isEmpty()) {
        throw incompatibleProgress;
      }
      TableSchema reconciled =
          schemaPolicyEngine.reconcileStrict(
              contractSchema.orElseThrow(), currentSchema, "dump progress recovery");
      DumpTableProgress acceptedProgress = progress.withAcceptedSchema(reconciled);
      dumpProgress.save(acceptedProgress);
      return acceptedProgress;
    }
  }

  private Chunk acceptCompatibleChunk(TableSchema currentSchema, Chunk chunk) {
    Objects.requireNonNull(currentSchema, "currentSchema");
    Objects.requireNonNull(chunk, "chunk");
    TableSchema reconciled =
        schemaPolicyEngine.reconcileStrict(currentSchema, chunk.schema(), "dump chunk read");
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

  private static boolean hasReachedRequestUpperBound(
      DumpTableProgress progress, TableSchema schema) {
    if (progress.requestUpperBoundPrimaryKeyTuple() == null) {
      return true;
    }
    if (progress.lastCompletedPrimaryKeyTuple() == null) {
      return false;
    }
    if (progress
        .lastCompletedPrimaryKeyTuple()
        .equals(progress.requestUpperBoundPrimaryKeyTuple())) {
      return true;
    }
    return schema.canComparePrimaryKeyOrderInMemory()
        && schema.comparePrimaryKeyTuples(
                progress.lastCompletedPrimaryKeyTuple(),
                progress.requestUpperBoundPrimaryKeyTuple())
            > 0;
  }

  private record LoadedDumpProgress(DumpTableProgress progress, boolean persisted) {}

  @FunctionalInterface
  private interface SqlAction<T> {
    T run() throws Exception;
  }

  private <T> T executeSql(SqlAction<T> action, String label) {
    try {
      return action.run();
    } catch (RuntimeException ex) {
      throw ex; // typed runtime exceptions pass through unwrapped
    } catch (Exception ex) {
      throw executionFailure(label, ex);
    }
  }

  private CoreRequestExecutionException executionFailure(String label, Exception cause) {
    return new CoreRequestExecutionException(
        adapterLabel + " failed to " + label + " during dump window coordination", cause);
  }
}
