package io.github.aandreakis.dblog.core.request;

import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.reconcile.WatermarkWindow;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import io.github.aandreakis.dblog.core.schema.SchemaDriftException;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.util.Objects;

/**
 * Durable per-table progress state for one dump request.
 *
 * <p>The model is intentionally chunk-boundary based. It persists the request-scoped upper bound,
 * the last completed primary key, and at most one active low/high watermark pair. It never
 * attempts to persist a partially reconciled in-window buffer; if a process stops mid-window, the
 * active chunk is retried from the last completed boundary.
 *
 * <p>The canonical constructor fail-closes impossible field combinations so restart logic does not
 * guess through corrupted partial state.
 */
public record DumpTableProgress(
    String jobId,
    String tableName,
    String schemaFingerprint,
    PrimaryKeyTuple requestUpperBoundPrimaryKeyTuple,
    PrimaryKeyTuple lastCompletedPrimaryKeyTuple,
    PrimaryKeyTuple activeChunkStartAfterTuple,
    String activeLowWatermark,
    String activeHighWatermark,
    boolean chunkCompleted) {

  public DumpTableProgress {
    Objects.requireNonNull(jobId, "jobId");
    Objects.requireNonNull(tableName, "tableName");
    Objects.requireNonNull(schemaFingerprint, "schemaFingerprint");

    boolean hasActiveStartAfter = activeChunkStartAfterTuple != null;
    boolean hasActiveLow = activeLowWatermark != null;
    boolean hasActiveHigh = activeHighWatermark != null;

    if (hasActiveLow != hasActiveHigh) {
      throw new DumpStateCorruptionException(
          "partial dump state is corrupted: both active low and high watermark tokens must be present or absent");
    }
    if (!hasActiveLow && hasActiveStartAfter) {
      throw new DumpStateCorruptionException(
          "partial dump state is corrupted: activeChunkStartAfter cannot exist without active watermark tokens");
    }
    if (hasActiveStartAfter && requestUpperBoundPrimaryKeyTuple == null) {
      throw new DumpStateCorruptionException(
          "partial dump state is corrupted: activeChunkStartAfter cannot exist without requestUpperBoundPrimaryKey");
    }
    if (hasActiveLow && activeLowWatermark.equals(activeHighWatermark)) {
      throw new DumpStateCorruptionException(
          "partial dump state is corrupted: active low and high watermark tokens must be distinct");
    }
    if (hasActiveLow && !Objects.equals(activeChunkStartAfterTuple, lastCompletedPrimaryKeyTuple)) {
      throw new DumpStateCorruptionException(
          "partial dump state is corrupted: activeChunkStartAfter must match lastCompletedPrimaryKey for in-flight chunk retry");
    }
    if (chunkCompleted && lastCompletedPrimaryKeyTuple == null) {
      throw new DumpStateCorruptionException(
          "completed dump state is corrupted: lastCompletedPrimaryKey is missing");
    }
    if (chunkCompleted && (hasActiveStartAfter || hasActiveLow || hasActiveHigh)) {
      throw new DumpStateCorruptionException(
          "completed dump state is corrupted: completed chunks must not retain active chunk state");
    }
    if (lastCompletedPrimaryKeyTuple != null && requestUpperBoundPrimaryKeyTuple == null) {
      throw new DumpStateCorruptionException(
          "completed dump state is corrupted: lastCompletedPrimaryKey requires requestUpperBoundPrimaryKey");
    }
  }

  public DumpTableProgress beginChunk(Chunk chunk, WatermarkWindow window) {
    verifyChunkMatchesProgress(chunk);
    Objects.requireNonNull(window, "window");
    return new DumpTableProgress(
        jobId,
        tableName,
        schemaFingerprint,
        requestUpperBoundPrimaryKeyTuple,
        lastCompletedPrimaryKeyTuple,
        chunk.startAfterPrimaryKeyTuple(),
        window.low().value(),
        window.high().value(),
        false);
  }

  public DumpTableProgress completeChunk(Chunk chunk) {
    verifyChunkMatchesProgress(chunk);
    return new DumpTableProgress(
        jobId,
        tableName,
        schemaFingerprint,
        requestUpperBoundPrimaryKeyTuple,
        chunk.lastPrimaryKeyTuple(),
        null,
        null,
        null,
        true);
  }

  /**
   * Marks this request's dump work for the table as terminated at the persisted upper bound
   * without requiring a concrete chunk — used by the empty-chunk drain path, which reconciles
   * the watermark window without any chunk rows but still needs to signal "no remaining dump
   * work" so subsequent coordinator iterations short-circuit via {@code hasReachedRequestUpperBound}.
   *
   * <p>If no upper bound was captured yet the state is returned unchanged; the caller is
   * responsible for invoking this only after the upper-bound capture step.
   */
  public DumpTableProgress completeAtRequestUpperBound() {
    if (requestUpperBoundPrimaryKeyTuple == null) {
      return this;
    }
    return new DumpTableProgress(
        jobId,
        tableName,
        schemaFingerprint,
        requestUpperBoundPrimaryKeyTuple,
        requestUpperBoundPrimaryKeyTuple,
        null,
        null,
        null,
        true);
  }

  /**
   * Drops any in-flight watermark state and resumes from the last completed chunk boundary. This
   * is the only supported recovery path for a partially processed chunk.
   */
  public DumpTableProgress restartFromLastCompletedBoundary() {
    return new DumpTableProgress(
        jobId,
        tableName,
        schemaFingerprint,
        requestUpperBoundPrimaryKeyTuple,
        lastCompletedPrimaryKeyTuple,
        null,
        null,
        null,
        false);
  }

  public DumpTableProgress captureRequestUpperBound(PrimaryKeyTuple requestUpperBoundPrimaryKeyTuple) {
    if (lastCompletedPrimaryKeyTuple != null || hasActiveChunk()) {
      throw new DumpStateCorruptionException(
          "requestUpperBoundPrimaryKey must be captured before chunk progress is recorded");
    }
    return new DumpTableProgress(
        jobId,
        tableName,
        schemaFingerprint,
        requestUpperBoundPrimaryKeyTuple,
        null,
        null,
        null,
        null,
        false);
  }

  public DumpTableProgress captureRequestUpperBound(
      TableSchema schema, String requestUpperBoundPrimaryKey) {
    Objects.requireNonNull(schema, "schema");
    return captureRequestUpperBound(
        requestUpperBoundPrimaryKey == null
            ? null
            : schema.primaryKeyTupleFromLiteral(requestUpperBoundPrimaryKey));
  }

  public boolean hasActiveChunk() {
    return activeLowWatermark != null;
  }

  public PrimaryKeyTuple resumeAfterPrimaryKeyTuple() {
    return lastCompletedPrimaryKeyTuple;
  }

  public void requireCompatibleSchema(TableSchema currentSchema) {
    Objects.requireNonNull(currentSchema, "currentSchema");
    String currentTableName = currentSchema.tableId().displayName();
    if (!tableName.equals(currentTableName)) {
      throw new SchemaDriftException(
          "dump progress table changed from %s to %s".formatted(tableName, currentTableName));
    }
    if (!matchesStoredSchemaFingerprint(currentSchema, schemaFingerprint)) {
      throw new SchemaDriftException(
          "schema fingerprint changed across dump boundary for table %s: expected %s but found %s"
              .formatted(tableName, schemaFingerprint, currentSchema.fingerprint()));
    }
  }

  public DumpTableProgress withAcceptedSchema(TableSchema acceptedSchema) {
    Objects.requireNonNull(acceptedSchema, "acceptedSchema");
    String currentTableName = acceptedSchema.tableId().displayName();
    if (!tableName.equals(currentTableName)) {
      throw new SchemaDriftException(
          "dump progress table changed from %s to %s".formatted(tableName, currentTableName));
    }
    if (schemaFingerprint.equals(acceptedSchema.fingerprint())) {
      return this;
    }
    return new DumpTableProgress(
        jobId,
        tableName,
        acceptedSchema.fingerprint(),
        requestUpperBoundPrimaryKeyTuple,
        lastCompletedPrimaryKeyTuple,
        activeChunkStartAfterTuple,
        activeLowWatermark,
        activeHighWatermark,
        chunkCompleted);
  }

  public static DumpTableProgress initial(String jobId, String tableName, String schemaFingerprint) {
    return new DumpTableProgress(
        jobId, tableName, schemaFingerprint, null, null, null, null, null, false);
  }

  private void verifyChunkMatchesProgress(Chunk chunk) {
    Objects.requireNonNull(chunk, "chunk");
    if (!jobId.equals(chunk.jobId())) {
      throw new IllegalArgumentException(
          "chunk jobId %s does not match dump progress jobId %s".formatted(chunk.jobId(), jobId));
    }
    if (!tableName.equals(chunk.tableName())) {
      throw new IllegalArgumentException(
          "chunk tableName %s does not match dump progress tableName %s"
              .formatted(chunk.tableName(), tableName));
    }
    if (!matchesStoredSchemaFingerprint(chunk.schema(), schemaFingerprint)) {
      throw new SchemaDriftException(
          "chunk schema fingerprint changed for table %s: expected %s but found %s"
              .formatted(tableName, schemaFingerprint, chunk.schema().fingerprint()));
    }
    if (requestUpperBoundPrimaryKeyTuple == null) {
      throw new DumpStateCorruptionException(
          "chunk verification requires a persisted requestUpperBoundPrimaryKey for table "
              + tableName);
    }
    PrimaryKeyTuple normalizedUpperBound =
        chunk.schema().primaryKeyTupleFor(chunk.schema().primaryKeyRowFromTuple(requestUpperBoundPrimaryKeyTuple));
    if (chunk.startAfterPrimaryKeyTuple() != null
        && chunk.schema().comparePrimaryKeyTuples(chunk.startAfterPrimaryKeyTuple(), normalizedUpperBound)
            >= 0) {
      throw new DumpStateCorruptionException(
          "chunk startAfterPrimaryKey must remain below requestUpperBoundPrimaryKey for table "
              + tableName);
    }
    if (chunk.schema().comparePrimaryKeyTuples(chunk.lastPrimaryKeyTuple(), normalizedUpperBound) > 0) {
      throw new DumpStateCorruptionException(
          "chunk lastPrimaryKey exceeded requestUpperBoundPrimaryKey for table " + tableName);
    }
  }

  private static boolean matchesStoredSchemaFingerprint(
      TableSchema currentSchema, String storedFingerprint) {
    return storedFingerprint.equals(currentSchema.primaryKeyFingerprint())
        || storedFingerprint.equals(currentSchema.fingerprint());
  }

  public String requestUpperBoundPrimaryKey() {
    return requestUpperBoundPrimaryKeyTuple == null ? null : requestUpperBoundPrimaryKeyTuple.literal();
  }

  public String lastCompletedPrimaryKey() {
    return lastCompletedPrimaryKeyTuple == null ? null : lastCompletedPrimaryKeyTuple.literal();
  }

  public String activeChunkStartAfter() {
    return activeChunkStartAfterTuple == null ? null : activeChunkStartAfterTuple.literal();
  }

  public String resumeAfterPrimaryKey() {
    return lastCompletedPrimaryKey();
  }
}
