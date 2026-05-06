package io.github.aandreakis.dblog.core.reconcile;

import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.PrimaryKeyHash;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.model.WatermarkToken;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.tap.Tap;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Authoritative implementation of the DBLog watermark algorithm for one buffered {@link Chunk}
 * and one active {@link WatermarkWindow}.
 *
 * <p>The reconciler preserves log-event order while suppressing stale snapshot rows that collide
 * with same-table log events inside the watermark window. Snapshot rows that survive reconciliation
 * are emitted only when the matching high watermark arrives.
 *
 * <h2>State machine</h2>
 *
 * <p>Three semantic phases keyed off {@code lowSeen} / {@code highSeen}:
 *
 * <ul>
 *   <li><b>before low</b> — non-watermark events pass through immediately.
 *   <li><b>between low and high</b> — non-watermark events pass through in original order; when an
 *       event targets the <em>same table</em> as the chunk and the same {@link PrimaryKeyHash} as
 *       a buffered snapshot row, that snapshot row is removed from the chunk buffer. A collision
 *       suppresses at most one {@code SELECT}-origin refresh row. Changes for other tables inside
 *       the window never suppress chunk rows.
 *   <li><b>after high</b> — the remaining buffered chunk rows are emitted as {@code UPDATE} events
 *       with {@code captureOrigin=SELECT}. If the high watermark has not yet arrived, the chunk
 *       remains incomplete and the completed-chunk checkpoint must not advance.
 * </ul>
 *
 * <h2>Fail-closed watermark sequence rules</h2>
 *
 * <p>Any of the following throw {@link WatermarkSequenceException}:
 *
 * <ul>
 *   <li>a metadata-table change surfaced as anything other than operation {@code WATERMARK},
 *   <li>a watermark event with capture origin other than {@code LOG},
 *   <li>a watermark event that does not target the dedicated metadata table's singleton row
 *       ({@code id = 1}),
 *   <li>a watermark event missing {@code afterRow} or a non-blank {@code token},
 *   <li>low-after-high, duplicate low, high-before-low, or duplicate high for the active chunk,
 *   <li>an unexpected watermark token for the active window, except that
 *       {@link #openSession(Chunk, WatermarkWindow, String)} may drain stale retry/orphan tokens
 *       that carry a non-blank {@code run_id}.
 * </ul>
 *
 * <h2>Memory shape</h2>
 *
 * <p>The reconciler never caches a separate log window. It keeps the remaining chunk rows keyed by
 * {@link PrimaryKeyHash}, two booleans, and the emitted-event list.
 */
public final class WindowReconciler {
  private final Tap tap;

  public WindowReconciler(Tap tap) {
    this.tap = Objects.requireNonNull(tap, "tap");
  }

  public ReconciliationResult reconcile(Chunk chunk, WatermarkWindow window, List<ChangeEvent> logEvents) {
    return reconcile(chunk, window, logEvents, null);
  }

  public ReconciliationResult reconcile(
      Chunk chunk, WatermarkWindow window, List<ChangeEvent> logEvents, String currentRunId) {
    ReconciliationSession session = openSession(chunk, window, currentRunId);
    session.apply(logEvents);
    return session.snapshot();
  }

  public ReconciliationSession openSession(Chunk chunk, WatermarkWindow window) {
    return new ReconciliationSession(chunk, window, null, tap);
  }

  /**
   * Open a reconciliation session that drains stale watermark tokens carrying any run id. When
   * {@code currentRunId} is non-null, watermark events whose token does not match the active
   * window's low or high are silently dropped as long as they carry a non-blank {@link
   * WatermarkMetadata#RUN_ID_COLUMN}. This covers two recovery cases with one rule:
   *
   * <ul>
   *   <li>Same-session retries after a mid-window failure — the prior attempt's own low/high may
   *       already be replayed from the source log before the new attempt's tokens arrive.
   *   <li>Cross-restart orphans — a prior process left its low or high in {@code
   *       dblog_meta.watermarks}; the source log replays it to this process on resume.
   * </ul>
   *
   * <p>Foreign control rows are not active control signals for the current runtime; same-stream
   * interference detection belongs to the heartbeat path, not the watermark reconciler. Watermark
   * events with a missing or blank run id still fail closed — that is a writer-contract
   * violation, not a recoverable orphan.
   */
  public ReconciliationSession openSession(
      Chunk chunk, WatermarkWindow window, String currentRunId) {
    return new ReconciliationSession(chunk, window, currentRunId, tap);
  }

  private static void emitRemainingSelectedRows(
      Chunk chunk,
      Map<PrimaryKeyHash, ImmutableRowImage> remaining,
      List<ChangeEvent> emitted,
      SourcePosition sourcePosition) {
    TableSchema schema = chunk.schema();
    for (ImmutableRowImage snapshotRow : remaining.values()) {
      emitted.add(
          new ChangeEvent(
              schema.tableId(),
              OperationType.UPDATE,
              CaptureOrigin.SELECT,
              schema.primaryKeyRow(snapshotRow),
              null,
              snapshotRow,
              sourcePosition,
              null,
              chunk.jobId()));
    }
  }

  private static void requireLogWatermark(ChangeEvent event) {
    if (event.captureOrigin() != CaptureOrigin.LOG) {
      throw new WatermarkSequenceException(
          "watermark event must use LOG capture origin, but was " + event.captureOrigin());
    }
  }

  private static String watermarkToken(ChangeEvent event, TableId watermarkTableId) {
    if (!event.tableId().equals(watermarkTableId)) {
      throw new WatermarkSequenceException(
          "watermark event must use dedicated metadata table %s, but was %s"
              .formatted(watermarkTableId.displayName(), event.tableId().displayName()));
    }
    if (!isSingletonMetadataPrimaryKey(event.primaryKey())) {
      throw new WatermarkSequenceException(
          "watermark event must target exact singleton metadata row %s=%s"
              .formatted(WatermarkMetadata.PRIMARY_KEY_COLUMN, WatermarkMetadata.SINGLETON_ROW_ID));
    }
    if (event.afterRow() == null) {
      throw new WatermarkSequenceException(
          "watermark event is missing afterRow payload for token extraction");
    }
    Object tokenValue = event.afterRow().get(WatermarkMetadata.TOKEN_COLUMN);
    if (tokenValue == null) {
      throw new WatermarkSequenceException(
          "watermark event is missing non-blank token column '" + WatermarkMetadata.TOKEN_COLUMN + "'");
    }
    if (!(tokenValue instanceof String tokenString)) {
      throw new WatermarkSequenceException(
          "watermark token column must be a String, but was " + tokenValue.getClass().getName());
    }
    if (tokenString.isBlank()) {
      throw new WatermarkSequenceException(
          "watermark event is missing non-blank token column '" + WatermarkMetadata.TOKEN_COLUMN + "'");
    }
    return tokenString;
  }

  private static String extractRunId(ChangeEvent event) {
    if (event.afterRow() == null) {
      return null;
    }
    Object runIdValue = event.afterRow().get(WatermarkMetadata.RUN_ID_COLUMN);
    if (runIdValue == null) {
      return null;
    }
    if (!(runIdValue instanceof String runId)) {
      throw new WatermarkSequenceException(
          "watermark run_id column must be a String, but was " + runIdValue.getClass().getName());
    }
    return runId.isBlank() ? null : runId;
  }

  private static boolean isSingletonMetadataPrimaryKey(ImmutableRowImage primaryKey) {
    if (primaryKey.size() != 1 || !primaryKey.containsKey(WatermarkMetadata.PRIMARY_KEY_COLUMN)) {
      return false;
    }
    Object value = primaryKey.get(WatermarkMetadata.PRIMARY_KEY_COLUMN);
    if (value == null) {
      return false;
    }
    if (value instanceof Number number) {
      return number.longValue() == WatermarkMetadata.SINGLETON_ROW_ID
          && number.doubleValue() == WatermarkMetadata.SINGLETON_ROW_ID;
    }
    return Long.toString(WatermarkMetadata.SINGLETON_ROW_ID).equals(String.valueOf(value));
  }

  public static final class ReconciliationSession {
    private final Chunk chunk;
    private final WatermarkWindow window;
    private final TableSchema schema;
    private final TableId watermarkTableId;
    private final Map<PrimaryKeyHash, ImmutableRowImage> remainingSnapshotRows;
    private final List<ChangeEvent> emittedEvents;
    private final String currentRunId;
    private final Tap tap;

    private boolean lowSeen;
    private boolean highSeen;

    private ReconciliationSession(Chunk chunk, WatermarkWindow window, String currentRunId, Tap tap) {
      this.chunk = Objects.requireNonNull(chunk, "chunk");
      this.window = Objects.requireNonNull(window, "window");
      this.schema = chunk.schema();
      this.watermarkTableId = WatermarkMetadata.tableIdFor(schema.tableId().databaseName());
      this.remainingSnapshotRows = new LinkedHashMap<>(chunk.rowImagesByPrimaryKeyHashes());
      this.emittedEvents = new ArrayList<>(chunk.rowImages().size());
      this.currentRunId = currentRunId;
      this.tap = Objects.requireNonNull(tap, "tap");
    }

    public void apply(List<ChangeEvent> logEvents) {
      Objects.requireNonNull(logEvents, "logEvents");
      for (ChangeEvent event : logEvents) {
        try {
          apply(event);
        } catch (RuntimeException failure) {
          tap.onError(
              failure.getClass().getSimpleName(),
              failure.getMessage() == null ? "" : failure.getMessage(),
              Map.of(
                  "table", chunk.schema().tableId().displayName(),
                  "event_lsn",
                  event.sourcePosition() == null
                      ? ""
                      : event.sourcePosition().displayValue()));
          throw failure;
        }
      }
    }

    public boolean chunkCompleted() {
      return highSeen;
    }

    public List<ChangeEvent> emittedEvents() {
      return List.copyOf(emittedEvents);
    }

    public ReconciliationResult snapshot() {
      return new ReconciliationResult(
          List.copyOf(emittedEvents), snapshotRemainingAsMaps(), highSeen);
    }

    private Map<PrimaryKeyTuple, Map<String, Object>> snapshotRemainingAsMaps() {
      Map<PrimaryKeyTuple, Map<String, Object>> ordered = new LinkedHashMap<>(remainingSnapshotRows.size());
      for (ImmutableRowImage image : remainingSnapshotRows.values()) {
        ordered.put(schema.primaryKeyTupleFor(image), image.asMap());
      }
      return Map.copyOf(ordered);
    }

    private void apply(ChangeEvent event) {
      if (event.tableId().equals(watermarkTableId)
          && event.operationType() != OperationType.WATERMARK) {
        throw new WatermarkSequenceException(
            "metadata table changes must be surfaced as WATERMARK events: "
                + watermarkTableId.displayName());
      }

      if (event.operationType() == OperationType.WATERMARK) {
        requireLogWatermark(event);
        String token = watermarkToken(event, watermarkTableId);
        if (window.low().value().equals(token)) {
          if (highSeen) {
            throw new WatermarkSequenceException(
                "low watermark arrived after the high watermark for active chunk "
                    + chunk.tableName());
          }
          if (lowSeen) {
            throw new WatermarkSequenceException(
                "duplicate low watermark for active chunk " + chunk.tableName());
          }
          lowSeen = true;
          tap.onWatermarkReceived(
              Tap.WatermarkLevel.LOW, new WatermarkToken(token), event.sourcePosition());
          return;
        }
        if (window.high().value().equals(token)) {
          if (!lowSeen) {
            throw new WatermarkSequenceException(
                "high watermark arrived before the low watermark for active chunk "
                    + chunk.tableName());
          }
          if (highSeen) {
            throw new WatermarkSequenceException(
                "duplicate high watermark for active chunk " + chunk.tableName());
          }
          highSeen = true;
          tap.onWatermarkReceived(
              Tap.WatermarkLevel.HIGH, new WatermarkToken(token), event.sourcePosition());
          emitRemainingSelectedRows(chunk, remainingSnapshotRows, emittedEvents, event.sourcePosition());
          // Once emitted, the rows are represented in `emittedEvents` — dropping them here keeps
          // `snapshotRemainingAsMaps` cheap when snapshot() is called on a completed session.
          remainingSnapshotRows.clear();
          return;
        }
        if (currentRunId != null && extractRunId(event) != null) {
          // Stale watermark token carrying some run id — either this runtime's own (same-session
          // retry leftover) or a prior process's orphan that the source log is now replaying
          // (cross-restart). Both are recoverable; drain silently so the new window's tokens can
          // complete. A missing or blank run id falls through to fail closed because that is a
          // writer-contract violation (the metadata table enforces non-blank run_id on write).
          return;
        }
        throw new WatermarkSequenceException(
            "unexpected watermark token '%s' for active chunk %s"
                .formatted(token, chunk.tableName()));
      }

      if (!lowSeen || highSeen) {
        emittedEvents.add(event);
        return;
      }

      if (event.tableId().equals(schema.tableId())) {
        PrimaryKeyHash key = event.primaryKeyHash();
        if (key == null) {
          key = schema.primaryKeyHashFor(event.primaryKey());
        }
        ImmutableRowImage removed = remainingSnapshotRows.remove(key);
        if (removed != null) {
          tap.onChunkCollision(schema, removed, event);
        }
      }
      emittedEvents.add(event);
    }
  }
}
