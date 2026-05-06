package io.github.aandreakis.dblog.tap;

import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.model.WatermarkToken;
import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.request.DumpRequestState;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.util.List;
import java.util.Map;

/**
 * Educational observability channel for DBLog's watermark algorithm.
 *
 * <p>Call sites pass rich DBLog domain objects ({@link Chunk}, raw event lists, etc.) and
 * implementations perform their own derivation. That keeps the changes inside existing DBLog
 * classes to a single-line {@code tap.onX(...)} call at each event boundary. Dispatch between
 * an emitting implementation ({@link ActiveTap}) and a no-op one ({@link NoopTap}) happens once
 * at bean-construction time via Spring, not per-method — callers never branch on configuration.
 *
 * <p>The tap deliberately blocks the DBLog pump thread when the HTTP subscriber (the TUI) can't
 * keep up. That's the whole point — step-mode for the algorithm. Never enable in production.
 */
public interface Tap {

  /** Work-unit boundary: the pump is about to append a batch to the sink chain. */
  void onSinkBatchStart();

  /**
   * Work-unit complete: sink chain succeeded and local progress is persisted. Implementations may
   * block here to apply backpressure from a slow subscriber to the pump.
   */
  void onSinkBatchCommit();

  /**
   * One CDC row change made it through a sink delegate. Emitted per sink per event (fan-out by
   * the {@link TappingChangeEventSink} decorator).
   */
  void onSinkEvent(ChangeEvent event, String sinkName);

  /**
   * Raw transaction events as observed by the pump (both CDC streaming and chunked batch paths).
   * Implementations filter LOG-origin user-table rows internally and drop metadata events.
   */
  void onCdcBatch(List<ChangeEvent> events);

  /** DBLog wrote a LOW/HIGH watermark on {@code dblog_meta.watermarks}. */
  void onWatermarkWritten(WatermarkLevel level, WatermarkToken token);

  /** The reconciler observed the LOW/HIGH watermark token come back on the source change log. */
  void onWatermarkReceived(WatermarkLevel level, WatermarkToken token, SourcePosition lsn);

  /** A chunk SELECT finished inside the currently open watermark window. */
  void onChunkSelected(String requestId, Chunk chunk);

  /** An in-window CDC event removed a row from the active chunk's refresh buffer. */
  void onChunkCollision(TableSchema schema, ImmutableRowImage removedRow, ChangeEvent cause);

  /**
   * HIGH watermark observed, refresh rows emitted, chunk acknowledged. {@code emittedEvents} is
   * the reconciler's final event list for the chunk (used to count refresh {@code SELECT} rows
   * vs collisions). {@code hwLsn} is the high watermark's source position — the refresh rows
   * share it.
   */
  void onChunkCompleted(
      String requestId, Chunk chunk, List<ChangeEvent> emittedEvents, SourcePosition hwLsn);

  /** Buffered streaming checkpoint rolled forward. {@code reason} is the raw dispatcher reason. */
  void onCheckpointAdvanced(SourcePosition position, int bufferedEvents, String reason);

  /**
   * A dump request's durable state changed. {@code previous} is {@code null} on the first-ever
   * transition (no prior status row); {@code reason} is typically populated when
   * {@code current == FAILED}. {@code table} is {@code null} for {@link DumpScope#ALL_TABLES}.
   */
  void onRequestTransition(
      String requestId,
      DumpScope scope,
      TableId table,
      DumpRequestState previous,
      DumpRequestState current,
      String reason);

  /** A fail-closed runtime boundary was hit. */
  void onError(String exceptionClass, String message, Map<String, Object> context);

  enum WatermarkLevel {
    LOW,
    HIGH
  }
}
