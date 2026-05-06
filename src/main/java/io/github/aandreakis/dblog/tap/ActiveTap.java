package io.github.aandreakis.dblog.tap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.model.WatermarkToken;
import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.request.DumpRequestState;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.tap.generated.CdcEvent;
import io.github.aandreakis.dblog.tap.generated.CheckpointAdvanced;
import io.github.aandreakis.dblog.tap.generated.ChunkCollision;
import io.github.aandreakis.dblog.tap.generated.ChunkCompleted;
import io.github.aandreakis.dblog.tap.generated.ChunkSelected;
import io.github.aandreakis.dblog.tap.generated.ErrorEvent;
import io.github.aandreakis.dblog.tap.generated.RequestTransition;
import io.github.aandreakis.dblog.tap.generated.SinkEvent;
import io.github.aandreakis.dblog.tap.generated.StreamHeartbeat;
import io.github.aandreakis.dblog.tap.generated.StreamResumed;
import io.github.aandreakis.dblog.tap.generated.StreamStandby;
import io.github.aandreakis.dblog.tap.generated.WatermarkReceived;
import io.github.aandreakis.dblog.tap.generated.WatermarkWritten;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The real {@link Tap}. Wired by Spring when {@code dblog.tap.enabled=true}; otherwise
 * {@link NoopTap#INSTANCE} takes its place. Constructed once per boot; callers pass it around via
 * constructor arguments.
 *
 * <p>Each {@code onX} method derives its event payload from the rich DBLog domain object it
 * receives (so existing DBLog classes stay untouched except for the one-line {@code tap.onX(...)}
 * call), populates a typed event POJO generated from the JSON schemas under
 * {@code docs/schema/events/}, buffers its JSON line, and flushes the per-work-unit buffer to a
 * bounded queue at {@link #onSinkBatchCommit()}. When the queue is full,
 * {@link TapQueue#put(byte[])} blocks the pump thread until the HTTP subscriber drains — that is
 * the intended educational backpressure mechanism. Never enable in production.
 *
 * <p>Single-producer invariant: all {@code onX} methods are called from the DBLog pump thread.
 * The queue is drained by the HTTP streaming thread.
 */
public final class ActiveTap implements Tap {
  private static final Logger log = LoggerFactory.getLogger(ActiveTap.class);
  private static final ObjectMapper MAPPER = TapObjectMapper.create();
  private static final long TAP_SCHEMA_VERSION = 1L;

  private final TapConfig config;
  private final String runId;
  private final String sourceId;
  private final TapQueue queue;
  private final long heartbeatIntervalNanos;
  private final long standbyThresholdNanos;

  // All mutable fields below are only touched from the single pump thread
  // (see "Single-producer invariant" in the class javadoc).
  private final List<byte[]> pendingBuffer = new ArrayList<>(64);
  private long sequence = 0L;
  private int chunkIdCounter = 0;
  private int currentChunkId = -1;
  private long lwWrittenAtNanos = -1L;
  private long hwWrittenAtNanos = -1L;
  private long lastHeartbeatNanos;

  public ActiveTap(TapConfig config, String runId, String sourceId) {
    this.config = Objects.requireNonNull(config, "config");
    this.runId = requireNonBlank(runId, "runId");
    this.sourceId = requireNonBlank(sourceId, "sourceId");
    this.heartbeatIntervalNanos = config.getHeartbeatInterval().toNanos();
    this.standbyThresholdNanos =
        Math.multiplyExact((long) config.getStandbyThresholdMs(), 1_000_000L);
    this.lastHeartbeatNanos = System.nanoTime();
    this.queue = new TapQueue(config.getQueueCapacity());
    log.warn(
        "DBLog tap is enabled (run_id={}, source_id={}). This is an educational observer that"
            + " BLOCKS the pump when a slow subscriber is attached — never enable in production.",
        runId,
        sourceId);
  }

  public String runId() {
    return runId;
  }

  public String sourceId() {
    return sourceId;
  }

  public TapConfig config() {
    return config;
  }

  TapQueue queue() {
    return queue;
  }

  long heartbeatIntervalNanos() {
    return heartbeatIntervalNanos;
  }

  long standbyThresholdNanos() {
    return standbyThresholdNanos;
  }

  // --- Tap interface ---------------------------------------------------------

  @Override
  public void onSinkBatchStart() {
    // The dump coordinator emits onWatermarkWritten / onChunkSelected / onWatermarkReceived /
    // onChunkCollision before the pump calls appendThroughEventSink (which invokes this
    // method). A plain clear() here drops those events; flushPendingBuffer() delivers them in
    // seq order and then clears. Matches the flush-before-write pattern used by
    // onRequestTransition and onError.
    flushPendingBuffer();
  }

  @Override
  public void onSinkBatchCommit() {
    if (pendingBuffer.isEmpty()) {
      return;
    }
    try {
      for (byte[] line : pendingBuffer) {
        queue.put(line);
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    } finally {
      pendingBuffer.clear();
    }
    maybeEmitHeartbeat();
  }

  @Override
  public void onSinkEvent(ChangeEvent event, String sinkName) {
    if (event == null) {
      return;
    }
    OperationType operation = event.operationType();
    if (operation == OperationType.WATERMARK || operation == OperationType.HEARTBEAT) {
      return;
    }
    // `sink_name` is `required` in sink-event.schema.json. The only wrapper
    // calling this method (`TappingChangeEventSink`) already rejects null /
    // blank names at construction; keep that invariant tight here so a
    // future caller that skips the wrapper can't put an empty string on
    // the wire. Fail-closed rather than emit a schema-violating event.
    Objects.requireNonNull(sinkName, "sinkName");
    if (sinkName.isBlank()) {
      throw new IllegalArgumentException("sinkName must be non-blank");
    }
    SinkEvent payload =
        new SinkEvent.SinkEventBuilder(
                TAP_SCHEMA_VERSION,
                Instant.now(),
                runId,
                sourceId,
                SinkEvent.Kind.SINK_EVENT,
                sourcePositionDisplay(event.sourcePosition()),
                SinkEvent.Op.fromValue(operation.name()),
                tableDisplay(event.tableId()),
                primaryKeyLiteral(event.primaryKey()),
                SinkEvent.Origin.fromValue(event.captureOrigin().name()),
                sinkName)
            .withSeq(++sequence)
            .withTxId(event.transactionId())
            .withDumpId(event.dumpId())
            .withChunkId(currentChunkId >= 0 ? (long) currentChunkId : null)
            .build();
    bufferEnvelope(payload);
  }

  @Override
  public void onCdcBatch(List<ChangeEvent> events) {
    if (events == null || events.isEmpty()) {
      return;
    }
    for (ChangeEvent event : events) {
      if (event.captureOrigin() != CaptureOrigin.LOG) {
        continue;
      }
      OperationType operation = event.operationType();
      if (operation == OperationType.WATERMARK || operation == OperationType.HEARTBEAT) {
        continue;
      }
      CdcEvent payload =
          new CdcEvent.CdcEventBuilder(
                  TAP_SCHEMA_VERSION,
                  Instant.now(),
                  runId,
                  sourceId,
                  CdcEvent.Kind.CDC,
                  sourcePositionDisplay(event.sourcePosition()),
                  CdcEvent.Op.fromValue(operation.name()),
                  tableDisplay(event.tableId()),
                  primaryKeyLiteral(event.primaryKey()))
              .withSeq(++sequence)
              .withTxId(event.transactionId())
              .build();
      bufferEnvelope(payload);
    }
  }

  @Override
  public void onWatermarkWritten(WatermarkLevel level, WatermarkToken token) {
    if (level == null || token == null) {
      return;
    }
    if (level == WatermarkLevel.LOW) {
      currentChunkId = ++chunkIdCounter;
      lwWrittenAtNanos = System.nanoTime();
    } else {
      hwWrittenAtNanos = System.nanoTime();
    }
    WatermarkWritten payload =
        new WatermarkWritten.WatermarkWrittenBuilder(
                TAP_SCHEMA_VERSION,
                Instant.now(),
                runId,
                sourceId,
                WatermarkWritten.Kind.WATERMARK_WRITTEN,
                WatermarkWritten.Level.fromValue(level.name()),
                token.value(),
                (long) currentChunkId)
            .withSeq(++sequence)
            .build();
    bufferEnvelope(payload);
  }

  @Override
  public void onWatermarkReceived(WatermarkLevel level, WatermarkToken token, SourcePosition lsn) {
    if (level == null || token == null) {
      return;
    }
    // `chunk_id` is required >= 0 in the schema. In the normal control flow
    // (LW-written → LW-received → HW-written → HW-received → chunk.completed)
    // `currentChunkId` is always the in-flight chunk when this fires, but a
    // bug or reorder in a future caller could land here with -1. Dropping the
    // event is preferable to emitting a schema-violating `-1` that every
    // downstream consumer would reject.
    if (currentChunkId < 0) {
      log.warn(
          "onWatermarkReceived fired with no active chunk (level={}, token={}); dropping.",
          level,
          token.value());
      return;
    }
    long writtenAtNanos = level == WatermarkLevel.LOW ? lwWrittenAtNanos : hwWrittenAtNanos;
    WatermarkReceived payload =
        new WatermarkReceived.WatermarkReceivedBuilder(
                TAP_SCHEMA_VERSION,
                Instant.now(),
                runId,
                sourceId,
                WatermarkReceived.Kind.WATERMARK_RECEIVED,
                WatermarkReceived.Level.fromValue(level.name()),
                token.value(),
                (long) currentChunkId,
                sourcePositionDisplay(lsn))
            .withSeq(++sequence)
            .withLatencyMs(
                writtenAtNanos > 0L ? (System.nanoTime() - writtenAtNanos) / 1_000_000L : null)
            .build();
    bufferEnvelope(payload);
  }

  @Override
  public void onChunkSelected(String requestId, Chunk chunk) {
    if (chunk == null) {
      return;
    }
    TableSchema schema = chunk.schema();
    List<PrimaryKeyTuple> tuples = chunk.rowPrimaryKeyTuples();
    String pkMin = tuples.isEmpty() ? null : schema.primaryKeyLiteralFor(tuples.get(0));
    String pkMax =
        chunk.lastPrimaryKeyTuple() == null
            ? null
            : schema.primaryKeyLiteralFor(chunk.lastPrimaryKeyTuple());
    String startAfterPk =
        chunk.startAfterPrimaryKeyTuple() == null
            ? null
            : schema.primaryKeyLiteralFor(chunk.startAfterPrimaryKeyTuple());
    ChunkSelected payload =
        new ChunkSelected.ChunkSelectedBuilder(
                TAP_SCHEMA_VERSION,
                Instant.now(),
                runId,
                sourceId,
                ChunkSelected.Kind.CHUNK_SELECTED,
                (long) currentChunkId,
                requestId,
                chunk.jobId(),
                tableDisplay(schema.tableId()),
                ChunkSelected.Mode.RANGE,
                (long) chunk.rowImages().size(),
                chunk.finalChunk(),
                schema.fingerprint())
            .withSeq(++sequence)
            .withPkMin(pkMin)
            .withPkMax(pkMax)
            .withStartAfterPk(startAfterPk)
            .build();
    bufferEnvelope(payload);
  }

  @Override
  public void onChunkCollision(TableSchema schema, ImmutableRowImage removedRow, ChangeEvent cause) {
    if (removedRow == null || cause == null) {
      return;
    }
    // Use the schema's canonical literal routine so the pk we emit here matches the shape
    // produced by TableSchema.primaryKeyLiteralFor — same form the core uses for collision
    // matching. Consumers correlating chunk.collision with the triggering cdc / sink.event
    // must see identical pk strings across all three.
    String excludedPk =
        schema == null ? null : schema.primaryKeyLiteralFor(removedRow.asMap());
    ChunkCollision payload =
        new ChunkCollision.ChunkCollisionBuilder(
                TAP_SCHEMA_VERSION,
                Instant.now(),
                runId,
                sourceId,
                ChunkCollision.Kind.CHUNK_COLLISION,
                (long) currentChunkId,
                sourcePositionDisplay(cause.sourcePosition()),
                ChunkCollision.CauseOp.fromValue(cause.operationType().name()))
            .withSeq(++sequence)
            .withExcludedPk(excludedPk)
            .withCauseTxId(cause.transactionId())
            .build();
    bufferEnvelope(payload);
  }

  @Override
  public void onChunkCompleted(
      String requestId, Chunk chunk, List<ChangeEvent> emittedEvents, SourcePosition hwLsn) {
    if (chunk == null || emittedEvents == null) {
      return;
    }
    int selectRowCount = 0;
    for (ChangeEvent event : emittedEvents) {
      if (event.captureOrigin() == CaptureOrigin.SELECT) {
        selectRowCount++;
      }
    }
    int excluded = chunk.rowImages().size() - selectRowCount;
    long durationNanos = lwWrittenAtNanos > 0L ? System.nanoTime() - lwWrittenAtNanos : 0L;
    ChunkCompleted payload =
        new ChunkCompleted.ChunkCompletedBuilder(
                TAP_SCHEMA_VERSION,
                Instant.now(),
                runId,
                sourceId,
                ChunkCompleted.Kind.CHUNK_COMPLETED,
                (long) currentChunkId,
                requestId,
                tableDisplay(chunk.schema().tableId()),
                (long) selectRowCount,
                (long) excluded,
                sourcePositionDisplay(hwLsn),
                durationNanos / 1_000_000L,
                chunk.finalChunk())
            .withSeq(++sequence)
            .withLastPk(chunk.lastPrimaryKey())
            .build();
    bufferEnvelope(payload);
    currentChunkId = -1;
    lwWrittenAtNanos = -1L;
    hwWrittenAtNanos = -1L;
  }

  @Override
  public void onCheckpointAdvanced(SourcePosition position, int bufferedEvents, String reason) {
    CheckpointAdvanced payload =
        new CheckpointAdvanced.CheckpointAdvancedBuilder(
                TAP_SCHEMA_VERSION,
                Instant.now(),
                runId,
                sourceId,
                CheckpointAdvanced.Kind.CHECKPOINT_ADVANCED,
                sourcePositionDisplay(position),
                (long) bufferedEvents,
                normaliseCheckpointReason(reason))
            .withSeq(++sequence)
            .build();
    bufferEnvelope(payload);
  }

  @Override
  public void onRequestTransition(
      String requestId,
      DumpScope scope,
      TableId table,
      DumpRequestState previous,
      DumpRequestState current,
      String reason) {
    if (requestId == null || scope == null || current == null) {
      return;
    }
    RequestTransition payload =
        new RequestTransition.RequestTransitionBuilder(
                TAP_SCHEMA_VERSION,
                Instant.now(),
                runId,
                sourceId,
                RequestTransition.Kind.REQUEST_TRANSITION,
                requestId,
                RequestTransition.Scope.fromValue(scope.name()),
                RequestTransition.State.fromValue(current.name()))
            .withSeq(++sequence)
            .withTable(table == null ? null : tableDisplay(table))
            .withPrevState(
                previous == null ? null : RequestTransition.PrevState.fromValue(previous.name()))
            .withReason(reason == null || reason.isBlank() ? null : reason)
            .build();
    // Transitions may fire outside a sink batch (e.g. when a request is failed during schema
    // validation before any batch is scheduled). Bypass the per-work-unit buffer so the event
    // reaches the subscriber without waiting for an onSinkBatchCommit that may never come.
    //
    // Flush any buffered events FIRST so the monotonic seq order seen by the subscriber matches
    // assignment order — otherwise a transition firing mid-batch would leapfrog its pending
    // predecessors. Mirrors the onError contract below.
    flushPendingBuffer();
    writeEnvelopeDirect(payload);
  }

  @Override
  public void onError(String exceptionClass, String message, Map<String, Object> context) {
    // Fail-closed paths typically don't reach {@link #onSinkBatchCommit()}, so flush whatever
    // context the pump already buffered for this work unit before writing the error — the
    // subscriber sees the events that led up to the failure, and then the error itself.
    flushPendingBuffer();
    // jsonschema2pojo generates raw-typed fluent builders (see generated
    // ErrorEvent.ErrorEventBuilderBase). withContext takes Map<String,Object>,
    // so the call-on-raw-type trips unchecked. Narrow suppression.
    @SuppressWarnings("unchecked")
    ErrorEvent payload =
        new ErrorEvent.ErrorEventBuilder(
                TAP_SCHEMA_VERSION,
                Instant.now(),
                runId,
                sourceId,
                ErrorEvent.Kind.ERROR,
                exceptionClass == null ? "UnknownException" : exceptionClass,
                message == null ? "" : message)
            .withSeq(++sequence)
            .withContext(context == null || context.isEmpty() ? null : context)
            .build();
    writeEnvelopeDirect(payload);
  }

  private void flushPendingBuffer() {
    if (pendingBuffer.isEmpty()) {
      return;
    }
    try {
      for (byte[] line : pendingBuffer) {
        queue.put(line);
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    } finally {
      pendingBuffer.clear();
    }
  }

  // --- Out-of-band events written directly by the IO thread ------------------

  byte[] buildStandbyPayload(long queueFullMillis) {
    StreamStandby payload =
        new StreamStandby.StreamStandbyBuilder(
                TAP_SCHEMA_VERSION,
                Instant.now(),
                runId,
                sourceId,
                StreamStandby.Kind.STREAM_STANDBY,
                queueFullMillis,
                (long) queue.capacity(),
                StreamStandby.Reason.QUEUE_FULL,
                "DBLog is on standby — waiting for the reader to advance")
            .build();
    return serialize(payload);
  }

  byte[] buildResumedPayload(long standbyTotalMillis) {
    StreamResumed payload =
        new StreamResumed.StreamResumedBuilder(
                TAP_SCHEMA_VERSION,
                Instant.now(),
                runId,
                sourceId,
                StreamResumed.Kind.STREAM_RESUMED,
                standbyTotalMillis)
            .build();
    return serialize(payload);
  }

  // --- Internals -------------------------------------------------------------

  private void maybeEmitHeartbeat() {
    long now = System.nanoTime();
    if (now - lastHeartbeatNanos < heartbeatIntervalNanos) {
      return;
    }
    lastHeartbeatNanos = now;
    StreamHeartbeat payload =
        new StreamHeartbeat.StreamHeartbeatBuilder(
                TAP_SCHEMA_VERSION,
                Instant.now(),
                runId,
                sourceId,
                StreamHeartbeat.Kind.STREAM_HEARTBEAT,
                (long) queue.depth(),
                (long) queue.capacity())
            .withSeq(++sequence)
            .build();
    byte[] line = serialize(payload);
    // heartbeat drops on contention rather than blocking the pump; it's informational.
    queue.offer(line);
  }

  private void bufferEnvelope(Object payload) {
    pendingBuffer.add(serialize(payload));
  }

  private void writeEnvelopeDirect(Object payload) {
    byte[] line = serialize(payload);
    try {
      queue.put(line);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private static byte[] serialize(Object payload) {
    try {
      byte[] body = MAPPER.writeValueAsBytes(payload);
      byte[] line = new byte[body.length + 1];
      System.arraycopy(body, 0, line, 0, body.length);
      line[body.length] = '\n';
      return line;
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize tap event", e);
    }
  }

  private static String sourcePositionDisplay(SourcePosition position) {
    return position == null ? null : position.displayValue();
  }

  private static String tableDisplay(TableId tableId) {
    return tableId == null ? null : tableId.displayName();
  }

  /**
   * Best-effort canonical primary-key literal when no {@link TableSchema} is available (the
   * {@code cdc} / {@code sink.event} paths, which only receive the {@link ChangeEvent}).
   *
   * <p>Values arrive already normalised by {@code NeutralValueNormalizer}, so the canonical
   * output mostly matches {@link TableSchema#primaryKeyLiteralFor(java.util.Map)} — this helper
   * covers the two cases where plain {@code String.valueOf} would diverge from the repository's
   * canonical primary-key rendering rules:
   *
   * <ul>
   *   <li>{@code byte[]} → lowercase hexadecimal (otherwise {@code [B@abcdef}).
   *   <li>{@code BigDecimal} → {@code stripTrailingZeros().toPlainString()} with a bare {@code "0"}
   *       for zero (otherwise trailing zeros would slip through).
   * </ul>
   *
   * Composite keys follow the same {@code {col=val,col=val}} shape and escape the four reserved
   * characters ({@code \ , = { }}), matching the escape table in
   * {@code TableSchema#escapePrimaryKeyLiteralComponent}. The iteration order is the
   * {@code ImmutableRowImage#asMap()} order, which the adapter populates in primary-key-column
   * order.
   *
   * <p>Consumers that want bit-for-bit-identical canonicalisation across all tap event kinds can
   * rely on {@code chunk.collision}'s {@code excluded_pk} field, which uses the schema-backed
   * {@link TableSchema#primaryKeyLiteralFor(java.util.Map)} path.
   */
  private static String primaryKeyLiteral(ImmutableRowImage primaryKey) {
    if (primaryKey == null || primaryKey.isEmpty()) {
      return null;
    }
    if (primaryKey.size() == 1) {
      return canonicalValueLiteral(primaryKey.valueAt(0));
    }
    Map<String, Object> asMap = primaryKey.asMap();
    StringBuilder builder = new StringBuilder().append('{');
    boolean first = true;
    for (Map.Entry<String, Object> entry : asMap.entrySet()) {
      if (!first) {
        builder.append(',');
      }
      first = false;
      builder.append(escapeLiteralComponent(entry.getKey()));
      builder.append('=');
      builder.append(escapeLiteralComponent(canonicalValueLiteral(entry.getValue())));
    }
    return builder.append('}').toString();
  }

  private static String canonicalValueLiteral(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof byte[] bytes) {
      StringBuilder hex = new StringBuilder(bytes.length * 2);
      for (byte b : bytes) {
        hex.append(Character.forDigit((b >> 4) & 0xF, 16));
        hex.append(Character.forDigit(b & 0xF, 16));
      }
      return hex.toString();
    }
    if (value instanceof java.math.BigDecimal decimal) {
      java.math.BigDecimal stripped = decimal.stripTrailingZeros();
      return stripped.signum() == 0 ? "0" : stripped.toPlainString();
    }
    return String.valueOf(value);
  }

  private static String escapeLiteralComponent(String value) {
    if (value == null) {
      return "";
    }
    StringBuilder out = new StringBuilder(value.length());
    for (int index = 0; index < value.length(); index++) {
      char ch = value.charAt(index);
      if (ch == '\\' || ch == ',' || ch == '=' || ch == '{' || ch == '}') {
        out.append('\\');
      }
      out.append(ch);
    }
    return out.toString();
  }

  /**
   * Maps the checkpoint-dispatcher's caller-supplied reason string onto the tap spec's
   * {@code count | time | bytes | boundary} vocabulary. The canonical mapping is documented in
   * {@code docs/CONTROL_PLANE.md §5.4 "Checkpoint reason vocabulary"} and mirrored here:
   *
   * <table>
   *   <caption>Checkpoint-reason normalisation</caption>
   *   <tr><th>BufferedCheckpointDispatcher reason</th><th>tap {@code reason} field</th></tr>
   *   <tr><td>{@code null}</td><td>{@code count}</td></tr>
   *   <tr><td>{@code batched-threshold}</td><td>{@code count}</td></tr>
   *   <tr><td>{@code batched-time}</td><td>{@code time}</td></tr>
   *   <tr><td>{@code stage-end}, {@code shutdown}</td><td>{@code boundary}</td></tr>
   * </table>
   *
   * The schema enum is strict: unknown labels throw rather than pass through, because the wire
   * contract guarantees one of the canonical four. Adding a new dispatcher label therefore means
   * a coordinated change — extend this switch, extend the {@code reason} enum in
   * {@code docs/schema/events/checkpoint-advanced.schema.json}, regenerate the POJOs, and update
   * the table in {@code docs/CONTROL_PLANE.md §5.4}.
   */
  private static CheckpointAdvanced.Reason normaliseCheckpointReason(String reason) {
    if (reason == null) {
      return CheckpointAdvanced.Reason.COUNT;
    }
    return switch (reason) {
      case "batched-threshold" -> CheckpointAdvanced.Reason.COUNT;
      case "batched-time" -> CheckpointAdvanced.Reason.TIME;
      case "stage-end", "shutdown" -> CheckpointAdvanced.Reason.BOUNDARY;
      default -> throw new IllegalStateException(
          "Unrecognised BufferedCheckpointDispatcher reason '"
              + reason
              + "'; extend ActiveTap.normaliseCheckpointReason and the reason enum in"
              + " checkpoint-advanced.schema.json.");
    };
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
