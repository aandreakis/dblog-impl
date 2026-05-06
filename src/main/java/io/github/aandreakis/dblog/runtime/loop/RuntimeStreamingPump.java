package io.github.aandreakis.dblog.runtime.loop;

import io.github.aandreakis.dblog.adapter.api.SourceRuntime;
import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.checkpoint.CheckpointFlushPolicy;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.runtime.observer.RuntimeLoopObserver;
import io.github.aandreakis.dblog.sink.api.ChangeEventBatchContext;
import io.github.aandreakis.dblog.sink.api.ChangeEventSink;
import io.github.aandreakis.dblog.sink.api.ContextualChangeEventSink;
import io.github.aandreakis.dblog.tap.Tap;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public final class RuntimeStreamingPump<TX extends SourceTransaction<?>> {
  private static final Duration MIN_POLL_INTERVAL = Duration.ofMillis(1);
  private static final Duration MAX_POLL_INTERVAL = Duration.ofMillis(32);

  private final SourceRuntime<TX> runtime;
  private final ChangeEventSink sink;
  private final BufferedCheckpointDispatcher<TX> checkpointDispatcher;
  private final RuntimeLoopObserver<TX> observer;
  private final Duration pollInterval;
  private final Duration heartbeatInterval;
  private final AtomicBoolean stopRequested;
  private final Tap tap;

  public RuntimeStreamingPump(SourceRuntime<TX> runtime, ChangeEventSink sink, Tap tap) {
    this(
        runtime,
        sink,
        new BufferedCheckpointDispatcher<>(
            Objects.requireNonNull(runtime, "runtime"), CheckpointFlushPolicy.defaults(), tap),
        RuntimeLoopObserver.noop(),
        Duration.ofMillis(32),
        Duration.ofSeconds(5),
        new AtomicBoolean(false),
        tap);
  }

  public RuntimeStreamingPump(
      SourceRuntime<TX> runtime,
      ChangeEventSink sink,
      CheckpointFlushPolicy checkpointFlushPolicy,
      Tap tap) {
    this(
        runtime,
        sink,
        new BufferedCheckpointDispatcher<>(
            Objects.requireNonNull(runtime, "runtime"),
            Objects.requireNonNull(checkpointFlushPolicy, "checkpointFlushPolicy"),
            tap),
        RuntimeLoopObserver.noop(),
        Duration.ofMillis(32),
        Duration.ofSeconds(5),
        new AtomicBoolean(false),
        tap);
  }

  public RuntimeStreamingPump(
      SourceRuntime<TX> runtime,
      ChangeEventSink sink,
      CheckpointFlushPolicy checkpointFlushPolicy,
      RuntimeLoopObserver<TX> observer,
      Tap tap) {
    this(
        runtime,
        sink,
        new BufferedCheckpointDispatcher<>(
            Objects.requireNonNull(runtime, "runtime"),
            Objects.requireNonNull(checkpointFlushPolicy, "checkpointFlushPolicy"),
            observer,
            tap),
        observer,
        Duration.ofMillis(32),
        Duration.ofSeconds(5),
        new AtomicBoolean(false),
        tap);
  }

  RuntimeStreamingPump(
      SourceRuntime<TX> runtime,
      ChangeEventSink sink,
      BufferedCheckpointDispatcher<TX> checkpointDispatcher,
      RuntimeLoopObserver<TX> observer,
      Duration pollInterval,
      Duration heartbeatInterval,
      AtomicBoolean stopRequested,
      Tap tap) {
    this.runtime = Objects.requireNonNull(runtime, "runtime");
    this.sink = Objects.requireNonNull(sink, "sink");
    this.checkpointDispatcher = Objects.requireNonNull(checkpointDispatcher, "checkpointDispatcher");
    this.observer = Objects.requireNonNull(observer, "observer");
    this.pollInterval = requirePositive(pollInterval, "pollInterval");
    this.heartbeatInterval = requirePositive(heartbeatInterval, "heartbeatInterval");
    this.stopRequested = Objects.requireNonNull(stopRequested, "stopRequested");
    this.tap = Objects.requireNonNull(tap, "tap");
  }

  public SourceRuntime<TX> runtime() {
    return runtime;
  }

  public ChangeEventSink sink() {
    return sink;
  }

  public Tap tap() {
    return tap;
  }

  public int drainStreaming(String stageLabel, Duration idleTimeout) throws SQLException {
    requireNonBlank(stageLabel, "stageLabel");
    requirePositive(idleTimeout, "idleTimeout");

    long idleDeadline = System.nanoTime() + idleTimeout.toNanos();
    int drained = 0;
    Duration currentPollInterval = initialPollInterval();
    while (!stopRequested.get() && System.nanoTime() < idleDeadline) {
      emitHeartbeatIfDue(Instant.now());
      if (drainOneTransaction(stageLabel)) {
        drained++;
        idleDeadline = System.nanoTime() + idleTimeout.toNanos();
        currentPollInterval = initialPollInterval();
        continue;
      }
      flushIfDue(stageLabel, "batched-time");
      sleepPollInterval(currentPollInterval);
      currentPollInterval = nextPollInterval(currentPollInterval);
    }
    forceFlush(stageLabel, "stage-end");
    return drained;
  }

  public void requestStop() {
    stopRequested.set(true);
  }

  void emitHeartbeatIfDue(Instant loopNow) throws SQLException {
    runtime.emitHeartbeatIfDue(Objects.requireNonNull(loopNow, "loopNow"), heartbeatInterval);
  }

  boolean drainOneTransaction(String stageLabel) throws SQLException {
    Optional<TX> transaction = runtime.readPendingTransaction();
    if (transaction.isEmpty()) {
      return false;
    }
    TX committed = transaction.orElseThrow();
    appendThroughEventSink(stageLabel, committed.commitTimestamp(), committed.events());
    checkpointDispatcher.recordPersisted(stageLabel, committed);
    return true;
  }

  void appendThroughEventSink(
      String stageLabel, Instant sourceCommitTimestamp, List<ChangeEvent> events) {
    Objects.requireNonNull(stageLabel, "stageLabel");
    List<ChangeEvent> emittedEvents = Objects.requireNonNull(events, "events");
    observer.onEventSinkAppendStarted(stageLabel, sourceCommitTimestamp, emittedEvents.size());
    // Elapsed-time measurement uses System.nanoTime() (monotonic) rather than Instant.now()
    // (wall-clock) so a concurrent NTP step or clock adjustment cannot produce a negative
    // Duration for a successful interval. Also avoids the two per-event Instant allocations
    // that dominated the timing hot path at high throughput.
    long startedAtNanos = System.nanoTime();
    tap.onSinkBatchStart();
    tap.onCdcBatch(emittedEvents);
    try {
      List<ChangeEvent> sinkEvents = filterControlEvents(emittedEvents);
      if (!sinkEvents.isEmpty()) {
        if (sink instanceof ContextualChangeEventSink contextualSink) {
          contextualSink.appendEvents(
              new ChangeEventBatchContext(stageLabel, sourceCommitTimestamp), sinkEvents);
        } else {
          sink.appendEvents(sinkEvents);
        }
      }
      observer.onEventSinkAppendSucceeded(
          stageLabel,
          sourceCommitTimestamp,
          emittedEvents.size(),
          System.nanoTime() - startedAtNanos);
      tap.onSinkBatchCommit();
    } catch (RuntimeException | Error failure) {
      tap.onError(
          failure.getClass().getSimpleName(),
          failure.getMessage() == null ? "" : failure.getMessage(),
          Map.of("stage", stageLabel, "event_count", emittedEvents.size()));
      observer.onEventSinkAppendFailed(
          stageLabel,
          sourceCommitTimestamp,
          emittedEvents.size(),
          System.nanoTime() - startedAtNanos,
          failure);
      throw failure;
    }
  }

  void flushIfDue(String stageLabel, String reason) throws SQLException {
    checkpointDispatcher.flushIfDue(stageLabel, reason);
  }

  void forceFlush(String stageLabel, String reason) throws SQLException {
    checkpointDispatcher.forceFlush(stageLabel, reason);
  }

  void markCheckpointAdvanced(TX transaction) {
    checkpointDispatcher.markCheckpointAdvanced(transaction);
  }

  void sleepPollInterval(Duration interval) {
    try {
      Thread.sleep(Objects.requireNonNull(interval, "interval").toMillis());
    } catch (InterruptedException ex) {
      stopRequested.set(true);
      Thread.currentThread().interrupt();
    }
  }

  void sleepPollInterval() {
    sleepPollInterval(initialPollInterval());
  }

  /**
   * Returns the starting poll interval for the idle backoff curve. The backoff starts as small
   * as possible so we pick up new work quickly, then grows via {@link #nextPollInterval(Duration)}
   * up to a ceiling. Concretely: {@code min(pollInterval, MIN_POLL_INTERVAL)}.
   */
  Duration initialPollInterval() {
    return minDuration(pollInterval, MIN_POLL_INTERVAL);
  }

  /**
   * Advances the idle backoff: doubles the current interval, then caps at the smaller of the
   * user-configured {@code pollInterval} and {@link #MAX_POLL_INTERVAL}. The pre-doubling value
   * is first clamped up to {@link #MIN_POLL_INTERVAL} so that a sub-millisecond starting value
   * does not collapse to zero after {@link Duration#toMillis()} truncation.
   */
  Duration nextPollInterval(Duration currentInterval) {
    Duration requiredCurrent = Objects.requireNonNull(currentInterval, "currentInterval");
    long capMillis = Math.min(pollInterval.toMillis(), MAX_POLL_INTERVAL.toMillis());
    if (capMillis <= 0L) {
      return requiredCurrent;
    }
    long doubledMillis =
        Math.max(requiredCurrent.toMillis(), MIN_POLL_INTERVAL.toMillis()) * 2L;
    return Duration.ofMillis(Math.min(capMillis, doubledMillis));
  }

  private static Duration minDuration(Duration a, Duration b) {
    return a.compareTo(b) <= 0 ? a : b;
  }

  static List<ChangeEvent> filterControlEvents(List<ChangeEvent> events) {
    Objects.requireNonNull(events, "events");
    ArrayList<ChangeEvent> filtered = null;
    for (int index = 0; index < events.size(); index++) {
      ChangeEvent event = events.get(index);
      if (isControlEvent(event)) {
        if (filtered == null) {
          filtered = new ArrayList<>(Math.max(0, events.size() - 1));
          for (int copied = 0; copied < index; copied++) {
            filtered.add(events.get(copied));
          }
        }
        continue;
      }
      if (filtered != null) {
        filtered.add(event);
      }
    }
    if (filtered == null) {
      return events;
    }
    if (filtered.isEmpty()) {
      return List.of();
    }
    return List.copyOf(filtered);
  }

  private static boolean isControlEvent(ChangeEvent event) {
    return event.operationType() == OperationType.WATERMARK
        || event.operationType() == OperationType.HEARTBEAT;
  }

  private static Duration requirePositive(Duration value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isZero() || value.isNegative()) {
      throw new IllegalArgumentException(name + " must be > 0");
    }
    return value;
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
