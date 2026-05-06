package io.github.aandreakis.dblog.runtime.loop;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.request.CoreRequestExecutionException;
import io.github.aandreakis.dblog.core.request.DumpRequestCoordinator;
import io.github.aandreakis.dblog.core.request.ScheduledRequestBatch;
import io.github.aandreakis.dblog.runtime.observer.RuntimeLoopObserver;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.MDC;

/**
 * Runtime loop that interleaves operator request batches with live streaming. Two entry points:
 * {@link #processPendingRequests} drains the queue and returns; {@link #runUntilStopped} runs
 * until {@link #requestStop} is called (cooperative — current batch finishes first). Each is
 * single-threaded.
 */
public final class RuntimeRequestPump<TX extends SourceTransaction<?>> {
  private static final Duration DEFAULT_PENDING_REQUEST_COUNT_REFRESH_INTERVAL =
      Duration.ofMillis(250);

  private final RuntimeStreamingPump<TX> streamingPump;
  private final RuntimeLoopObserver<TX> observer;
  private final Duration pendingRequestCountRefreshInterval;
  private final AtomicBoolean stopRequested;

  public RuntimeRequestPump(RuntimeStreamingPump<TX> streamingPump) {
    this(
        streamingPump,
        RuntimeLoopObserver.noop(),
        DEFAULT_PENDING_REQUEST_COUNT_REFRESH_INTERVAL,
        new AtomicBoolean(false));
  }

  public RuntimeRequestPump(
      RuntimeStreamingPump<TX> streamingPump,
      RuntimeLoopObserver<TX> observer,
      Duration pendingRequestCountRefreshInterval,
      AtomicBoolean stopRequested) {
    this.streamingPump = Objects.requireNonNull(streamingPump, "streamingPump");
    this.observer = Objects.requireNonNull(observer, "observer");
    this.pendingRequestCountRefreshInterval =
        requirePositive(
            pendingRequestCountRefreshInterval == null
                ? DEFAULT_PENDING_REQUEST_COUNT_REFRESH_INTERVAL
                : pendingRequestCountRefreshInterval,
            "pendingRequestCountRefreshInterval");
    this.stopRequested = Objects.requireNonNull(stopRequested, "stopRequested");
  }

  public void processPendingRequests(
      DumpRequestCoordinator<TX> coordinator, Duration idleDrainTimeout) throws Exception {
    processPendingRequests(coordinator, idleDrainTimeout, RequestBatchHook.noop());
  }

  public void processPendingRequests(
      DumpRequestCoordinator<TX> coordinator,
      Duration idleDrainTimeout,
      RequestBatchHook<TX> batchHook)
      throws Exception {
    Objects.requireNonNull(coordinator, "coordinator");
    requirePositive(idleDrainTimeout, "idleDrainTimeout");
    RequestBatchHook<TX> effectiveHook =
        batchHook == null ? RequestBatchHook.noop() : batchHook;
    PendingRequestCountTracker<TX> pendingRequestCountTracker =
        new PendingRequestCountTracker<>(
            coordinator, observer, pendingRequestCountRefreshInterval);
    pendingRequestCountTracker.refreshNow();

    while (!stopRequested.get()) {
      if (pendingRequestCountTracker.hasNoPendingRequests()) {
        break;
      }
      streamingPump.emitHeartbeatIfDue(java.time.Instant.now());
      pendingRequestCountTracker.refreshIfDue();
      if (pendingRequestCountTracker.hasNoPendingRequests()) {
        break;
      }
      Optional<ScheduledRequestBatch<TX>> maybeBatch = coordinator.coordinateNextBatch();
      if (maybeBatch.isEmpty()) {
        streamingPump.drainStreaming("request-idle-drain", idleDrainTimeout);
        continue;
      }
      ScheduledRequestBatch<TX> batch = maybeBatch.orElseThrow();
      observer.onRequestBatch(batch);
      streamingPump.appendThroughEventSink(
          "request-batch-" + batch.request().requestId(),
          batch.checkpointTransaction().commitTimestamp(),
          batch.emittedEvents());
      effectiveHook.beforeAcknowledge(batch);
      coordinator.acknowledgeCompletedBatch(batch);
      pendingRequestCountTracker.markRequestCompleted(batch.finalRequestBatch());
      pendingRequestCountTracker.publishIfChanged();
      streamingPump.markCheckpointAdvanced(batch.checkpointTransaction());
    }
    pendingRequestCountTracker.publishZero();
  }

  public void runUntilStopped(DumpRequestCoordinator<TX> coordinator, String streamingStageLabel)
      throws Exception {
    Objects.requireNonNull(coordinator, "coordinator");
    requireNonBlank(streamingStageLabel, "streamingStageLabel");
    PendingRequestCountTracker<TX> pendingRequestCountTracker =
        new PendingRequestCountTracker<>(
            coordinator, observer, pendingRequestCountRefreshInterval);
    IdlePollBackoff idlePollBackoff = new IdlePollBackoff(streamingPump);

    while (!stopRequested.get()) {
      streamingPump.emitHeartbeatIfDue(java.time.Instant.now());
      pendingRequestCountTracker.refreshIfDue();
      Optional<ScheduledRequestBatch<TX>> maybeBatch;
      try {
        maybeBatch = coordinator.coordinateNextBatch();
      } catch (CoreRequestExecutionException failure) {
        streamingPump.sleepPollInterval(idlePollBackoff.nextSleepIntervalAfterIdle());
        continue;
      }
      if (maybeBatch.isPresent()) {
        ScheduledRequestBatch<TX> batch = maybeBatch.orElseThrow();
        // MDC requestId scope for the life of this batch's emit/ack. Every log line from the
        // sink, coordinator, or observer during this stretch carries the id for grep-friendly
        // debugging.
        try (MDC.MDCCloseable ignored = MDC.putCloseable("requestId", batch.request().requestId())) {
          observer.onRequestBatch(batch);
          streamingPump.appendThroughEventSink(
              "request-batch-" + batch.request().requestId(),
              batch.checkpointTransaction().commitTimestamp(),
              batch.emittedEvents());
          coordinator.acknowledgeCompletedBatch(batch);
          pendingRequestCountTracker.markRequestCompleted(batch.finalRequestBatch());
          pendingRequestCountTracker.publishIfChanged();
          streamingPump.markCheckpointAdvanced(batch.checkpointTransaction());
        }
        idlePollBackoff.reset();
        continue;
      }
      if (streamingPump.drainOneTransaction(streamingStageLabel)) {
        idlePollBackoff.reset();
        continue;
      }
      streamingPump.flushIfDue(streamingStageLabel, "batched-time");
      streamingPump.sleepPollInterval(idlePollBackoff.nextSleepIntervalAfterIdle());
    }
    pendingRequestCountTracker.publishZero();
    streamingPump.forceFlush(streamingStageLabel, "shutdown");
  }

  public void requestStop() {
    stopRequested.set(true);
  }

  @FunctionalInterface
  public interface RequestBatchHook<TX extends SourceTransaction<?>> {
    void beforeAcknowledge(ScheduledRequestBatch<TX> batch) throws Exception;

    static <TX extends SourceTransaction<?>> RequestBatchHook<TX> noop() {
      return batch -> {};
    }
  }

  static final class IdlePollBackoff {
    private final RuntimeStreamingPump<?> streamingPump;
    private Duration currentInterval;

    IdlePollBackoff(RuntimeStreamingPump<?> streamingPump) {
      this.streamingPump = Objects.requireNonNull(streamingPump, "streamingPump");
      this.currentInterval = this.streamingPump.initialPollInterval();
    }

    Duration currentInterval() {
      return currentInterval;
    }

    Duration nextSleepIntervalAfterIdle() {
      Duration interval = currentInterval;
      currentInterval = streamingPump.nextPollInterval(currentInterval);
      return interval;
    }

    void reset() {
      currentInterval = streamingPump.initialPollInterval();
    }
  }

  private static final class PendingRequestCountTracker<TX extends SourceTransaction<?>> {
    private final DumpRequestCoordinator<TX> coordinator;
    private final RuntimeLoopObserver<TX> observer;
    private final long refreshIntervalNanos;

    private long nextRefreshAtNanos;
    private int latestCount = -1;
    private int lastPublishedCount = Integer.MIN_VALUE;

    private PendingRequestCountTracker(
        DumpRequestCoordinator<TX> coordinator,
        RuntimeLoopObserver<TX> observer,
        Duration refreshInterval) {
      this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
      this.observer = Objects.requireNonNull(observer, "observer");
      this.refreshIntervalNanos = requirePositive(refreshInterval, "refreshInterval").toNanos();
    }

    private void refreshIfDue() {
      long now = System.nanoTime();
      if (now < nextRefreshAtNanos) {
        return;
      }
      refresh(now);
    }

    private void refreshNow() {
      refresh(System.nanoTime());
    }

    private void markRequestCompleted(boolean requestCompleted) {
      if (!requestCompleted || latestCount < 0) {
        return;
      }
      latestCount = Math.max(0, latestCount - 1);
    }

    private void publishZero() {
      latestCount = 0;
      publishIfChanged();
    }

    private boolean hasNoPendingRequests() {
      return latestCount == 0;
    }

    private void publishIfChanged() {
      if (latestCount < 0) {
        return;
      }
      if (lastPublishedCount == latestCount) {
        return;
      }
      lastPublishedCount = latestCount;
      observer.onPendingRequestCountChanged(latestCount);
    }

    private void refresh(long now) {
      int pendingRequestCount = coordinator.pendingRequestCount();
      nextRefreshAtNanos = now + refreshIntervalNanos;
      if (pendingRequestCount < 0) {
        return;
      }
      latestCount = pendingRequestCount;
      publishIfChanged();
    }
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
