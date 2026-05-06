package io.github.aandreakis.dblog.runtime.observer;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.request.ScheduledRequestBatch;
import java.time.Instant;

public interface RuntimeLoopObserver<TX extends SourceTransaction<?>> {
  default void onTransactionPersisted(String stageLabel, TX transaction) {}

  default void onCheckpointAdvanced(String stageLabel, String reason, TX transaction) {}

  default void onRequestBatch(ScheduledRequestBatch<TX> batch) {}

  default void onBufferedEventCountChanged(int bufferedEventCount) {}

  default void onPendingRequestCountChanged(int pendingRequestCount) {}

  default void onEventSinkAppendStarted(
      String stageLabel, Instant sourceCommitTimestamp, int eventCount) {}

  /**
   * Elapsed append time is reported as monotonic {@code long} nanoseconds (from
   * {@link System#nanoTime()}). Observers that want a {@link java.time.Duration} should
   * construct one themselves — passing a boxed {@code Duration} allocates on every pump tick,
   * which matters on the streaming hot path where the default observer does nothing.
   */
  default void onEventSinkAppendSucceeded(
      String stageLabel, Instant sourceCommitTimestamp, int eventCount, long appendNanos) {}

  default void onEventSinkAppendFailed(
      String stageLabel,
      Instant sourceCommitTimestamp,
      int eventCount,
      long appendNanos,
      Throwable failure) {}

  static <TX extends SourceTransaction<?>> RuntimeLoopObserver<TX> noop() {
    return new RuntimeLoopObserver<>() {};
  }
}
