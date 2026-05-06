package io.github.aandreakis.dblog.runtime.observer;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.request.ScheduledRequestBatch;
import java.time.Instant;
import java.util.List;

/** Composes multiple final runtime-loop observers into one. */
public final class CompositeRuntimeLoopObserver<TX extends SourceTransaction<?>>
    implements RuntimeLoopObserver<TX> {
  private final List<RuntimeLoopObserver<TX>> delegates;

  private CompositeRuntimeLoopObserver(List<RuntimeLoopObserver<TX>> delegates) {
    this.delegates = List.copyOf(delegates);
  }

  @SafeVarargs
  public static <TX extends SourceTransaction<?>> RuntimeLoopObserver<TX> of(
      RuntimeLoopObserver<TX>... delegates) {
    return new CompositeRuntimeLoopObserver<>(List.of(delegates));
  }

  @Override
  public void onTransactionPersisted(String stageLabel, TX transaction) {
    for (RuntimeLoopObserver<TX> delegate : delegates) {
      delegate.onTransactionPersisted(stageLabel, transaction);
    }
  }

  @Override
  public void onCheckpointAdvanced(String stageLabel, String reason, TX transaction) {
    for (RuntimeLoopObserver<TX> delegate : delegates) {
      delegate.onCheckpointAdvanced(stageLabel, reason, transaction);
    }
  }

  @Override
  public void onRequestBatch(ScheduledRequestBatch<TX> batch) {
    for (RuntimeLoopObserver<TX> delegate : delegates) {
      delegate.onRequestBatch(batch);
    }
  }

  @Override
  public void onBufferedEventCountChanged(int bufferedEventCount) {
    for (RuntimeLoopObserver<TX> delegate : delegates) {
      delegate.onBufferedEventCountChanged(bufferedEventCount);
    }
  }

  @Override
  public void onPendingRequestCountChanged(int pendingRequestCount) {
    for (RuntimeLoopObserver<TX> delegate : delegates) {
      delegate.onPendingRequestCountChanged(pendingRequestCount);
    }
  }

  @Override
  public void onEventSinkAppendStarted(
      String stageLabel, Instant sourceCommitTimestamp, int eventCount) {
    for (RuntimeLoopObserver<TX> delegate : delegates) {
      delegate.onEventSinkAppendStarted(stageLabel, sourceCommitTimestamp, eventCount);
    }
  }

  @Override
  public void onEventSinkAppendSucceeded(
      String stageLabel, Instant sourceCommitTimestamp, int eventCount, long appendNanos) {
    for (RuntimeLoopObserver<TX> delegate : delegates) {
      delegate.onEventSinkAppendSucceeded(
          stageLabel, sourceCommitTimestamp, eventCount, appendNanos);
    }
  }

  @Override
  public void onEventSinkAppendFailed(
      String stageLabel,
      Instant sourceCommitTimestamp,
      int eventCount,
      long appendNanos,
      Throwable failure) {
    for (RuntimeLoopObserver<TX> delegate : delegates) {
      delegate.onEventSinkAppendFailed(
          stageLabel, sourceCommitTimestamp, eventCount, appendNanos, failure);
    }
  }
}
