package io.github.aandreakis.dblog.runtime.loop;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.checkpoint.CheckpointFlushPolicy;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

public final class BufferedCheckpointAcknowledger<TX extends SourceTransaction<?>> {
  private final CheckpointFlushPolicy policy;
  private final long maxBufferedNanos;
  private final LongSupplier nanoClock;

  private TX latestPendingTransaction;
  private int bufferedEventCount;
  private long firstBufferedAtNanos;
  private boolean hasBufferedStartTime;

  public BufferedCheckpointAcknowledger(CheckpointFlushPolicy policy) {
    this(policy, System::nanoTime);
  }

  BufferedCheckpointAcknowledger(CheckpointFlushPolicy policy, LongSupplier nanoClock) {
    this.policy = Objects.requireNonNull(policy, "policy");
    this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    this.maxBufferedNanos = policy.maxBufferedTime().toNanos();
  }

  public Optional<TX> recordPersisted(TX transaction) {
    Objects.requireNonNull(transaction, "transaction");
    if (!hasBufferedStartTime) {
      firstBufferedAtNanos = nanoClock.getAsLong();
      hasBufferedStartTime = true;
    }
    latestPendingTransaction = transaction;
    bufferedEventCount += transaction.eventCount();
    return flushIfDue();
  }

  public Optional<TX> flushIfDue() {
    if (latestPendingTransaction == null) {
      return Optional.empty();
    }
    // Event-count threshold is the cheap check (integer compare) and in most workloads fires
    // far earlier than the time threshold — short-circuit so the time check only runs when we
    // actually need it. Using System.nanoTime() here (not wall-clock Instant.now()) matches
    // the streaming pump's own elapsed-time measurement and is immune to NTP / clock jumps.
    if (bufferedEventCount >= policy.maxBufferedEvents()) {
      return forceFlush();
    }
    long elapsedNanos = nanoClock.getAsLong() - firstBufferedAtNanos;
    if (elapsedNanos >= maxBufferedNanos) {
      return forceFlush();
    }
    return Optional.empty();
  }

  public Optional<TX> forceFlush() {
    if (latestPendingTransaction == null) {
      return Optional.empty();
    }
    TX flushTarget = latestPendingTransaction;
    latestPendingTransaction = null;
    bufferedEventCount = 0;
    hasBufferedStartTime = false;
    return Optional.of(flushTarget);
  }

  public void markCheckpointAdvanced(TX transaction) {
    Objects.requireNonNull(transaction, "transaction");
    if (latestPendingTransaction == null) {
      return;
    }
    if (compareCheckpointPositions(
            transaction.checkpointPosition(), latestPendingTransaction.checkpointPosition())
        >= 0) {
      latestPendingTransaction = null;
      bufferedEventCount = 0;
      hasBufferedStartTime = false;
    }
  }

  public int bufferedEventCount() {
    return bufferedEventCount;
  }

  public boolean hasPendingCheckpoint() {
    return latestPendingTransaction != null;
  }

  private static int compareCheckpointPositions(SourcePosition left, SourcePosition right) {
    if (left instanceof Comparable<?> comparable) {
      @SuppressWarnings("unchecked")
      Comparable<SourcePosition> typedComparable = (Comparable<SourcePosition>) comparable;
      return typedComparable.compareTo(right);
    }
    throw new IllegalStateException(
        "checkpoint positions must implement Comparable for buffered acknowledgement");
  }
}
