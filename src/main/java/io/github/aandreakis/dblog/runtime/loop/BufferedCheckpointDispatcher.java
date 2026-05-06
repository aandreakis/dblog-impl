package io.github.aandreakis.dblog.runtime.loop;

import io.github.aandreakis.dblog.adapter.api.SourceRuntime;
import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.checkpoint.CheckpointFlushPolicy;
import io.github.aandreakis.dblog.runtime.observer.RuntimeLoopObserver;
import io.github.aandreakis.dblog.tap.Tap;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;

/**
 * Applies the shared buffered checkpoint policy after transactions have already been durably
 * appended downstream.
 *
 * <p>Each persisted transaction becomes the latest checkpoint candidate. The dispatcher advances
 * the source checkpoint when the configured {@link CheckpointFlushPolicy} is due, when a drain
 * boundary forces a flush, or when shutdown requests a final flush.
 *
 * <p>If dump/repair coordination advances the checkpoint immediately after persisting its own local
 * state, {@link #markCheckpointAdvanced(SourceTransaction)} clears any older buffered candidate so
 * the runtime does not re-ack a superseded checkpoint.
 */
public final class BufferedCheckpointDispatcher<TX extends SourceTransaction<?>> {
  private final SourceRuntime<TX> runtime;
  private final BufferedCheckpointAcknowledger<TX> acknowledger;
  private final RuntimeLoopObserver<TX> observer;
  private final Tap tap;

  public BufferedCheckpointDispatcher(
      SourceRuntime<TX> runtime, CheckpointFlushPolicy policy, Tap tap) {
    this(
        Objects.requireNonNull(runtime, "runtime"),
        new BufferedCheckpointAcknowledger<>(Objects.requireNonNull(policy, "policy")),
        RuntimeLoopObserver.noop(),
        tap);
  }

  public BufferedCheckpointDispatcher(
      SourceRuntime<TX> runtime,
      CheckpointFlushPolicy policy,
      RuntimeLoopObserver<TX> observer,
      Tap tap) {
    this(
        Objects.requireNonNull(runtime, "runtime"),
        new BufferedCheckpointAcknowledger<>(Objects.requireNonNull(policy, "policy")),
        observer,
        tap);
  }

  BufferedCheckpointDispatcher(
      SourceRuntime<TX> runtime,
      BufferedCheckpointAcknowledger<TX> acknowledger,
      RuntimeLoopObserver<TX> observer,
      Tap tap) {
    this.runtime = Objects.requireNonNull(runtime, "runtime");
    this.acknowledger = Objects.requireNonNull(acknowledger, "acknowledger");
    this.observer = Objects.requireNonNull(observer, "observer");
    this.tap = Objects.requireNonNull(tap, "tap");
  }

  public void recordPersisted(String stageLabel, TX transaction) throws SQLException {
    Objects.requireNonNull(stageLabel, "stageLabel");
    Objects.requireNonNull(transaction, "transaction");
    observer.onTransactionPersisted(stageLabel, transaction);
    observer.onBufferedEventCountChanged(acknowledger.bufferedEventCount() + transaction.eventCount());
    Optional<TX> flushTarget = acknowledger.recordPersisted(transaction);
    if (flushTarget.isPresent()) {
      acknowledge(stageLabel, "batched-threshold", flushTarget.orElseThrow());
    } else {
      observer.onBufferedEventCountChanged(acknowledger.bufferedEventCount());
    }
  }

  public void flushIfDue(String stageLabel, String reason) throws SQLException {
    Objects.requireNonNull(stageLabel, "stageLabel");
    Objects.requireNonNull(reason, "reason");
    Optional<TX> flushTarget = acknowledger.flushIfDue();
    if (flushTarget.isPresent()) {
      acknowledge(stageLabel, reason, flushTarget.orElseThrow());
    } else {
      observer.onBufferedEventCountChanged(acknowledger.bufferedEventCount());
    }
  }

  public void forceFlush(String stageLabel, String reason) throws SQLException {
    Objects.requireNonNull(stageLabel, "stageLabel");
    Objects.requireNonNull(reason, "reason");
    Optional<TX> flushTarget = acknowledger.forceFlush();
    if (flushTarget.isPresent()) {
      acknowledge(stageLabel, reason, flushTarget.orElseThrow());
    } else {
      observer.onBufferedEventCountChanged(acknowledger.bufferedEventCount());
    }
  }

  public void markCheckpointAdvanced(TX transaction) {
    acknowledger.markCheckpointAdvanced(Objects.requireNonNull(transaction, "transaction"));
    observer.onBufferedEventCountChanged(acknowledger.bufferedEventCount());
  }

  private void acknowledge(String stageLabel, String reason, TX transaction) throws SQLException {
    runtime.acknowledge(transaction);
    observer.onBufferedEventCountChanged(acknowledger.bufferedEventCount());
    observer.onCheckpointAdvanced(stageLabel, reason, transaction);
    tap.onCheckpointAdvanced(
        transaction.checkpointPosition(), acknowledger.bufferedEventCount(), reason);
  }
}
