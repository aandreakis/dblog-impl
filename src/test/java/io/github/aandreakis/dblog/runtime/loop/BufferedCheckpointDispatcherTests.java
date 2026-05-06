package io.github.aandreakis.dblog.runtime.loop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.adapter.api.SourceRuntime;
import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.checkpoint.CheckpointFlushPolicy;
import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.runtime.observer.RuntimeLoopObserver;
import io.github.aandreakis.dblog.tap.NoopTap;
import io.github.aandreakis.dblog.testsupport.ChangeEventTestFixtures;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class BufferedCheckpointDispatcherTests {
  @Test
  void acknowledgesImmediatelyAtThresholdAndNotifiesObserver() throws Exception {
    TestTransaction transaction = transaction("tx-1", 1, 2);
    RecordingRuntime runtime = new RecordingRuntime();
    RecordingObserver observer = new RecordingObserver();
    BufferedCheckpointDispatcher<TestTransaction> dispatcher =
        new BufferedCheckpointDispatcher<>(
            runtime,
            new CheckpointFlushPolicy(1, Duration.ofMinutes(5)),
            observer, NoopTap.INSTANCE);

    dispatcher.recordPersisted("stream", transaction);

    assertThat(runtime.acknowledged).containsExactly(transaction);
    assertThat(observer.persistedStages).containsExactly("stream");
    assertThat(observer.advancedReasons).containsExactly("batched-threshold");
    assertThat(observer.bufferedCounts).containsExactly(2, 0);
  }

  @Test
  void forceFlushAcknowledgesPendingTransactionAndFlushIfDueWithoutPendingIsIdle() throws Exception {
    TestTransaction transaction = transaction("tx-2", 2, 1);
    RecordingRuntime runtime = new RecordingRuntime();
    RecordingObserver observer = new RecordingObserver();
    BufferedCheckpointDispatcher<TestTransaction> dispatcher =
        new BufferedCheckpointDispatcher<>(
            runtime,
            new CheckpointFlushPolicy(10, Duration.ofMinutes(5)),
            observer, NoopTap.INSTANCE);

    dispatcher.recordPersisted("stream", transaction);
    dispatcher.flushIfDue("stream", "batched-time");
    dispatcher.forceFlush("stream", "shutdown");

    assertThat(runtime.acknowledged).containsExactly(transaction);
    assertThat(observer.advancedReasons).containsExactly("shutdown");
    assertThat(observer.bufferedCounts).containsExactly(1, 1, 1, 0);
  }

  @Test
  void acknowledgeFailurePropagatesWithoutReportingCheckpointAdvance() throws Exception {
    TestTransaction transaction = transaction("tx-3", 3, 1);
    RecordingObserver observer = new RecordingObserver();
    RecordingRuntime runtime = new RecordingRuntime();
    runtime.failAcknowledge = new SQLException("ack exploded");
    BufferedCheckpointDispatcher<TestTransaction> dispatcher =
        new BufferedCheckpointDispatcher<>(
            runtime,
            new CheckpointFlushPolicy(1, Duration.ofMinutes(5)),
            observer, NoopTap.INSTANCE);

    assertThatThrownBy(() -> dispatcher.recordPersisted("stream", transaction))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("ack exploded");
    assertThat(observer.persistedStages).containsExactly("stream");
    assertThat(observer.advancedReasons).isEmpty();
  }

  private static TestTransaction transaction(String transactionId, int ordinal, int eventCount) {
    List<ChangeEvent> events = new ArrayList<>();
    for (int index = 0; index < eventCount; index++) {
      events.add(
          ChangeEventTestFixtures.fromRowMaps(
              new TableId("source", "appdb", "widgets"),
              OperationType.INSERT,
              CaptureOrigin.LOG,
              Map.of("id", ordinal),
              null,
              Map.of("id", ordinal),
              new ComparablePosition(ordinal),
              transactionId,
              null));
    }
    return new TestTransaction(transactionId, new ComparablePosition(ordinal), events);
  }

  private record TestTransaction(
      String transactionId, ComparablePosition checkpointPosition, List<ChangeEvent> events)
      implements SourceTransaction<ComparablePosition> {
    @Override
    public Instant commitTimestamp() {
      return Instant.parse("2026-03-25T00:00:00Z");
    }
  }

  private record ComparablePosition(int ordinal)
      implements SourcePosition, Comparable<ComparablePosition> {
    @Override
    public String displayValue() {
      return "checkpoint:" + ordinal;
    }

    @Override
    public int compareTo(ComparablePosition other) {
      return Integer.compare(ordinal, other.ordinal);
    }
  }

  private static final class RecordingRuntime implements SourceRuntime<TestTransaction> {
    private final List<TestTransaction> acknowledged = new ArrayList<>();
    private SQLException failAcknowledge;

    @Override
    public Optional<TestTransaction> readPendingTransaction() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void acknowledge(TestTransaction transaction) throws SQLException {
      if (failAcknowledge != null) {
        throw failAcknowledge;
      }
      acknowledged.add(transaction);
    }
  }

  private static final class RecordingObserver implements RuntimeLoopObserver<TestTransaction> {
    private final List<String> persistedStages = new ArrayList<>();
    private final List<String> advancedReasons = new ArrayList<>();
    private final List<Integer> bufferedCounts = new ArrayList<>();

    @Override
    public void onTransactionPersisted(String stageLabel, TestTransaction transaction) {
      persistedStages.add(stageLabel);
    }

    @Override
    public void onCheckpointAdvanced(
        String stageLabel, String reason, TestTransaction transaction) {
      advancedReasons.add(reason);
    }

    @Override
    public void onBufferedEventCountChanged(int bufferedEventCount) {
      bufferedCounts.add(bufferedEventCount);
    }
  }
}
