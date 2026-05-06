package io.github.aandreakis.dblog.runtime.loop;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.api.SourceRuntime;
import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.checkpoint.CheckpointFlushPolicy;
import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.runtime.observer.RuntimeLoopObserver;
import io.github.aandreakis.dblog.sink.api.ChangeEventSink;
import io.github.aandreakis.dblog.tap.NoopTap;
import io.github.aandreakis.dblog.testsupport.ChangeEventTestFixtures;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class RuntimeStreamingPumpTests {
  @Test
  void drainStreamingPersistsUserEventsAndAdvancesCheckpointAtStageEnd() throws Exception {
    TestTransaction transaction =
        new TestTransaction(
            "tx-1",
            new ComparablePosition(1),
            List.of(
                userEvent(1),
                controlEvent(OperationType.WATERMARK, "lw-1"),
                controlEvent(OperationType.HEARTBEAT, "hb-1")));
    FakeRuntime runtime = new FakeRuntime(List.of(transaction));
    RecordingSink sink = new RecordingSink();
    RecordingObserver observer = new RecordingObserver();
    RuntimeStreamingPump<TestTransaction> pump =
        new RuntimeStreamingPump<>(
            runtime,
            sink,
            new BufferedCheckpointDispatcher<>(
                runtime,
                new CheckpointFlushPolicy(100, Duration.ofMinutes(5)),
                observer, NoopTap.INSTANCE),
            observer,
            Duration.ofMillis(1),
            Duration.ofSeconds(5),
            new AtomicBoolean(false), NoopTap.INSTANCE);

    int drained = pump.drainStreaming("startup-drain", Duration.ofMillis(20));

    assertThat(drained).isEqualTo(1);
    assertThat(sink.batches).hasSize(1);
    assertThat(sink.batches.get(0)).hasSize(1);
    assertThat(sink.batches.get(0).get(0).operationType()).isEqualTo(OperationType.UPDATE);
    assertThat(runtime.acknowledgedTransactions).containsExactly(transaction);
    assertThat(observer.persistedStages).containsExactly("startup-drain");
    assertThat(observer.advancedReasons).containsExactly("stage-end");
    assertThat(observer.appendStartedStages).containsExactly("startup-drain");
    assertThat(observer.appendSucceededStages).containsExactly("startup-drain");
  }

  @Test
  void emitsHeartbeatsWhileIdle() throws Exception {
    FakeRuntime runtime = new FakeRuntime(List.of());
    RuntimeStreamingPump<TestTransaction> pump =
        new RuntimeStreamingPump<>(
            runtime,
            new RecordingSink(),
            new BufferedCheckpointDispatcher<>(
                runtime, new CheckpointFlushPolicy(100, Duration.ofMinutes(5)), NoopTap.INSTANCE),
            RuntimeLoopObserver.noop(),
            Duration.ofMillis(1),
            Duration.ofMillis(1),
            new AtomicBoolean(false), NoopTap.INSTANCE);

    pump.drainStreaming("idle-drain", Duration.ofMillis(20));

    assertThat(runtime.heartbeatEmissions).isNotEmpty();
  }

  @Test
  void adaptivePollBackoffStartsAtOneMillisecondAndCapsAtThirtyTwoMilliseconds() {
    RuntimeStreamingPump<TestTransaction> pump =
        new RuntimeStreamingPump<>(
            new FakeRuntime(List.of()),
            new RecordingSink(),
            new BufferedCheckpointDispatcher<>(
                new FakeRuntime(List.of()),
                new CheckpointFlushPolicy(100, Duration.ofMinutes(5)), NoopTap.INSTANCE),
            RuntimeLoopObserver.noop(),
            Duration.ofMillis(32),
            Duration.ofSeconds(5),
            new AtomicBoolean(false), NoopTap.INSTANCE);

    Duration first = pump.initialPollInterval();
    Duration second = pump.nextPollInterval(first);
    Duration third = pump.nextPollInterval(second);
    Duration fourth = pump.nextPollInterval(third);
    Duration fifth = pump.nextPollInterval(fourth);
    Duration sixth = pump.nextPollInterval(fifth);

    assertThat(first).isEqualTo(Duration.ofMillis(1));
    assertThat(second).isEqualTo(Duration.ofMillis(2));
    assertThat(third).isEqualTo(Duration.ofMillis(4));
    assertThat(fourth).isEqualTo(Duration.ofMillis(8));
    assertThat(fifth).isEqualTo(Duration.ofMillis(16));
    assertThat(sixth).isEqualTo(Duration.ofMillis(32));
    assertThat(pump.nextPollInterval(sixth)).isEqualTo(Duration.ofMillis(32));
  }

  @Test
  void adaptivePollBackoffCapsAtMaxWhenConfiguredIntervalIsLarger() {
    RuntimeStreamingPump<TestTransaction> pump =
        new RuntimeStreamingPump<>(
            new FakeRuntime(List.of()),
            new RecordingSink(),
            new BufferedCheckpointDispatcher<>(
                new FakeRuntime(List.of()),
                new CheckpointFlushPolicy(100, Duration.ofMinutes(5)), NoopTap.INSTANCE),
            RuntimeLoopObserver.noop(),
            Duration.ofSeconds(5),
            Duration.ofSeconds(5),
            new AtomicBoolean(false), NoopTap.INSTANCE);

    // pollInterval (5s) is above MAX (32ms). Backoff still starts at MIN and caps at MAX.
    Duration current = pump.initialPollInterval();
    assertThat(current).isEqualTo(Duration.ofMillis(1));
    for (int doublings = 0; doublings < 10; doublings++) {
      current = pump.nextPollInterval(current);
    }
    assertThat(current).isEqualTo(Duration.ofMillis(32));
  }

  @Test
  void adaptivePollBackoffReturnsConfiguredValueWhenBelowMinimumFloor() {
    // Sub-millisecond configured pollInterval collapses under toMillis() truncation; verify
    // the contract: initial poll honors the user's low value and nextPollInterval leaves the
    // interval alone rather than crashing on a zero cap.
    RuntimeStreamingPump<TestTransaction> pump =
        new RuntimeStreamingPump<>(
            new FakeRuntime(List.of()),
            new RecordingSink(),
            new BufferedCheckpointDispatcher<>(
                new FakeRuntime(List.of()),
                new CheckpointFlushPolicy(100, Duration.ofMinutes(5)), NoopTap.INSTANCE),
            RuntimeLoopObserver.noop(),
            Duration.ofNanos(500_000), // 0.5 ms
            Duration.ofSeconds(5),
            new AtomicBoolean(false), NoopTap.INSTANCE);

    Duration first = pump.initialPollInterval();
    assertThat(first).isEqualTo(Duration.ofNanos(500_000));
    // Cap collapses to 0ms under millis truncation; nextPollInterval then short-circuits.
    assertThat(pump.nextPollInterval(first)).isEqualTo(first);
  }

  @Test
  void adaptivePollBackoffRespectsSmallerConfiguredPollCap() {
    RuntimeStreamingPump<TestTransaction> pump =
        new RuntimeStreamingPump<>(
            new FakeRuntime(List.of()),
            new RecordingSink(),
            new BufferedCheckpointDispatcher<>(
                new FakeRuntime(List.of()),
                new CheckpointFlushPolicy(100, Duration.ofMinutes(5)), NoopTap.INSTANCE),
            RuntimeLoopObserver.noop(),
            Duration.ofMillis(5),
            Duration.ofSeconds(5),
            new AtomicBoolean(false), NoopTap.INSTANCE);

    Duration first = pump.initialPollInterval();
    Duration second = pump.nextPollInterval(first);
    Duration third = pump.nextPollInterval(second);
    Duration fourth = pump.nextPollInterval(third);

    assertThat(first).isEqualTo(Duration.ofMillis(1));
    assertThat(second).isEqualTo(Duration.ofMillis(2));
    assertThat(third).isEqualTo(Duration.ofMillis(4));
    assertThat(fourth).isEqualTo(Duration.ofMillis(5));
  }

  private static ChangeEvent userEvent(int id) {
    return ChangeEventTestFixtures.fromRowMaps(
        new TableId("source", "appdb", "widgets"),
        OperationType.UPDATE,
        CaptureOrigin.LOG,
        Map.of("id", id),
        null,
        Map.of("id", id),
        new OpaqueSourcePosition("source:" + id),
        "tx-" + id,
        null);
  }

  private static ChangeEvent controlEvent(OperationType type, String token) {
    return ChangeEventTestFixtures.fromRowMaps(
        new TableId("source", "dblog_meta", type == OperationType.HEARTBEAT ? "heartbeats" : "watermarks"),
        type,
        CaptureOrigin.LOG,
        Map.of("id", 1),
        null,
        Map.of("token", token),
        new OpaqueSourcePosition("control:" + token),
        null,
        null);
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

  private static final class FakeRuntime implements SourceRuntime<TestTransaction> {
    private final Deque<TestTransaction> pendingTransactions;
    private final List<TestTransaction> acknowledgedTransactions = new ArrayList<>();
    private final List<Instant> heartbeatEmissions = new ArrayList<>();

    private FakeRuntime(List<TestTransaction> pendingTransactions) {
      this.pendingTransactions = new ArrayDeque<>(pendingTransactions);
    }

    @Override
    public Optional<TestTransaction> readPendingTransaction() {
      return Optional.ofNullable(pendingTransactions.pollFirst());
    }

    @Override
    public void acknowledge(TestTransaction transaction) throws SQLException {
      acknowledgedTransactions.add(transaction);
    }

    @Override
    public boolean emitHeartbeatIfDue(Instant heartbeatTime, Duration minimumInterval) {
      heartbeatEmissions.add(heartbeatTime);
      return true;
    }
  }

  private static final class RecordingSink implements ChangeEventSink {
    private final List<List<ChangeEvent>> batches = new ArrayList<>();

    @Override
    public void appendEvents(List<ChangeEvent> events) {
      batches.add(List.copyOf(events));
    }
  }

  private static final class RecordingObserver implements RuntimeLoopObserver<TestTransaction> {
    private final List<String> persistedStages = new ArrayList<>();
    private final List<String> advancedReasons = new ArrayList<>();
    private final List<String> appendStartedStages = new ArrayList<>();
    private final List<String> appendSucceededStages = new ArrayList<>();

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
    public void onEventSinkAppendStarted(
        String stageLabel, Instant sourceCommitTimestamp, int eventCount) {
      appendStartedStages.add(stageLabel);
    }

    @Override
    public void onEventSinkAppendSucceeded(
        String stageLabel, Instant sourceCommitTimestamp, int eventCount, long appendNanos) {
      appendSucceededStages.add(stageLabel);
    }
  }
}
