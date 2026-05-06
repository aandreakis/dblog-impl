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
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.core.request.DumpRequestCoordinator;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.request.ScheduledRequestBatch;
import io.github.aandreakis.dblog.core.request.TargetedRepairBatch;
import io.github.aandreakis.dblog.runtime.observer.RuntimeLoopObserver;
import io.github.aandreakis.dblog.sink.api.ChangeEventSink;
import io.github.aandreakis.dblog.tap.NoopTap;
import io.github.aandreakis.dblog.testsupport.ChangeEventTestFixtures;
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

class RuntimeRequestPumpTests {
  @Test
  void processPendingRequestsAppendsBatchAndAcknowledgesCoordinator() throws Exception {
    FakeRuntime runtime = new FakeRuntime(List.of());
    RecordingSink sink = new RecordingSink();
    RecordingObserver observer = new RecordingObserver();
    RuntimeStreamingPump<TestTransaction> streamingPump =
        new RuntimeStreamingPump<>(
            runtime,
            sink,
            new CheckpointFlushPolicy(100, Duration.ofMinutes(5)),
            observer, NoopTap.INSTANCE);
    RuntimeRequestPump<TestTransaction> requestPump =
        new RuntimeRequestPump<>(
            streamingPump,
            observer,
            Duration.ofSeconds(1),
            new AtomicBoolean(false));

    DumpRequest request =
        DumpRequest.fromPrimaryKeyLiterals(
            "request-1",
            DumpScope.PRIMARY_KEYS,
            new TableId("source", "appdb", "widgets"),
            schema(),
            List.of("1"));
    TestTransaction checkpointTransaction = transaction("tx-1", 1, 1);
    ScheduledRequestBatch<TestTransaction> batch =
        ScheduledRequestBatch.targetedRepair(
            request,
            new TargetedRepairBatch<>(
                request,
                io.github.aandreakis.dblog.core.reconcile.Chunk.fromMapRows(
                    request.requestId(),
                    request.tableId().displayName(),
                    schema(),
                    null,
                    List.of(Map.of("id", 1)),
                    "1",
                    true),
                new io.github.aandreakis.dblog.core.reconcile.WatermarkWindow(
                    new io.github.aandreakis.dblog.core.model.WatermarkToken("lw-1"),
                    new io.github.aandreakis.dblog.core.model.WatermarkToken("hw-1")),
                List.of(),
                checkpointTransaction.events(),
                checkpointTransaction));
    FakeCoordinator coordinator = new FakeCoordinator(List.of(batch));

    requestPump.processPendingRequests(coordinator, Duration.ofMillis(5));

    assertThat(sink.batches).hasSize(1);
    assertThat(sink.batches.getFirst()).hasSize(1);
    assertThat(coordinator.acknowledgedBatches).containsExactly(batch);
    assertThat(observer.pendingRequestCounts).containsExactly(1, 0);
  }

  private static io.github.aandreakis.dblog.core.schema.TableSchema schema() {
    return io.github.aandreakis.dblog.core.schema.TableSchema.create(
        new TableId("source", "appdb", "widgets"),
        List.of(
            new io.github.aandreakis.dblog.core.schema.ColumnDefinition(
                "id",
                "bigint",
                io.github.aandreakis.dblog.core.schema.NeutralColumnType.INTEGER,
                true,
                false)),
        Instant.parse("2026-03-25T00:00:00Z"));
  }

  @Test
  void runUntilStoppedFallsBackToStreamingWhenNoPendingBatchExists() throws Exception {
    TestTransaction transaction = transaction("tx-stream", 2, 1);
    FakeRuntime runtime = new FakeRuntime(List.of(transaction));
    RecordingSink sink = new RecordingSink();
    RecordingObserver observer = new RecordingObserver();
    AtomicBoolean stopRequested = new AtomicBoolean(false);
    RuntimeStreamingPump<TestTransaction> streamingPump =
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
            stopRequested, NoopTap.INSTANCE);
    RuntimeRequestPump<TestTransaction> requestPump =
        new RuntimeRequestPump<>(streamingPump, observer, Duration.ofSeconds(1), stopRequested);
    FakeCoordinator coordinator = new FakeCoordinator(List.of());
    coordinator.onCoordinate = () -> requestPump.requestStop();

    requestPump.runUntilStopped(coordinator, "runtime-streaming");

    assertThat(sink.batches).hasSize(1);
    assertThat(runtime.acknowledgedTransactions).containsExactly(transaction);
  }

  @Test
  void idlePollBackoffDoublesUntilTheStreamingCapAndResetsAfterWork() {
    FakeRuntime runtime = new FakeRuntime(List.of());
    RecordingSink sink = new RecordingSink();
    RuntimeStreamingPump<TestTransaction> streamingPump =
        new RuntimeStreamingPump<>(
            runtime,
            sink,
            new BufferedCheckpointDispatcher<>(
                runtime,
                new CheckpointFlushPolicy(100, Duration.ofMinutes(5)),
                RuntimeLoopObserver.noop(), NoopTap.INSTANCE),
            RuntimeLoopObserver.noop(),
            Duration.ofMillis(64),
            Duration.ofSeconds(5),
            new AtomicBoolean(false), NoopTap.INSTANCE);

    RuntimeRequestPump.IdlePollBackoff backoff =
        new RuntimeRequestPump.IdlePollBackoff(streamingPump);

    assertThat(backoff.currentInterval()).isEqualTo(Duration.ofMillis(1));
    assertThat(backoff.nextSleepIntervalAfterIdle()).isEqualTo(Duration.ofMillis(1));
    assertThat(backoff.nextSleepIntervalAfterIdle()).isEqualTo(Duration.ofMillis(2));
    assertThat(backoff.nextSleepIntervalAfterIdle()).isEqualTo(Duration.ofMillis(4));
    assertThat(backoff.nextSleepIntervalAfterIdle()).isEqualTo(Duration.ofMillis(8));
    assertThat(backoff.nextSleepIntervalAfterIdle()).isEqualTo(Duration.ofMillis(16));
    assertThat(backoff.nextSleepIntervalAfterIdle()).isEqualTo(Duration.ofMillis(32));
    assertThat(backoff.nextSleepIntervalAfterIdle()).isEqualTo(Duration.ofMillis(32));

    backoff.reset();

    assertThat(backoff.currentInterval()).isEqualTo(Duration.ofMillis(1));
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

  private static final class FakeRuntime implements SourceRuntime<TestTransaction> {
    private final Deque<TestTransaction> pendingTransactions;
    private final List<TestTransaction> acknowledgedTransactions = new ArrayList<>();

    private FakeRuntime(List<TestTransaction> pendingTransactions) {
      this.pendingTransactions = new ArrayDeque<>(pendingTransactions);
    }

    @Override
    public Optional<TestTransaction> readPendingTransaction() {
      return Optional.ofNullable(pendingTransactions.pollFirst());
    }

    @Override
    public void acknowledge(TestTransaction transaction) {
      acknowledgedTransactions.add(transaction);
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
    private final List<Integer> pendingRequestCounts = new ArrayList<>();

    @Override
    public void onPendingRequestCountChanged(int pendingRequestCount) {
      pendingRequestCounts.add(pendingRequestCount);
    }
  }

  private static final class FakeCoordinator implements DumpRequestCoordinator<TestTransaction> {
    private final Deque<ScheduledRequestBatch<TestTransaction>> batches = new ArrayDeque<>();
    private final List<ScheduledRequestBatch<TestTransaction>> acknowledgedBatches = new ArrayList<>();
    private Runnable onCoordinate = () -> {};

    private FakeCoordinator(List<ScheduledRequestBatch<TestTransaction>> batches) {
      this.batches.addAll(batches);
    }

    @Override
    public Optional<ScheduledRequestBatch<TestTransaction>> coordinateNextBatch() {
      onCoordinate.run();
      return Optional.ofNullable(batches.pollFirst());
    }

    @Override
    public void acknowledgeCompletedBatch(ScheduledRequestBatch<TestTransaction> batch) {
      acknowledgedBatches.add(batch);
    }

    @Override
    public int pendingRequestCount() {
      return batches.size();
    }
  }
}
