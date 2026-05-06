package io.github.aandreakis.dblog.runtime.host;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
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
import io.github.aandreakis.dblog.testsupport.ChangeEventTestFixtures;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class DbLogRuntimeHostLifecycleTests {
  @Test
  void drainsStreamingThroughOwnedSessionAndAcknowledgesCheckpoint() throws Exception {
    FakeRuntime runtime =
        new FakeRuntime(
            List.of(
                new TestTransaction(
                    "tx-1",
                    new ComparablePosition(1),
                    List.of(
                        ChangeEventTestFixtures.fromRowMaps(
                            new TableId("source", "appdb", "widgets"),
                            OperationType.UPDATE,
                            CaptureOrigin.LOG,
                            Map.of("id", 1),
                            null,
                            Map.of("id", 1),
                            new OpaqueSourcePosition("source:1"),
                            "tx-1",
                            null)))));
    RecordingSink sink = new RecordingSink();
    DbLogRuntimeHostLifecycle<TestTransaction> lifecycle =
        new DbLogRuntimeHostLifecycle<>(
            new RuntimeSession<>(runtime, new NoopChunkReader(), sink),
            new CheckpointFlushPolicy(100, Duration.ofMillis(5)),
            RuntimeLoopObserver.noop());

    int drained = lifecycle.runStreamingDrain("startup-drain", Duration.ofMillis(20));

    assertThat(drained).isEqualTo(1);
    assertThat(runtime.acknowledged).hasSize(1);
    assertThat(sink.events).hasSize(1);
  }

  @Test
  void stopPreventsFurtherDrainWork() throws Exception {
    FakeRuntime runtime = new FakeRuntime(List.of());
    DbLogRuntimeHostLifecycle<TestTransaction> lifecycle =
        new DbLogRuntimeHostLifecycle<>(new RuntimeSession<>(runtime, new NoopChunkReader(), new RecordingSink()));

    lifecycle.requestStop();

    assertThat(lifecycle.runStreamingDrain("idle", Duration.ofMillis(10))).isZero();
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
    private final Deque<TestTransaction> pending = new ArrayDeque<>();
    private final List<TestTransaction> acknowledged = new ArrayList<>();

    private FakeRuntime(List<TestTransaction> pendingTransactions) {
      pending.addAll(pendingTransactions);
    }

    @Override
    public Optional<TestTransaction> readPendingTransaction() {
      return Optional.ofNullable(pending.pollFirst());
    }

    @Override
    public void acknowledge(TestTransaction transaction) {
      acknowledged.add(transaction);
    }
  }

  private static final class RecordingSink implements ChangeEventSink {
    private final List<ChangeEvent> events = new ArrayList<>();

    @Override
    public void appendEvents(List<ChangeEvent> events) {
      this.events.addAll(events);
    }
  }

  private static final class NoopChunkReader implements SourceChunkReader {
    @Override
    public Optional<io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple>
        tableScanUpperBoundPrimaryKeyTuple(
            java.sql.Connection connection,
            io.github.aandreakis.dblog.core.schema.TableSchema schema) {
      return Optional.empty();
    }

    @Override
    public Optional<io.github.aandreakis.dblog.core.reconcile.Chunk> nextTableChunk(
        java.sql.Connection connection,
        String jobId,
        io.github.aandreakis.dblog.core.schema.TableSchema schema,
        io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple startAfterPrimaryKey,
        io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple stopAtPrimaryKey,
        int chunkSize) {
      return Optional.empty();
    }

    @Override
    public Optional<io.github.aandreakis.dblog.core.reconcile.Chunk>
        targetedPrimaryKeyTuples(
            java.sql.Connection connection,
            String jobId,
            io.github.aandreakis.dblog.core.schema.TableSchema schema,
            List<io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple>
                requestedPrimaryKeys) {
      return Optional.empty();
    }
  }
}
