package io.github.aandreakis.dblog.runtime.loop;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.checkpoint.CheckpointFlushPolicy;
import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.testsupport.ChangeEventTestFixtures;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class BufferedCheckpointAcknowledgerTests {
  @Test
  void flushesWhenBufferedEventThresholdIsReached() {
    MonotonicNanoClock clock = new MonotonicNanoClock();
    BufferedCheckpointAcknowledger<TestTransaction> acknowledger =
        new BufferedCheckpointAcknowledger<>(
            new CheckpointFlushPolicy(3, Duration.ofMinutes(5)), clock);

    assertThat(acknowledger.recordPersisted(transaction("1", 1, 1))).isEmpty();
    assertThat(acknowledger.recordPersisted(transaction("2", 2, 2)))
        .contains(transaction("2", 2, 2));
    assertThat(acknowledger.forceFlush()).isEmpty();
  }

  @Test
  void flushesWhenBufferedTimeThresholdIsReached() {
    MonotonicNanoClock clock = new MonotonicNanoClock();
    BufferedCheckpointAcknowledger<TestTransaction> acknowledger =
        new BufferedCheckpointAcknowledger<>(
            new CheckpointFlushPolicy(100, Duration.ofSeconds(5)), clock);

    TestTransaction tx = transaction("1", 1, 1);
    assertThat(acknowledger.recordPersisted(tx)).isEmpty();
    clock.advance(Duration.ofSeconds(4));
    assertThat(acknowledger.flushIfDue()).isEmpty();
    clock.advance(Duration.ofSeconds(1));
    assertThat(acknowledger.flushIfDue()).contains(tx);
  }

  @Test
  void markCheckpointAdvancedClearsOlderPendingCheckpoint() {
    MonotonicNanoClock clock = new MonotonicNanoClock();
    BufferedCheckpointAcknowledger<TestTransaction> acknowledger =
        new BufferedCheckpointAcknowledger<>(
            new CheckpointFlushPolicy(100, Duration.ofMinutes(5)), clock);

    TestTransaction buffered = transaction("1", 1, 10);
    acknowledger.recordPersisted(buffered);

    acknowledger.markCheckpointAdvanced(transaction("2", 2, 1));
    assertThat(acknowledger.forceFlush()).contains(buffered);

    acknowledger.recordPersisted(buffered);
    acknowledger.markCheckpointAdvanced(transaction("3", 3, 99));
    assertThat(acknowledger.forceFlush()).isEmpty();
  }

  private static TestTransaction transaction(String id, int eventCount, int checkpointOrdinal) {
    return new TestTransaction(
        id,
        new ComparablePosition(checkpointOrdinal),
        IntStream.range(0, eventCount)
            .mapToObj(
                index ->
                    ChangeEventTestFixtures.fromRowMaps(
                        new TableId("source", "appdb", "widgets"),
                        OperationType.INSERT,
                        CaptureOrigin.LOG,
                        java.util.Map.of("id", checkpointOrdinal),
                        null,
                        java.util.Map.of("id", checkpointOrdinal),
                        new OpaqueSourcePosition("source:" + checkpointOrdinal + ":" + index),
                        id,
                        null))
            .toList());
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

  private static final class MonotonicNanoClock implements java.util.function.LongSupplier {
    private long nanos;

    private void advance(Duration duration) {
      nanos += duration.toNanos();
    }

    @Override
    public long getAsLong() {
      return nanos;
    }
  }
}
