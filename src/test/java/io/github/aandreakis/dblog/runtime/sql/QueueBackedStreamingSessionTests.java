package io.github.aandreakis.dblog.runtime.sql;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class QueueBackedStreamingSessionTests {
  @Test
  void enqueuesAcknowledgesAndTracksCapturedSchemas() {
    TableSchema schema =
        TableSchema.create(
            new TableId("sourceA", "appdb", "widgets"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-11T00:00:00Z"));
    AtomicReference<TestPosition> persisted = new AtomicReference<>();
    QueueBackedStreamingSession<TestTransaction, TestPosition> session =
        new QueueBackedStreamingSession<>(
            "mysql",
            4,
            List.of(schema),
            persisted::set,
            TestTransaction::checkpointPosition,
            new TestPosition("cp:0"));

    assertThat(session.lastAcknowledgedCheckpointDisplayValue()).isEqualTo("cp:0");
    assertThat(session.acknowledgedPositions()).containsExactly(new TestPosition("cp:0"));
    assertThat(session.acknowledgedCount()).isZero();

    TestTransaction transaction =
        new TestTransaction("tx-1", new TestPosition("cp:1"), List.of());
    session.enqueue(transaction);

    assertThat(session.pendingTransactionCount()).isEqualTo(1);
    assertThat(session.readPendingTransaction()).contains(transaction);
    assertThat(session.pendingTransactionCount()).isZero();

    session.acknowledge(transaction);
    assertThat(persisted.get()).isEqualTo(new TestPosition("cp:1"));
    assertThat(session.lastAcknowledgedCheckpointDisplayValue()).isEqualTo("cp:1");
    assertThat(session.acknowledgedPositions()).containsExactly(new TestPosition("cp:1"));
    assertThat(session.acknowledgedCount()).isEqualTo(1L);
    assertThat(session.sourceFlowControlSnapshot().mode())
        .isEqualTo(SourceFlowControlSnapshot.Mode.BOUNDED_QUEUE);
    assertThat(session.currentCapturedSchemas()).containsExactly(schema);
  }

  @Test
  void retainsOnlyLatestAcknowledgedCheckpoint() {
    TableSchema schema =
        TableSchema.create(
            new TableId("sourceA", "appdb", "widgets"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-11T00:00:00Z"));
    QueueBackedStreamingSession<TestTransaction, TestPosition> session =
        new QueueBackedStreamingSession<>(
            "mysql",
            4,
            List.of(schema),
            ignored -> {},
            TestTransaction::checkpointPosition,
            null);

    session.acknowledge(new TestTransaction("tx-1", new TestPosition("cp:1"), List.of()));
    session.acknowledge(new TestTransaction("tx-2", new TestPosition("cp:2"), List.of()));

    assertThat(session.acknowledgedPositions()).containsExactly(new TestPosition("cp:2"));
    assertThat(session.lastAcknowledgedCheckpointDisplayValue()).isEqualTo("cp:2");
    assertThat(session.acknowledgedCount()).isEqualTo(2L);
  }

  private record TestTransaction(
      String transactionId, TestPosition checkpointPosition, List<ChangeEvent> events)
      implements io.github.aandreakis.dblog.adapter.api.SourceTransaction<TestPosition> {
    @Override
    public Instant commitTimestamp() {
      return Instant.EPOCH;
    }
  }

  private record TestPosition(String displayValue)
      implements SourcePosition, Comparable<TestPosition> {
    @Override
    public int compareTo(TestPosition other) {
      return displayValue.compareTo(other.displayValue);
    }
  }
}
