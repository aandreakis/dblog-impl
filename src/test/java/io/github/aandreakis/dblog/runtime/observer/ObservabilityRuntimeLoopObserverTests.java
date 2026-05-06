package io.github.aandreakis.dblog.runtime.observer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.model.WatermarkToken;
import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.reconcile.WatermarkWindow;
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.request.ScheduledRequestBatch;
import io.github.aandreakis.dblog.core.request.TargetedRepairBatch;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ObservabilityRuntimeLoopObserverTests {

  @Test
  void incrementsTransactionAndEventCountersOnPersistedHook() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    ObservabilityRuntimeLoopObserver<TestTx> observer =
        new ObservabilityRuntimeLoopObserver<>(new DbLogRuntimeObservability(), "mysql", registry);

    observer.onTransactionPersisted("streaming", new TestTx("tx-1", 3));
    observer.onTransactionPersisted("streaming", new TestTx("tx-2", 5));

    assertThat(
            registry
                .counter("dblog.transactions.processed", "adapter", "mysql", "stage", "streaming")
                .count())
        .isEqualTo(2.0d);
    assertThat(
            registry
                .counter("dblog.events.processed", "adapter", "mysql", "stage", "streaming")
                .count())
        .isEqualTo(8.0d);
  }

  @Test
  void recordsSinkAppendTimerAndEventCounterBySuccessOrFailure() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    ObservabilityRuntimeLoopObserver<TestTx> observer =
        new ObservabilityRuntimeLoopObserver<>(new DbLogRuntimeObservability(), "postgres", registry);

    observer.onEventSinkAppendSucceeded(
        "streaming", Instant.parse("2026-03-01T00:00:00Z"), 10, Duration.ofMillis(7).toNanos());
    observer.onEventSinkAppendFailed(
        "streaming",
        Instant.parse("2026-03-01T00:00:01Z"),
        4,
        Duration.ofMillis(3).toNanos(),
        new IllegalStateException("boom"));

    assertThat(
            registry
                .timer(
                    "dblog.sink.append.seconds",
                    "adapter", "postgres",
                    "stage", "streaming",
                    "outcome", "success")
                .count())
        .isEqualTo(1L);
    assertThat(
            registry
                .timer(
                    "dblog.sink.append.seconds",
                    "adapter", "postgres",
                    "stage", "streaming",
                    "outcome", "failure")
                .count())
        .isEqualTo(1L);
    assertThat(
            registry
                .counter(
                    "dblog.sink.events",
                    "adapter", "postgres",
                    "stage", "streaming",
                    "outcome", "success")
                .count())
        .isEqualTo(10.0d);
    assertThat(
            registry
                .counter(
                    "dblog.sink.events",
                    "adapter", "postgres",
                    "stage", "streaming",
                    "outcome", "failure")
                .count())
        .isEqualTo(4.0d);
  }

  @Test
  void tracksPendingRequestsGauge() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    ObservabilityRuntimeLoopObserver<TestTx> observer =
        new ObservabilityRuntimeLoopObserver<>(new DbLogRuntimeObservability(), "mysql", registry);

    observer.onPendingRequestCountChanged(3);
    assertThat(registry.find("dblog.pending.requests").gauge().value()).isEqualTo(3.0d);

    observer.onPendingRequestCountChanged(0);
    assertThat(registry.find("dblog.pending.requests").gauge().value()).isEqualTo(0.0d);

    // Negative counts are ignored (the coordinator uses -1 to mean "not yet known").
    observer.onPendingRequestCountChanged(-1);
    assertThat(registry.find("dblog.pending.requests").gauge().value()).isEqualTo(0.0d);
  }

  @Test
  void incrementsCheckpointAdvancedCounterWithReasonTag() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    ObservabilityRuntimeLoopObserver<TestTx> observer =
        new ObservabilityRuntimeLoopObserver<>(new DbLogRuntimeObservability(), "mysql", registry);

    observer.onCheckpointAdvanced("streaming", "batched-time", new TestTx("tx-1", 1));
    observer.onCheckpointAdvanced("streaming", "stage-end", new TestTx("tx-2", 1));
    observer.onCheckpointAdvanced("streaming", "batched-time", new TestTx("tx-3", 1));

    assertThat(
            registry
                .counter(
                    "dblog.checkpoint.advanced",
                    "adapter", "mysql",
                    "stage", "streaming",
                    "reason", "batched-time")
                .count())
        .isEqualTo(2.0d);
    assertThat(
            registry
                .counter(
                    "dblog.checkpoint.advanced",
                    "adapter", "mysql",
                    "stage", "streaming",
                    "reason", "stage-end")
                .count())
        .isEqualTo(1.0d);
  }

  @Test
  void incrementsRequestBatchesCounterAndNormalizesVariableStageLabels() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    ObservabilityRuntimeLoopObserver<TestTx> observer =
        new ObservabilityRuntimeLoopObserver<>(new DbLogRuntimeObservability(), "mysql", registry);

    TableSchema schema =
        TableSchema.create(
            new TableId("app", "public", "widgets"),
            List.of(new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false)),
            Instant.parse("2026-03-17T00:00:00Z"));
    Chunk chunk =
        Chunk.fromMapRows(
            "req-1",
            schema.tableId().displayName(),
            schema,
            (String) null,
            List.of(Map.of("id", "1")),
            (String) null,
            true);
    DumpRequest request =
        DumpRequest.fromPrimaryKeyLiterals(
            "req-1", DumpScope.PRIMARY_KEYS, schema.tableId(), schema, List.of("1"));
    TestTx checkpointTx = new TestTx("tx-1", 1);
    TargetedRepairBatch<TestTx> repairBatch =
        new TargetedRepairBatch<>(
            request,
            chunk,
            new WatermarkWindow(new WatermarkToken("lw"), new WatermarkToken("hw")),
            List.of(),
            List.of(),
            checkpointTx);
    ScheduledRequestBatch<TestTx> batch =
        ScheduledRequestBatch.targetedRepair(request, repairBatch);

    observer.onRequestBatch(batch);
    observer.onRequestBatch(batch);

    assertThat(
            registry
                .counter(
                    "dblog.request.batches",
                    "adapter", "mysql",
                    "scope", "PRIMARY_KEYS",
                    "finalBatch", "true")
                .count())
        .isEqualTo(2.0d);

    // Also confirm the stage-label normalization: request-batch-req-1 collapses to request-batch.
    observer.onTransactionPersisted("request-batch-req-1", new TestTx("tx-x", 2));
    observer.onTransactionPersisted("request-batch-req-2", new TestTx("tx-y", 3));
    assertThat(
            registry
                .counter(
                    "dblog.transactions.processed",
                    "adapter", "mysql",
                    "stage", "request-batch")
                .count())
        .isEqualTo(2.0d);
  }

  @Test
  void exposesSourceQueueDepthGauge() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    DbLogRuntimeObservability observability = new DbLogRuntimeObservability();
    new ObservabilityRuntimeLoopObserver<TestTx>(observability, "mysql", registry);

    // With no flow-control provider wired, queueDepth is 0 on the unavailable snapshot.
    assertThat(registry.find("dblog.source.queue.depth").gauge().value()).isEqualTo(0.0d);
  }

  private record TestTx(String transactionId, int eventCount)
      implements SourceTransaction<SourcePosition> {
    @Override
    public SourcePosition checkpointPosition() {
      return new OpaqueSourcePosition("lsn:" + transactionId);
    }

    @Override
    public Instant commitTimestamp() {
      return Instant.parse("2026-03-01T00:00:00Z");
    }

    @Override
    public List<ChangeEvent> events() {
      return List.of();
    }
  }
}
