package io.github.aandreakis.dblog.runtime.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.api.CommittedTransactionIngress;
import io.github.aandreakis.dblog.adapter.api.OpenedSourceRuntime;
import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.adapter.api.WatermarkWindowResult;
import io.github.aandreakis.dblog.adapter.api.WatermarkWindowRuntime;
import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.model.WatermarkToken;
import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.reconcile.WatermarkWindow;
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.core.request.DumpRequestCoordinator;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.request.ScheduledRequestBatch;
import io.github.aandreakis.dblog.core.request.TargetedRepairBatch;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.testsupport.ChangeEventTestFixtures;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class RuntimeMeasurementWrappersTests {
  @Test
  void instrumentsSourceCaptureChunkingAndCoordinatorMetrics() throws Exception {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    TableSchema schema = schema();
    TestTransaction transaction = transaction(schema.tableId());
    TestRuntime runtime = new TestRuntime(transaction);
    TestChunkReader chunkReader = new TestChunkReader(schema);

    @SuppressWarnings("unchecked")
    OpenedSourceRuntime<TestTransaction> opened =
        (OpenedSourceRuntime<TestTransaction>)
            RuntimeMeasurementWrappers.instrumentOpenedRuntime(
                "mysql",
                meterRegistry,
                new OpenedSourceRuntime<>(runtime, chunkReader, "cp-1"));

    assertThat(opened.runtime()).isInstanceOf(CommittedTransactionIngress.class);
    assertThat(opened.runtime().readPendingTransaction()).contains(transaction);
    opened.runtime().acknowledge(transaction);
    assertThat(
            opened.runtime().emitHeartbeatIfDue(Instant.now(), Duration.ofSeconds(1)))
        .isTrue();
    assertThat(opened.chunkReader().tableScanUpperBoundPrimaryKeyTuple(null, schema))
        .map(io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple::literal)
        .contains("9");
    assertThat(
            opened
                .chunkReader()
                .nextTableChunk(
                    null, "job-1", schema, null, schema.primaryKeyTupleFromLiteral("9"), 100))
        .isPresent();
    assertThat(
            opened
                .chunkReader()
                .targetedPrimaryKeyTuples(
                    null, "job-1", schema, schema.primaryKeyTuplesFromLiterals(List.of("1", "2"))))
        .isPresent();

    DumpRequest request =
        DumpRequest.fromPrimaryKeyLiterals(
            "repair-1", DumpScope.PRIMARY_KEYS, schema.tableId(), schema, List.of("1"));
    Chunk targetedChunk =
        Chunk.fromMapRows(
            request.requestId(),
            schema.tableId().displayName(),
            schema,
            null,
            List.of(Map.of("id", "1", "name", "one")),
            "1",
            true);
    ScheduledRequestBatch<TestTransaction> scheduled =
        ScheduledRequestBatch.targetedRepair(
            request,
            new TargetedRepairBatch<>(
                request,
                targetedChunk,
                new WatermarkWindow(new WatermarkToken("lw"), new WatermarkToken("hw")),
                List.of(),
                List.of(transaction.events().getFirst()),
                transaction));
    DumpRequestCoordinator<TestTransaction> coordinator =
        RuntimeMeasurementWrappers.instrumentCoordinator(
            "mysql", meterRegistry, new TestCoordinator(Optional.of(scheduled)));

    assertThat(coordinator.coordinateNextBatch()).contains(scheduled);
    coordinator.acknowledgeCompletedBatch(scheduled);

    RuntimeMeasurementMetricsSnapshot snapshot =
        RuntimeMeasurementMetricsSnapshot.capture(meterRegistry, "mysql");

    assertThat(snapshot.sourceCapturePolls()).isEqualTo(1.0d);
    assertThat(snapshot.sourceCaptureTransactions()).isEqualTo(1.0d);
    assertThat(snapshot.sourceCaptureEvents()).isEqualTo(1.0d);
    assertThat(snapshot.sourceCaptureAcknowledgeCalls()).isEqualTo(1.0d);
    assertThat(snapshot.sourceCaptureHeartbeatsWritten()).isEqualTo(1.0d);
    assertThat(snapshot.chunkUpperBoundReads()).isEqualTo(1.0d);
    assertThat(snapshot.nextTableChunkCalls()).isEqualTo(1.0d);
    assertThat(snapshot.nextTableChunkRows()).isEqualTo(2.0d);
    assertThat(snapshot.nextTableChunkFinal()).isEqualTo(1.0d);
    assertThat(snapshot.targetedPrimaryKeyChunkCalls()).isEqualTo(1.0d);
    assertThat(snapshot.targetedPrimaryKeyRequestedKeys()).isEqualTo(2.0d);
    assertThat(snapshot.targetedPrimaryKeyRows()).isEqualTo(1.0d);
    assertThat(snapshot.coordinateCalls()).isEqualTo(1.0d);
    assertThat(snapshot.acknowledgeCalls()).isEqualTo(1.0d);
  }

  private static TableSchema schema() {
    return TableSchema.create(
        new TableId("sourceA", "appdb", "widgets"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
        Instant.parse("2026-04-10T00:00:00Z"));
  }

  private static TestTransaction transaction(TableId tableId) {
    return new TestTransaction(
        "tx-1",
        new OpaqueSourcePosition("pos-1"),
        List.of(
            ChangeEventTestFixtures.fromRowMaps(
                tableId,
                OperationType.UPDATE,
                CaptureOrigin.LOG,
                Map.of("id", "1"),
                null,
                Map.of("id", "1", "name", "one"),
                new OpaqueSourcePosition("pos-1"),
                "tx-1",
                null)));
  }

  private record TestTransaction(
      String transactionId, SourcePosition checkpointPosition, List<ChangeEvent> events)
      implements SourceTransaction<SourcePosition> {
    @Override
    public Instant commitTimestamp() {
      return Instant.now();
    }
  }

  private static final class TestRuntime
      implements WatermarkWindowRuntime<TestTransaction>, CommittedTransactionIngress<TestTransaction> {
    private final Optional<TestTransaction> transaction;

    private TestRuntime(TestTransaction transaction) {
      this.transaction = Optional.of(transaction);
    }

    @Override
    public Optional<TestTransaction> readPendingTransaction() {
      return transaction;
    }

    @Override
    public void acknowledge(TestTransaction transaction) {}

    @Override
    public boolean emitHeartbeatIfDue(Instant heartbeatTime, Duration minimumInterval) {
      return true;
    }

    @Override
    public <T> T executeChunkRead(
        SourceChunkReader reader, io.github.aandreakis.dblog.adapter.api.ChunkReadWork<T> work) {
      return null;
    }

    @Override
    public <T> WatermarkWindowResult<T> executeChunkReadInWatermarkWindow(
        SourceChunkReader reader,
        io.github.aandreakis.dblog.adapter.api.ChunkReadWithWindowWork<T> work) {
      return new WatermarkWindowResult<>(
          null, new WatermarkWindow(new WatermarkToken("lw"), new WatermarkToken("hw")));
    }

    @Override
    public void enqueueCommittedTransaction(TestTransaction transaction) {}
  }

  private static final class TestChunkReader implements SourceChunkReader {
    private final TableSchema schema;

    private TestChunkReader(TableSchema schema) {
      this.schema = schema;
    }

    @Override
    public Optional<io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple>
        tableScanUpperBoundPrimaryKeyTuple(Connection connection, TableSchema schema) {
      return Optional.of(schema.primaryKeyTupleFromLiteral("9"));
    }

    @Override
    public Optional<Chunk> nextTableChunk(
        Connection connection,
        String jobId,
        TableSchema schema,
        io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple startAfterPrimaryKey,
        io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple stopAtPrimaryKey,
        int chunkSize) {
      return Optional.of(
          Chunk.fromMapRows(
              jobId,
              schema.tableId().displayName(),
              this.schema,
              startAfterPrimaryKey,
              List.of(Map.of("id", "1", "name", "one"), Map.of("id", "2", "name", "two")),
              schema.primaryKeyTupleFromLiteral("2"),
              true));
    }

    @Override
    public Optional<Chunk> targetedPrimaryKeyTuples(
        Connection connection,
        String jobId,
        TableSchema schema,
        List<io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple> requestedPrimaryKeys) {
      return Optional.of(
          Chunk.fromMapRows(
              jobId,
              schema.tableId().displayName(),
              this.schema,
              null,
              List.of(Map.of("id", "1", "name", "one")),
              schema.primaryKeyTupleFromLiteral("1"),
              true));
    }
  }

  private static final class TestCoordinator implements DumpRequestCoordinator<TestTransaction> {
    private final Optional<ScheduledRequestBatch<TestTransaction>> batch;

    private TestCoordinator(Optional<ScheduledRequestBatch<TestTransaction>> batch) {
      this.batch = batch;
    }

    @Override
    public Optional<ScheduledRequestBatch<TestTransaction>> coordinateNextBatch() {
      return batch;
    }

    @Override
    public void acknowledgeCompletedBatch(ScheduledRequestBatch<TestTransaction> batch) {}

    @Override
    public int pendingRequestCount() {
      return batch.isPresent() ? 1 : 0;
    }
  }
}
