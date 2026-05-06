package io.github.aandreakis.dblog.runtime.telemetry;

import io.github.aandreakis.dblog.adapter.api.ChunkReadWithWindowWork;
import io.github.aandreakis.dblog.adapter.api.ChunkReadWork;
import io.github.aandreakis.dblog.adapter.api.CommittedTransactionIngress;
import io.github.aandreakis.dblog.adapter.api.OpenedSourceRuntime;
import io.github.aandreakis.dblog.adapter.api.RuntimeStatusInspectable;
import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.api.SourceRuntime;
import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.adapter.api.WatermarkWindowResult;
import io.github.aandreakis.dblog.adapter.api.WatermarkWindowRuntime;
import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.request.DumpRequestCoordinator;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.request.ScheduledRequestBatch;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.runtime.sql.SourceFlowControlSnapshot;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** Non-invasive measurement wrappers for source capture, chunking, and coordinator timing. */
public final class RuntimeMeasurementWrappers {
  private RuntimeMeasurementWrappers() {}

  public static OpenedSourceRuntime<? extends SourceTransaction<?>> instrumentOpenedRuntime(
      String adapter,
      MeterRegistry meterRegistry,
      OpenedSourceRuntime<? extends SourceTransaction<?>> opened) {
    Objects.requireNonNull(opened, "opened");
    if (meterRegistry == null) {
      return opened;
    }
    @SuppressWarnings({"rawtypes", "unchecked"})
    OpenedSourceRuntime<? extends SourceTransaction<?>> instrumented =
        new OpenedSourceRuntime(
            instrumentRuntime(adapter, meterRegistry, opened.runtime()),
            instrumentChunkReader(adapter, meterRegistry, opened.chunkReader()),
            opened.loadedCheckpointDisplayValue());
    return instrumented;
  }

  public static <TX extends SourceTransaction<?>> SourceRuntime<TX> instrumentRuntime(
      String adapter, MeterRegistry meterRegistry, SourceRuntime<TX> runtime) {
    Objects.requireNonNull(runtime, "runtime");
    if (meterRegistry == null) {
      return runtime;
    }
    if (runtime instanceof WatermarkWindowRuntime<TX> watermarkRuntime) {
      if (runtime instanceof CommittedTransactionIngress<?> ingress) {
        @SuppressWarnings("unchecked")
        CommittedTransactionIngress<TX> typedIngress = (CommittedTransactionIngress<TX>) ingress;
        return new InstrumentedIngressWatermarkWindowRuntime<>(
            adapter, meterRegistry, watermarkRuntime, typedIngress);
      }
      return new InstrumentedWatermarkWindowRuntime<>(adapter, meterRegistry, watermarkRuntime);
    }
    if (runtime instanceof CommittedTransactionIngress<?> ingress) {
      @SuppressWarnings("unchecked")
      CommittedTransactionIngress<TX> typedIngress = (CommittedTransactionIngress<TX>) ingress;
      return new InstrumentedIngressSourceRuntime<>(adapter, meterRegistry, runtime, typedIngress);
    }
    return new InstrumentedSourceRuntime<>(adapter, meterRegistry, runtime);
  }

  public static SourceChunkReader instrumentChunkReader(
      String adapter, MeterRegistry meterRegistry, SourceChunkReader chunkReader) {
    Objects.requireNonNull(chunkReader, "chunkReader");
    if (meterRegistry == null) {
      return chunkReader;
    }
    return new InstrumentedSourceChunkReader(adapter, meterRegistry, chunkReader);
  }

  public static <TX extends SourceTransaction<?>> DumpRequestCoordinator<TX> instrumentCoordinator(
      String adapter, MeterRegistry meterRegistry, DumpRequestCoordinator<TX> coordinator) {
    Objects.requireNonNull(coordinator, "coordinator");
    if (meterRegistry == null) {
      return coordinator;
    }
    return new InstrumentedDumpRequestCoordinator<>(adapter, meterRegistry, coordinator);
  }

  private abstract static class AbstractInstrumentedSourceRuntime<TX extends SourceTransaction<?>>
      implements SourceRuntime<TX>, RuntimeStatusInspectable {
    private final SourceRuntime<TX> delegate;
    private final RuntimeStatusInspectable statusInspectable;
    private final Counter pollCalls;
    private final Counter emptyPolls;
    private final Counter transactionsRead;
    private final Counter eventsRead;
    private final Counter pollFailures;
    /**
     * Time spent inside one {@code readPendingTransaction()} call on the live streaming
     * session (pgoutput or binlog decode). On a transaction-bearing return, this covers
     * the full decode path for that committed transaction — the longer the transaction,
     * the longer this takes. On an empty return, this is the wire-poll cost only. A high
     * max relative to the average typically reflects a large single committed tx, not a
     * stalled wire; cross-reference with {@code transactions} and {@code events} counters.
     */
    private final Timer transactionDecodeDuration;
    private final Counter acknowledgeCalls;
    private final Counter acknowledgeFailures;
    private final Timer acknowledgeDuration;
    private final Counter heartbeatsWritten;
    private final Counter heartbeatsSkipped;
    private final Counter heartbeatFailures;
    private final Timer heartbeatDuration;

    private AbstractInstrumentedSourceRuntime(
        String adapter, MeterRegistry meterRegistry, SourceRuntime<TX> delegate) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
      this.statusInspectable =
          delegate instanceof RuntimeStatusInspectable inspectable ? inspectable : null;
      MeasurementMeters meters = new MeasurementMeters(Objects.requireNonNull(meterRegistry), adapter);
      this.pollCalls = meters.counter("dblog.runtime.source_capture.polls.total");
      this.emptyPolls = meters.counter("dblog.runtime.source_capture.empty_polls.total");
      this.transactionsRead = meters.counter("dblog.runtime.source_capture.transactions.total");
      this.eventsRead = meters.counter("dblog.runtime.source_capture.events.total");
      this.pollFailures = meters.counter("dblog.runtime.source_capture.poll.failures.total");
      this.transactionDecodeDuration =
          meters.timer("dblog.runtime.source_capture.transaction_decode.duration");
      this.acknowledgeCalls = meters.counter("dblog.runtime.source_capture.acknowledges.total");
      this.acknowledgeFailures =
          meters.counter("dblog.runtime.source_capture.acknowledge.failures.total");
      this.acknowledgeDuration =
          meters.timer("dblog.runtime.source_capture.acknowledge.duration");
      this.heartbeatsWritten =
          meters.counter("dblog.runtime.source_capture.heartbeats.written.total");
      this.heartbeatsSkipped =
          meters.counter("dblog.runtime.source_capture.heartbeats.skipped.total");
      this.heartbeatFailures =
          meters.counter("dblog.runtime.source_capture.heartbeat.failures.total");
      this.heartbeatDuration = meters.timer("dblog.runtime.source_capture.heartbeat.duration");
    }

    @Override
    public Optional<TX> readPendingTransaction() throws SQLException {
      pollCalls.increment();
      long startedAt = System.nanoTime();
      try {
        Optional<TX> transaction = delegate.readPendingTransaction();
        transactionDecodeDuration.record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS);
        if (transaction.isPresent()) {
          TX committed = transaction.orElseThrow();
          transactionsRead.increment();
          eventsRead.increment(committed.eventCount());
        } else {
          emptyPolls.increment();
        }
        return transaction;
      } catch (SQLException | RuntimeException failure) {
        pollFailures.increment();
        transactionDecodeDuration.record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS);
        throw failure;
      }
    }

    @Override
    public void acknowledge(TX transaction) throws SQLException {
      acknowledgeCalls.increment();
      long startedAt = System.nanoTime();
      try {
        delegate.acknowledge(transaction);
        acknowledgeDuration.record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS);
      } catch (SQLException | RuntimeException failure) {
        acknowledgeFailures.increment();
        acknowledgeDuration.record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS);
        throw failure;
      }
    }

    @Override
    public boolean emitHeartbeatIfDue(Instant heartbeatTime, Duration minimumInterval)
        throws SQLException {
      long startedAt = System.nanoTime();
      try {
        boolean emitted = delegate.emitHeartbeatIfDue(heartbeatTime, minimumInterval);
        heartbeatDuration.record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS);
        if (emitted) {
          heartbeatsWritten.increment();
        } else {
          heartbeatsSkipped.increment();
        }
        return emitted;
      } catch (SQLException | RuntimeException failure) {
        heartbeatFailures.increment();
        heartbeatDuration.record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS);
        throw failure;
      }
    }

    @Override
    public List<TableSchema> currentCapturedSchemas() {
      return delegate.currentCapturedSchemas();
    }

    @Override
    public String lastAcknowledgedCheckpointDisplayValue() {
      return statusInspectable == null
          ? null
          : statusInspectable.lastAcknowledgedCheckpointDisplayValue();
    }

    @Override
    public int pendingTransactionCount() {
      return statusInspectable == null ? -1 : statusInspectable.pendingTransactionCount();
    }

    @Override
    public int capturedTableCount() {
      return statusInspectable == null ? -1 : statusInspectable.capturedTableCount();
    }

    @Override
    public SourceFlowControlSnapshot sourceFlowControlSnapshot() {
      return statusInspectable == null
          ? SourceFlowControlSnapshot.unavailable()
          : statusInspectable.sourceFlowControlSnapshot();
    }

    @Override
    public void close() throws Exception {
      delegate.close();
    }
  }

  private static final class InstrumentedSourceRuntime<TX extends SourceTransaction<?>>
      extends AbstractInstrumentedSourceRuntime<TX> {
    private InstrumentedSourceRuntime(
        String adapter, MeterRegistry meterRegistry, SourceRuntime<TX> delegate) {
      super(adapter, meterRegistry, delegate);
    }
  }

  private static final class InstrumentedIngressSourceRuntime<TX extends SourceTransaction<?>>
      extends AbstractInstrumentedSourceRuntime<TX> implements CommittedTransactionIngress<TX> {
    private final CommittedTransactionIngress<TX> ingress;

    private InstrumentedIngressSourceRuntime(
        String adapter,
        MeterRegistry meterRegistry,
        SourceRuntime<TX> delegate,
        CommittedTransactionIngress<TX> ingress) {
      super(adapter, meterRegistry, delegate);
      this.ingress = Objects.requireNonNull(ingress, "ingress");
    }

    @Override
    public void enqueueCommittedTransaction(TX transaction) {
      ingress.enqueueCommittedTransaction(transaction);
    }
  }

  private static class InstrumentedWatermarkWindowRuntime<TX extends SourceTransaction<?>>
      extends AbstractInstrumentedSourceRuntime<TX> implements WatermarkWindowRuntime<TX> {
    private final WatermarkWindowRuntime<TX> delegate;
    private final Counter chunkReadCalls;
    private final Counter chunkReadFailures;
    private final Timer chunkReadDuration;
    private final Counter chunkReadWindowCalls;
    private final Counter chunkReadWindowFailures;
    private final Timer chunkReadWindowDuration;

    private InstrumentedWatermarkWindowRuntime(
        String adapter, MeterRegistry meterRegistry, WatermarkWindowRuntime<TX> delegate) {
      super(adapter, meterRegistry, delegate);
      this.delegate = Objects.requireNonNull(delegate, "delegate");
      MeasurementMeters meters = new MeasurementMeters(meterRegistry, adapter);
      this.chunkReadCalls = meters.counter("dblog.runtime.chunking.chunk_read.calls.total");
      this.chunkReadFailures = meters.counter("dblog.runtime.chunking.chunk_read.failures.total");
      this.chunkReadDuration = meters.timer("dblog.runtime.chunking.chunk_read.duration");
      this.chunkReadWindowCalls =
          meters.counter("dblog.runtime.chunking.chunk_read_window.calls.total");
      this.chunkReadWindowFailures =
          meters.counter("dblog.runtime.chunking.chunk_read_window.failures.total");
      this.chunkReadWindowDuration =
          meters.timer("dblog.runtime.chunking.chunk_read_window.duration");
    }

    @Override
    public <T> T executeChunkRead(SourceChunkReader reader, ChunkReadWork<T> work)
        throws SQLException {
      chunkReadCalls.increment();
      long startedAt = System.nanoTime();
      try {
        T value = delegate.executeChunkRead(reader, work);
        chunkReadDuration.record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS);
        return value;
      } catch (SQLException | RuntimeException failure) {
        chunkReadFailures.increment();
        chunkReadDuration.record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS);
        throw failure;
      }
    }

    @Override
    public <T> WatermarkWindowResult<T> executeChunkReadInWatermarkWindow(
        SourceChunkReader reader, ChunkReadWithWindowWork<T> work) throws SQLException {
      chunkReadWindowCalls.increment();
      long startedAt = System.nanoTime();
      try {
        WatermarkWindowResult<T> value = delegate.executeChunkReadInWatermarkWindow(reader, work);
        chunkReadWindowDuration.record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS);
        return value;
      } catch (SQLException | RuntimeException failure) {
        chunkReadWindowFailures.increment();
        chunkReadWindowDuration.record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS);
        throw failure;
      }
    }

    @Override
    public void updateCapturedSchema(TableSchema schema) {
      delegate.updateCapturedSchema(schema);
    }

    @Override
    public String currentRunId() {
      return delegate.currentRunId();
    }
  }

  private static final class InstrumentedIngressWatermarkWindowRuntime<
          TX extends SourceTransaction<?>>
      extends InstrumentedWatermarkWindowRuntime<TX> implements CommittedTransactionIngress<TX> {
    private final CommittedTransactionIngress<TX> ingress;

    private InstrumentedIngressWatermarkWindowRuntime(
        String adapter,
        MeterRegistry meterRegistry,
        WatermarkWindowRuntime<TX> delegate,
        CommittedTransactionIngress<TX> ingress) {
      super(adapter, meterRegistry, delegate);
      this.ingress = Objects.requireNonNull(ingress, "ingress");
    }

    @Override
    public void enqueueCommittedTransaction(TX transaction) {
      ingress.enqueueCommittedTransaction(transaction);
    }
  }

  private static final class InstrumentedSourceChunkReader implements SourceChunkReader {
    private final SourceChunkReader delegate;
    private final Counter upperBoundCalls;
    private final Counter upperBoundMisses;
    private final Counter upperBoundFailures;
    private final Timer upperBoundDuration;
    private final Counter nextTableCalls;
    private final Counter nextTableEmpty;
    private final Counter nextTableRows;
    private final Counter nextTableFinal;
    private final Counter nextTableFailures;
    private final Timer nextTableDuration;
    private final Counter targetedCalls;
    private final Counter targetedEmpty;
    private final Counter targetedRequestedKeys;
    private final Counter targetedRows;
    private final Counter targetedFailures;
    private final Timer targetedDuration;

    private InstrumentedSourceChunkReader(
        String adapter, MeterRegistry meterRegistry, SourceChunkReader delegate) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
      MeasurementMeters meters = new MeasurementMeters(meterRegistry, adapter);
      this.upperBoundCalls = meters.counter("dblog.runtime.chunking.upper_bound_reads.total");
      this.upperBoundMisses =
          meters.counter("dblog.runtime.chunking.upper_bound_reads.empty.total");
      this.upperBoundFailures =
          meters.counter("dblog.runtime.chunking.upper_bound_reads.failures.total");
      this.upperBoundDuration =
          meters.timer("dblog.runtime.chunking.upper_bound_read.duration");
      this.nextTableCalls =
          meters.counter("dblog.runtime.chunking.next_table_chunk.calls.total");
      this.nextTableEmpty =
          meters.counter("dblog.runtime.chunking.next_table_chunk.empty.total");
      this.nextTableRows =
          meters.counter("dblog.runtime.chunking.next_table_chunk.rows.total");
      this.nextTableFinal =
          meters.counter("dblog.runtime.chunking.next_table_chunk.final.total");
      this.nextTableFailures =
          meters.counter("dblog.runtime.chunking.next_table_chunk.failures.total");
      this.nextTableDuration =
          meters.timer("dblog.runtime.chunking.next_table_chunk.duration");
      this.targetedCalls =
          meters.counter("dblog.runtime.chunking.targeted_primary_keys.calls.total");
      this.targetedEmpty =
          meters.counter("dblog.runtime.chunking.targeted_primary_keys.empty.total");
      this.targetedRequestedKeys =
          meters.counter("dblog.runtime.chunking.targeted_primary_keys.requested_keys.total");
      this.targetedRows =
          meters.counter("dblog.runtime.chunking.targeted_primary_keys.rows.total");
      this.targetedFailures =
          meters.counter("dblog.runtime.chunking.targeted_primary_keys.failures.total");
      this.targetedDuration =
          meters.timer("dblog.runtime.chunking.targeted_primary_keys.duration");
    }

    @Override
    public Optional<PrimaryKeyTuple> tableScanUpperBoundPrimaryKeyTuple(
        Connection connection, TableSchema schema) throws SQLException {
      upperBoundCalls.increment();
      long startedAt = System.nanoTime();
      try {
        Optional<PrimaryKeyTuple> value = delegate.tableScanUpperBoundPrimaryKeyTuple(connection, schema);
        upperBoundDuration.record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS);
        if (value.isEmpty()) {
          upperBoundMisses.increment();
        }
        return value;
      } catch (SQLException | RuntimeException failure) {
        upperBoundFailures.increment();
        upperBoundDuration.record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS);
        throw failure;
      }
    }

    @Override
    public Optional<Chunk> nextTableChunk(
        Connection connection,
        String jobId,
        TableSchema schema,
        PrimaryKeyTuple startAfterPrimaryKey,
        PrimaryKeyTuple stopAtPrimaryKey,
        int chunkSize)
        throws SQLException {
      nextTableCalls.increment();
      long startedAt = System.nanoTime();
      try {
        Optional<Chunk> chunk =
            delegate.nextTableChunk(
                connection, jobId, schema, startAfterPrimaryKey, stopAtPrimaryKey, chunkSize);
        nextTableDuration.record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS);
        if (chunk.isPresent()) {
          Chunk resolved = chunk.orElseThrow();
          nextTableRows.increment(resolved.rows().size());
          if (resolved.finalChunk()) {
            nextTableFinal.increment();
          }
        } else {
          nextTableEmpty.increment();
        }
        return chunk;
      } catch (SQLException | RuntimeException failure) {
        nextTableFailures.increment();
        nextTableDuration.record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS);
        throw failure;
      }
    }

    @Override
    public Optional<Chunk> targetedPrimaryKeyTuples(
        Connection connection,
        String jobId,
        TableSchema schema,
        List<PrimaryKeyTuple> requestedPrimaryKeys)
        throws SQLException {
      targetedCalls.increment();
      targetedRequestedKeys.increment(requestedPrimaryKeys.size());
      long startedAt = System.nanoTime();
      try {
        Optional<Chunk> chunk =
            delegate.targetedPrimaryKeyTuples(connection, jobId, schema, requestedPrimaryKeys);
        targetedDuration.record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS);
        if (chunk.isPresent()) {
          targetedRows.increment(chunk.orElseThrow().rows().size());
        } else {
          targetedEmpty.increment();
        }
        return chunk;
      } catch (SQLException | RuntimeException failure) {
        targetedFailures.increment();
        targetedDuration.record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS);
        throw failure;
      }
    }

  }

  private static final class InstrumentedDumpRequestCoordinator<TX extends SourceTransaction<?>>
      implements DumpRequestCoordinator<TX> {
    private final DumpRequestCoordinator<TX> delegate;
    private final Counter coordinateCalls;
    private final Counter coordinateEmpty;
    private final Counter coordinateFailures;
    private final Counter tableBatches;
    private final Counter targetedRepairBatches;
    private final Counter allTablesBatches;
    private final Timer coordinateDuration;
    private final Counter acknowledgeCalls;
    private final Counter acknowledgeFailures;
    private final Timer acknowledgeDuration;

    private InstrumentedDumpRequestCoordinator(
        String adapter, MeterRegistry meterRegistry, DumpRequestCoordinator<TX> delegate) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
      MeasurementMeters meters = new MeasurementMeters(meterRegistry, adapter);
      this.coordinateCalls =
          meters.counter("dblog.runtime.request_coordinator.coordinate.calls.total");
      this.coordinateEmpty =
          meters.counter("dblog.runtime.request_coordinator.coordinate.empty.total");
      this.coordinateFailures =
          meters.counter("dblog.runtime.request_coordinator.coordinate.failures.total");
      this.tableBatches =
          meters.counter("dblog.runtime.request_coordinator.batches.table.total");
      this.targetedRepairBatches =
          meters.counter("dblog.runtime.request_coordinator.batches.primary_keys.total");
      this.allTablesBatches =
          meters.counter("dblog.runtime.request_coordinator.batches.all_tables.total");
      this.coordinateDuration =
          meters.timer("dblog.runtime.request_coordinator.coordinate.duration");
      this.acknowledgeCalls =
          meters.counter("dblog.runtime.request_coordinator.acknowledges.total");
      this.acknowledgeFailures =
          meters.counter("dblog.runtime.request_coordinator.acknowledge.failures.total");
      this.acknowledgeDuration =
          meters.timer("dblog.runtime.request_coordinator.acknowledge.duration");
    }

    @Override
    public Optional<ScheduledRequestBatch<TX>> coordinateNextBatch() {
      coordinateCalls.increment();
      long startedAt = System.nanoTime();
      try {
        Optional<ScheduledRequestBatch<TX>> batch = delegate.coordinateNextBatch();
        coordinateDuration.record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS);
        if (batch.isEmpty()) {
          coordinateEmpty.increment();
        } else {
          DumpScope scope = batch.orElseThrow().request().scope();
          switch (scope) {
            case TABLE -> tableBatches.increment();
            case PRIMARY_KEYS -> targetedRepairBatches.increment();
            case ALL_TABLES -> allTablesBatches.increment();
          }
        }
        return batch;
      } catch (RuntimeException failure) {
        coordinateFailures.increment();
        coordinateDuration.record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS);
        throw failure;
      }
    }

    @Override
    public void acknowledgeCompletedBatch(ScheduledRequestBatch<TX> batch) {
      acknowledgeCalls.increment();
      long startedAt = System.nanoTime();
      try {
        delegate.acknowledgeCompletedBatch(batch);
        acknowledgeDuration.record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS);
      } catch (RuntimeException failure) {
        acknowledgeFailures.increment();
        acknowledgeDuration.record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS);
        throw failure;
      }
    }

    @Override
    public int pendingRequestCount() {
      return delegate.pendingRequestCount();
    }
  }

  private record MeasurementMeters(MeterRegistry meterRegistry, String adapter) {
    private MeasurementMeters {
      Objects.requireNonNull(meterRegistry, "meterRegistry");
      Objects.requireNonNull(adapter, "adapter");
    }

    private Counter counter(String name) {
      return meterRegistry.counter(name, "adapter", adapter);
    }

    private Timer timer(String name) {
      return Timer.builder(name).tags("adapter", adapter).register(meterRegistry);
    }
  }
}
