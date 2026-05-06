package io.github.aandreakis.dblog.runtime.telemetry;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Snapshot of measurement-oriented runtime counters and timers for one adapter. */
public record RuntimeMeasurementMetricsSnapshot(
    double sourceCapturePolls,
    double sourceCaptureEmptyPolls,
    double sourceCaptureTransactions,
    double sourceCaptureEvents,
    double sourceCapturePollFailures,
    // Time spent inside one readPendingTransaction() call. When the call returns a
    // committed transaction, this covers the full pgoutput/binlog decode for that
    // transaction — so the max scales with the largest single committed tx, not with
    // raw wire-poll latency. See RuntimeMeasurementWrappers.transactionDecodeDuration.
    double sourceCaptureTransactionDecodeDurationMillisTotal,
    double sourceCaptureTransactionDecodeDurationMillisMax,
    double sourceCaptureAcknowledgeCalls,
    double sourceCaptureAcknowledgeFailures,
    double sourceCaptureAcknowledgeDurationMillisTotal,
    double sourceCaptureAcknowledgeDurationMillisMax,
    double sourceCaptureHeartbeatsWritten,
    double sourceCaptureHeartbeatsSkipped,
    double sourceCaptureHeartbeatFailures,
    double sourceCaptureHeartbeatDurationMillisTotal,
    double sourceCaptureHeartbeatDurationMillisMax,
    double chunkUpperBoundReads,
    double chunkUpperBoundReadMisses,
    double chunkUpperBoundReadFailures,
    double chunkUpperBoundReadDurationMillisTotal,
    double chunkUpperBoundReadDurationMillisMax,
    double nextTableChunkCalls,
    double nextTableChunkEmpty,
    double nextTableChunkRows,
    double nextTableChunkFinal,
    double nextTableChunkFailures,
    double nextTableChunkDurationMillisTotal,
    double nextTableChunkDurationMillisMax,
    double targetedPrimaryKeyChunkCalls,
    double targetedPrimaryKeyChunkEmpty,
    double targetedPrimaryKeyRequestedKeys,
    double targetedPrimaryKeyRows,
    double targetedPrimaryKeyFailures,
    double targetedPrimaryKeyDurationMillisTotal,
    double targetedPrimaryKeyDurationMillisMax,
    double coordinateCalls,
    double coordinateEmpty,
    double coordinateFailures,
    double coordinateDurationMillisTotal,
    double coordinateDurationMillisMax,
    double acknowledgeCalls,
    double acknowledgeFailures,
    double acknowledgeDurationMillisTotal,
    double acknowledgeDurationMillisMax) {
  public static RuntimeMeasurementMetricsSnapshot capture(
      MeterRegistry meterRegistry, String adapter) {
    MeterRegistry registry = Objects.requireNonNull(meterRegistry, "meterRegistry");
    Objects.requireNonNull(adapter, "adapter");
    return new RuntimeMeasurementMetricsSnapshot(
        counter(registry, "dblog.runtime.source_capture.polls.total", adapter),
        counter(registry, "dblog.runtime.source_capture.empty_polls.total", adapter),
        counter(registry, "dblog.runtime.source_capture.transactions.total", adapter),
        counter(registry, "dblog.runtime.source_capture.events.total", adapter),
        counter(registry, "dblog.runtime.source_capture.poll.failures.total", adapter),
        timerTotalMillis(
            registry, "dblog.runtime.source_capture.transaction_decode.duration", adapter),
        timerMaxMillis(
            registry, "dblog.runtime.source_capture.transaction_decode.duration", adapter),
        counter(registry, "dblog.runtime.source_capture.acknowledges.total", adapter),
        counter(registry, "dblog.runtime.source_capture.acknowledge.failures.total", adapter),
        timerTotalMillis(registry, "dblog.runtime.source_capture.acknowledge.duration", adapter),
        timerMaxMillis(registry, "dblog.runtime.source_capture.acknowledge.duration", adapter),
        counter(registry, "dblog.runtime.source_capture.heartbeats.written.total", adapter),
        counter(registry, "dblog.runtime.source_capture.heartbeats.skipped.total", adapter),
        counter(registry, "dblog.runtime.source_capture.heartbeat.failures.total", adapter),
        timerTotalMillis(registry, "dblog.runtime.source_capture.heartbeat.duration", adapter),
        timerMaxMillis(registry, "dblog.runtime.source_capture.heartbeat.duration", adapter),
        counter(registry, "dblog.runtime.chunking.upper_bound_reads.total", adapter),
        counter(registry, "dblog.runtime.chunking.upper_bound_reads.empty.total", adapter),
        counter(registry, "dblog.runtime.chunking.upper_bound_reads.failures.total", adapter),
        timerTotalMillis(registry, "dblog.runtime.chunking.upper_bound_read.duration", adapter),
        timerMaxMillis(registry, "dblog.runtime.chunking.upper_bound_read.duration", adapter),
        counter(registry, "dblog.runtime.chunking.next_table_chunk.calls.total", adapter),
        counter(registry, "dblog.runtime.chunking.next_table_chunk.empty.total", adapter),
        counter(registry, "dblog.runtime.chunking.next_table_chunk.rows.total", adapter),
        counter(registry, "dblog.runtime.chunking.next_table_chunk.final.total", adapter),
        counter(registry, "dblog.runtime.chunking.next_table_chunk.failures.total", adapter),
        timerTotalMillis(registry, "dblog.runtime.chunking.next_table_chunk.duration", adapter),
        timerMaxMillis(registry, "dblog.runtime.chunking.next_table_chunk.duration", adapter),
        counter(registry, "dblog.runtime.chunking.targeted_primary_keys.calls.total", adapter),
        counter(registry, "dblog.runtime.chunking.targeted_primary_keys.empty.total", adapter),
        counter(
            registry,
            "dblog.runtime.chunking.targeted_primary_keys.requested_keys.total",
            adapter),
        counter(registry, "dblog.runtime.chunking.targeted_primary_keys.rows.total", adapter),
        counter(
            registry, "dblog.runtime.chunking.targeted_primary_keys.failures.total", adapter),
        timerTotalMillis(
            registry, "dblog.runtime.chunking.targeted_primary_keys.duration", adapter),
        timerMaxMillis(
            registry, "dblog.runtime.chunking.targeted_primary_keys.duration", adapter),
        counter(registry, "dblog.runtime.request_coordinator.coordinate.calls.total", adapter),
        counter(registry, "dblog.runtime.request_coordinator.coordinate.empty.total", adapter),
        counter(registry, "dblog.runtime.request_coordinator.coordinate.failures.total", adapter),
        timerTotalMillis(registry, "dblog.runtime.request_coordinator.coordinate.duration", adapter),
        timerMaxMillis(registry, "dblog.runtime.request_coordinator.coordinate.duration", adapter),
        counter(registry, "dblog.runtime.request_coordinator.acknowledges.total", adapter),
        counter(
            registry, "dblog.runtime.request_coordinator.acknowledge.failures.total", adapter),
        timerTotalMillis(
            registry, "dblog.runtime.request_coordinator.acknowledge.duration", adapter),
        timerMaxMillis(
            registry, "dblog.runtime.request_coordinator.acknowledge.duration", adapter));
  }

  public RuntimeMeasurementMetricsSnapshot delta(RuntimeMeasurementMetricsSnapshot before) {
    return new RuntimeMeasurementMetricsSnapshot(
        sourceCapturePolls - before.sourceCapturePolls,
        sourceCaptureEmptyPolls - before.sourceCaptureEmptyPolls,
        sourceCaptureTransactions - before.sourceCaptureTransactions,
        sourceCaptureEvents - before.sourceCaptureEvents,
        sourceCapturePollFailures - before.sourceCapturePollFailures,
        sourceCaptureTransactionDecodeDurationMillisTotal
            - before.sourceCaptureTransactionDecodeDurationMillisTotal,
        sourceCaptureTransactionDecodeDurationMillisMax,
        sourceCaptureAcknowledgeCalls - before.sourceCaptureAcknowledgeCalls,
        sourceCaptureAcknowledgeFailures - before.sourceCaptureAcknowledgeFailures,
        sourceCaptureAcknowledgeDurationMillisTotal
            - before.sourceCaptureAcknowledgeDurationMillisTotal,
        sourceCaptureAcknowledgeDurationMillisMax,
        sourceCaptureHeartbeatsWritten - before.sourceCaptureHeartbeatsWritten,
        sourceCaptureHeartbeatsSkipped - before.sourceCaptureHeartbeatsSkipped,
        sourceCaptureHeartbeatFailures - before.sourceCaptureHeartbeatFailures,
        sourceCaptureHeartbeatDurationMillisTotal
            - before.sourceCaptureHeartbeatDurationMillisTotal,
        sourceCaptureHeartbeatDurationMillisMax,
        chunkUpperBoundReads - before.chunkUpperBoundReads,
        chunkUpperBoundReadMisses - before.chunkUpperBoundReadMisses,
        chunkUpperBoundReadFailures - before.chunkUpperBoundReadFailures,
        chunkUpperBoundReadDurationMillisTotal - before.chunkUpperBoundReadDurationMillisTotal,
        chunkUpperBoundReadDurationMillisMax,
        nextTableChunkCalls - before.nextTableChunkCalls,
        nextTableChunkEmpty - before.nextTableChunkEmpty,
        nextTableChunkRows - before.nextTableChunkRows,
        nextTableChunkFinal - before.nextTableChunkFinal,
        nextTableChunkFailures - before.nextTableChunkFailures,
        nextTableChunkDurationMillisTotal - before.nextTableChunkDurationMillisTotal,
        nextTableChunkDurationMillisMax,
        targetedPrimaryKeyChunkCalls - before.targetedPrimaryKeyChunkCalls,
        targetedPrimaryKeyChunkEmpty - before.targetedPrimaryKeyChunkEmpty,
        targetedPrimaryKeyRequestedKeys - before.targetedPrimaryKeyRequestedKeys,
        targetedPrimaryKeyRows - before.targetedPrimaryKeyRows,
        targetedPrimaryKeyFailures - before.targetedPrimaryKeyFailures,
        targetedPrimaryKeyDurationMillisTotal
            - before.targetedPrimaryKeyDurationMillisTotal,
        targetedPrimaryKeyDurationMillisMax,
        coordinateCalls - before.coordinateCalls,
        coordinateEmpty - before.coordinateEmpty,
        coordinateFailures - before.coordinateFailures,
        coordinateDurationMillisTotal - before.coordinateDurationMillisTotal,
        coordinateDurationMillisMax,
        acknowledgeCalls - before.acknowledgeCalls,
        acknowledgeFailures - before.acknowledgeFailures,
        acknowledgeDurationMillisTotal
            - before.acknowledgeDurationMillisTotal,
        acknowledgeDurationMillisMax);
  }

  private static double counter(MeterRegistry meterRegistry, String name, String adapter) {
    var counter = meterRegistry.find(name).tags("adapter", adapter).counter();
    return counter == null ? 0.0d : counter.count();
  }

  private static double timerTotalMillis(
      MeterRegistry meterRegistry, String name, String adapter) {
    Timer timer = meterRegistry.find(name).tags("adapter", adapter).timer();
    return timer == null ? 0.0d : timer.totalTime(TimeUnit.MILLISECONDS);
  }

  private static double timerMaxMillis(MeterRegistry meterRegistry, String name, String adapter) {
    Timer timer = meterRegistry.find(name).tags("adapter", adapter).timer();
    return timer == null ? 0.0d : timer.max(TimeUnit.MILLISECONDS);
  }
}
