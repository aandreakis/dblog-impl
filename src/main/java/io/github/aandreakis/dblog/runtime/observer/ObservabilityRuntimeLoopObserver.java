package io.github.aandreakis.dblog.runtime.observer;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.request.ScheduledRequestBatch;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Final-package runtime-loop observer for logs, operator-facing observability state, and
 * Micrometer metrics.
 *
 * <p>Metrics exposed on the injected {@link MeterRegistry}:
 *
 * <ul>
 *   <li>{@code dblog.transactions.processed} (counter, tag {@code stage}) — committed source
 *       transactions flushed through a runtime stage.
 *   <li>{@code dblog.events.processed} (counter, tag {@code stage}) — count of individual change
 *       events carried by those transactions.
 *   <li>{@code dblog.sink.append.seconds} (timer, tags {@code stage}, {@code outcome}) — duration
 *       of a sink append call.
 *   <li>{@code dblog.sink.events} (counter, tags {@code stage}, {@code outcome}) — event count
 *       per sink append call, split by outcome.
 *   <li>{@code dblog.checkpoint.advanced} (counter, tags {@code stage}, {@code reason}) — durable
 *       checkpoint advance calls.
 *   <li>{@code dblog.request.batches} (counter, tags {@code scope}, {@code finalBatch}) — dump
 *       request batches coordinated by the coordinator.
 *   <li>{@code dblog.pending.requests} (gauge) — current count of pending dump requests as most
 *       recently reported by the coordinator.
 *   <li>{@code dblog.source.queue.depth} (gauge) — current depth of the source-flow queue
 *       reported by {@link DbLogRuntimeObservability#sourceFlowControlSnapshot()}.
 * </ul>
 *
 * <p>All metrics carry a common {@code adapter} tag so dashboards can filter mysql vs postgres.
 * Cardinality is kept bounded by not tagging with unbounded values such as {@code requestId},
 * {@code transactionId}, or per-table names.
 */
public final class ObservabilityRuntimeLoopObserver<TX extends SourceTransaction<?>>
    implements RuntimeLoopObserver<TX> {
  private static final String METRIC_TRANSACTIONS_PROCESSED = "dblog.transactions.processed";
  private static final String METRIC_EVENTS_PROCESSED = "dblog.events.processed";
  private static final String METRIC_SINK_APPEND_SECONDS = "dblog.sink.append.seconds";
  private static final String METRIC_SINK_EVENTS = "dblog.sink.events";
  private static final String METRIC_CHECKPOINT_ADVANCED = "dblog.checkpoint.advanced";
  private static final String METRIC_REQUEST_BATCHES = "dblog.request.batches";
  private static final String METRIC_PENDING_REQUESTS = "dblog.pending.requests";
  private static final String METRIC_SOURCE_QUEUE_DEPTH = "dblog.source.queue.depth";
  private static final String METRIC_DUMP_CHUNKS_COMPLETED = "dblog.dump.chunks.completed.total";
  private static final String METRIC_DUMP_ROWS_EMITTED = "dblog.dump.rows.emitted.total";

  private final Logger log;
  private final DbLogRuntimeObservability observability;
  private final MeterRegistry meterRegistry;
  private final String adapterLabel;
  private final AtomicInteger pendingRequestsGaugeValue = new AtomicInteger(0);

  public ObservabilityRuntimeLoopObserver(
      DbLogRuntimeObservability observability,
      String adapterLabel,
      MeterRegistry meterRegistry) {
    this.observability = Objects.requireNonNull(observability, "observability");
    this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry");
    this.adapterLabel =
        Objects.requireNonNull(adapterLabel, "adapterLabel").toLowerCase(Locale.ROOT);
    this.log = LoggerFactory.getLogger("io.github.aandreakis.dblog.runtime.observer." + this.adapterLabel);

    // Gauges must be registered once. Micrometer's builder resolves by name+tags, so repeated
    // registration on runtime restart inside the same registry is safe.
    Gauge.builder(METRIC_PENDING_REQUESTS, pendingRequestsGaugeValue, AtomicInteger::doubleValue)
        .description("Current count of pending dump requests in the local state store")
        .tag("adapter", this.adapterLabel)
        .register(this.meterRegistry);
    Gauge.builder(
            METRIC_SOURCE_QUEUE_DEPTH,
            observability,
            obs -> (double) obs.sourceFlowControlSnapshot().queueDepth())
        .description("Current depth of the source-flow queue")
        .tag("adapter", this.adapterLabel)
        .register(this.meterRegistry);
  }

  @Override
  public void onTransactionPersisted(String stageLabel, TX transaction) {
    log.debug(
        "Persisted runtime transaction stage={} tx={} checkpoint={} eventCount={}",
        stageLabel,
        transaction.transactionId(),
        transaction.checkpointPosition().displayValue(),
        transaction.eventCount());
    String stage = normalizeStageTag(stageLabel);
    meterRegistry
        .counter(METRIC_TRANSACTIONS_PROCESSED, "adapter", adapterLabel, "stage", stage)
        .increment();
    int eventCount = transaction.eventCount();
    if (eventCount > 0) {
      meterRegistry
          .counter(METRIC_EVENTS_PROCESSED, "adapter", adapterLabel, "stage", stage)
          .increment(eventCount);
    }
  }

  @Override
  public void onCheckpointAdvanced(String stageLabel, String reason, TX transaction) {
    observability.sourceCheckpointAdvanced(
        transaction.checkpointPosition().displayValue(), Instant.now().toString());
    log.debug(
        "Advanced runtime checkpoint stage={} reason={} tx={} checkpoint={}",
        stageLabel,
        reason,
        transaction.transactionId(),
        transaction.checkpointPosition().displayValue());
    meterRegistry
        .counter(
            METRIC_CHECKPOINT_ADVANCED,
            "adapter", adapterLabel,
            "stage", normalizeStageTag(stageLabel),
            "reason", reason == null ? "unspecified" : reason)
        .increment();
  }

  @Override
  public void onRequestBatch(ScheduledRequestBatch<TX> batch) {
    String tableDisplayName = batch.tableId().displayName();
    log.info(
        "Coordinated request batch requestId={} scope={} finalRequestBatch={} table={}",
        batch.request().requestId(),
        batch.request().scope(),
        batch.finalRequestBatch(),
        tableDisplayName);
    meterRegistry
        .counter(
            METRIC_REQUEST_BATCHES,
            "adapter", adapterLabel,
            "scope", batch.request().scope().name(),
            "finalBatch", Boolean.toString(batch.finalRequestBatch()))
        .increment();
    // Dump-specific counters: one chunk per batch, plus rows emitted by origin (SELECT vs LOG).
    // Operators derive rows/s and chunks/s from these via their metrics stack.
    meterRegistry
        .counter(
            METRIC_DUMP_CHUNKS_COMPLETED, "adapter", adapterLabel, "table", tableDisplayName)
        .increment();
    long selectRows =
        batch.emittedEvents().stream()
            .filter(
                event ->
                    event.captureOrigin()
                        == CaptureOrigin.SELECT)
            .count();
    long logRows = batch.emittedEvents().size() - selectRows;
    if (selectRows > 0) {
      meterRegistry
          .counter(
              METRIC_DUMP_ROWS_EMITTED,
              "adapter", adapterLabel,
              "origin", "SELECT",
              "table", tableDisplayName)
          .increment(selectRows);
    }
    if (logRows > 0) {
      meterRegistry
          .counter(
              METRIC_DUMP_ROWS_EMITTED,
              "adapter", adapterLabel,
              "origin", "LOG",
              "table", tableDisplayName)
          .increment(logRows);
    }
  }

  @Override
  public void onPendingRequestCountChanged(int pendingRequestCount) {
    if (pendingRequestCount < 0) {
      return;
    }
    pendingRequestsGaugeValue.set(pendingRequestCount);
  }

  @Override
  public void onEventSinkAppendSucceeded(
      String stageLabel, Instant sourceCommitTimestamp, int eventCount, long appendNanos) {
    observability.sinkUp();
    observability.sinkApplySucceeded(null, Instant.now().toString());
    recordSinkAppend(stageLabel, eventCount, appendNanos, "success");
  }

  @Override
  public void onEventSinkAppendFailed(
      String stageLabel,
      Instant sourceCommitTimestamp,
      int eventCount,
      long appendNanos,
      Throwable failure) {
    observability.sinkFailed(failure);
    recordSinkAppend(stageLabel, eventCount, appendNanos, "failure");
  }

  private void recordSinkAppend(
      String stageLabel, int eventCount, long appendNanos, String outcome) {
    String stage = normalizeStageTag(stageLabel);
    Timer.builder(METRIC_SINK_APPEND_SECONDS)
        .description("Duration of sink append calls, split by outcome")
        .tag("adapter", adapterLabel)
        .tag("stage", stage)
        .tag("outcome", outcome)
        .register(meterRegistry)
        .record(Math.max(0L, appendNanos), java.util.concurrent.TimeUnit.NANOSECONDS);
    if (eventCount > 0) {
      meterRegistry
          .counter(
              METRIC_SINK_EVENTS,
              "adapter", adapterLabel,
              "stage", stage,
              "outcome", outcome)
          .increment(eventCount);
    }
  }

  /**
   * Keeps cardinality bounded: stage labels like {@code request-batch-req-42} are stripped down
   * to their family name {@code request-batch}. Known stable labels pass through.
   */
  private static String normalizeStageTag(String stageLabel) {
    if (stageLabel == null) {
      return "unknown";
    }
    if (stageLabel.startsWith("request-batch-")) {
      return "request-batch";
    }
    return stageLabel;
  }
}
