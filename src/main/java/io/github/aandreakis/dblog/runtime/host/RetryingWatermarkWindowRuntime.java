package io.github.aandreakis.dblog.runtime.host;

import io.github.aandreakis.dblog.adapter.api.ChunkReadWithWindowWork;
import io.github.aandreakis.dblog.adapter.api.ChunkReadWork;
import io.github.aandreakis.dblog.adapter.api.RuntimeStatusInspectable;
import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.adapter.api.WatermarkWindowResult;
import io.github.aandreakis.dblog.adapter.api.WatermarkWindowRuntime;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.runtime.observer.DbLogRuntimeObservability;
import io.github.aandreakis.dblog.runtime.sql.SourceFlowControlSnapshot;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runtime retry wrapper for source availability failures on the watermark-window runtime surface. */
public final class RetryingWatermarkWindowRuntime<
        TX extends SourceTransaction<?>,
        H extends WatermarkWindowRuntime<TX>>
    implements WatermarkWindowRuntime<TX>, RuntimeStatusInspectable {
  private static final Logger log = LoggerFactory.getLogger(RetryingWatermarkWindowRuntime.class);
  private static final AtomicBoolean LOGGED_PRIVATE_REGISTRY = new AtomicBoolean();
  private final RetryingRuntimeReadSupport<H> retrySupport;
  private static final RuntimeStatusInspectable NOOP_STATUS = new RuntimeStatusInspectable() {};

  public RetryingWatermarkWindowRuntime(
      String adapterLabel,
      MeterRegistry meterRegistry,
      H initialHandle,
      Duration reconnectBackoff,
      RetryingRuntimeReadSupport.RuntimeHandleOpener<H> opener) {
    this(
        adapterLabel,
        meterRegistry,
        initialHandle,
        reconnectBackoff,
        opener,
        RetryingRuntimeReadSupport.RetryObserver.noop());
  }

  public RetryingWatermarkWindowRuntime(
      String adapterLabel,
      DbLogRuntimeObservability observability,
      MeterRegistry meterRegistry,
      H initialHandle,
      Duration reconnectBackoff,
      RetryingRuntimeReadSupport.RuntimeHandleOpener<H> opener) {
    this(
        adapterLabel,
        meterRegistry,
        initialHandle,
        reconnectBackoff,
        opener,
        observabilityRetryObserver(observability));
  }

  private RetryingWatermarkWindowRuntime(
      String adapterLabel,
      MeterRegistry meterRegistry,
      H initialHandle,
      Duration reconnectBackoff,
      RetryingRuntimeReadSupport.RuntimeHandleOpener<H> opener,
      RetryingRuntimeReadSupport.RetryObserver retryObserver) {
    this.retrySupport =
        new RetryingRuntimeReadSupport<>(
            meterRegistryOrSimple(meterRegistry),
            Objects.requireNonNull(adapterLabel, "adapterLabel"),
            Objects.requireNonNull(initialHandle, "initialHandle"),
            Objects.requireNonNull(reconnectBackoff, "reconnectBackoff"),
            Objects.requireNonNull(opener, "opener"),
            RuntimeFailureClassifier::classifySource,
            Objects.requireNonNull(retryObserver, "retryObserver"),
            RetryingRuntimeReadSupport.Sleeper.threadSleep());
  }

  @Override
  public <T> T executeChunkRead(SourceChunkReader reader, ChunkReadWork<T> work)
      throws SQLException {
    return retrySupport.executeWithRetry(handle -> handle.executeChunkRead(reader, work));
  }

  @Override
  public <T> WatermarkWindowResult<T> executeChunkReadInWatermarkWindow(
      SourceChunkReader reader, ChunkReadWithWindowWork<T> work) throws SQLException {
    try {
      return retrySupport.currentHandle().executeChunkReadInWatermarkWindow(reader, work);
    } catch (SQLException failure) {
      retrySupport.recoverAfterFailure(failure);
      throw failure;
    }
  }

  @Override
  public Optional<TX> readPendingTransaction() throws SQLException {
    return retrySupport.readWithRetry(WatermarkWindowRuntime::readPendingTransaction);
  }

  @Override
  public void acknowledge(TX transaction) throws SQLException {
    retrySupport.executeWithRetry(
        handle -> {
          handle.acknowledge(transaction);
          return null;
        });
  }

  @Override
  public boolean emitHeartbeatIfDue(Instant heartbeatTime, Duration minimumInterval)
      throws SQLException {
    return retrySupport.executeWithRetry(
        handle -> handle.emitHeartbeatIfDue(heartbeatTime, minimumInterval));
  }

  @Override
  public List<TableSchema> currentCapturedSchemas() {
    return retrySupport.currentHandle().currentCapturedSchemas();
  }

  @Override
  public void updateCapturedSchema(TableSchema schema) {
    retrySupport.currentHandle().updateCapturedSchema(schema);
  }

  @Override
  public String currentRunId() {
    return retrySupport.currentHandle().currentRunId();
  }

  @Override
  public String lastAcknowledgedCheckpointDisplayValue() {
    return statusInspectable().lastAcknowledgedCheckpointDisplayValue();
  }

  @Override
  public int pendingTransactionCount() {
    return statusInspectable().pendingTransactionCount();
  }

  @Override
  public int capturedTableCount() {
    return statusInspectable().capturedTableCount();
  }

  @Override
  public SourceFlowControlSnapshot sourceFlowControlSnapshot() {
    return statusInspectable().sourceFlowControlSnapshot();
  }

  @Override
  public void close() throws SQLException {
    retrySupport.close();
  }

  private RuntimeStatusInspectable statusInspectable() {
    WatermarkWindowRuntime<TX> handle = retrySupport.currentHandle();
    return handle instanceof RuntimeStatusInspectable inspectable ? inspectable : NOOP_STATUS;
  }

  private static MeterRegistry meterRegistryOrSimple(MeterRegistry meterRegistry) {
    if (meterRegistry != null) {
      return meterRegistry;
    }
    if (LOGGED_PRIVATE_REGISTRY.compareAndSet(false, true)) {
      log.info(
          "Source retry metrics are using a private SimpleMeterRegistry because no MeterRegistry "
              + "was supplied; counters remain internal to the retry wrapper");
    }
    return new SimpleMeterRegistry();
  }

  private static RetryingRuntimeReadSupport.RetryObserver observabilityRetryObserver(
      DbLogRuntimeObservability observability) {
    DbLogRuntimeObservability requiredObservability =
        Objects.requireNonNull(observability, "observability");
    return new RetryingRuntimeReadSupport.RetryObserver() {
      @Override
      public void retrying(Throwable failure) {
        requiredObservability.sourceRetrying(failure);
      }

      @Override
      public void recovered() {
        requiredObservability.sourceUp();
      }

      @Override
      public void failed(Throwable failure) {
        requiredObservability.sourceFailed(failure);
      }
    };
  }
}
