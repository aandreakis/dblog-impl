package io.github.aandreakis.dblog.controlplane.service;

import io.github.aandreakis.dblog.core.model.ChangeEvent;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Asynchronous wrapper around a control-plane event store so runtime append paths do not pay
 * per-event recording cost on the caller thread.
 */
public final class AsyncControlPlaneEventStore implements ControlPlaneEventStore, AutoCloseable {
  private final ControlPlaneEventStore delegate;
  private final ExecutorService executor;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public AsyncControlPlaneEventStore(ControlPlaneEventStore delegate) {
    this(
        delegate,
        Executors.newSingleThreadExecutor(
            Thread.ofVirtual().name("dblog-controlplane-events-", 0).factory()));
  }

  AsyncControlPlaneEventStore(ControlPlaneEventStore delegate, ExecutorService executor) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.executor = Objects.requireNonNull(executor, "executor");
  }

  @Override
  public void record(String stageLabel, List<ChangeEvent> events) {
    Objects.requireNonNull(stageLabel, "stageLabel");
    List<ChangeEvent> snapshot = List.copyOf(Objects.requireNonNull(events, "events"));
    if (snapshot.isEmpty() || closed.get()) {
      return;
    }
    try {
      executor.execute(() -> delegate.record(stageLabel, snapshot));
    } catch (RejectedExecutionException ignored) {
      // Shutdown races should not fail the hot path.
    }
  }

  @Override
  public List<RecordedEvent> recentEvents(int limit) {
    awaitIdle();
    return delegate.recentEvents(limit);
  }

  @Override
  public List<RecordedEvent> recentEventsForTable(String tableDisplayName, int limit) {
    awaitIdle();
    return delegate.recentEventsForTable(tableDisplayName, limit);
  }

  @Override
  public List<TableEventSummary> tableSummaries() {
    awaitIdle();
    return delegate.tableSummaries();
  }

  @Override
  public int recentEventWindowSize() {
    awaitIdle();
    return delegate.recentEventWindowSize();
  }

  @Override
  public long cumulativeEventsObserved() {
    awaitIdle();
    return delegate.cumulativeEventsObserved();
  }

  @Override
  public void close() throws Exception {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    awaitIdle();
    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.SECONDS);
    if (delegate instanceof AutoCloseable closeable) {
      closeable.close();
    }
  }

  private void awaitIdle() {
    if (executor.isShutdown()) {
      return;
    }
    try {
      Future<?> barrier = executor.submit(() -> {});
      barrier.get(5, TimeUnit.SECONDS);
    } catch (RejectedExecutionException ignored) {
      // Best effort only after shutdown starts.
    } catch (Exception e) {
      throw new IllegalStateException("failed while awaiting control-plane event drain", e);
    }
  }
}
