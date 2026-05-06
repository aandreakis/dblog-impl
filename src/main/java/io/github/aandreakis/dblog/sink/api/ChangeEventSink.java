package io.github.aandreakis.dblog.sink.api;

import io.github.aandreakis.dblog.core.model.ChangeEvent;
import java.util.List;

/**
 * Output port for one configured DBLog sink. The runtime delivers events as ordered batches; the
 * sink decides what durability and idempotency it offers downstream.
 *
 * <h2>Delivery</h2>
 *
 * At-least-once. A batch may be redelivered after a crash that interrupts the runtime between
 * sink append and source-checkpoint advance. Sinks whose downstream is not naturally idempotent
 * must dedupe or upsert by primary key.
 *
 * <h2>Ordering</h2>
 *
 * Per-batch order matches the runtime's emission order — committed-transaction order on the live
 * stream, with reconciled chunk rows appended at the high-watermark boundary. Implementations
 * must preserve this order within a batch.
 *
 * <h2>Control events</h2>
 *
 * Watermark events are intercepted by the reconciler and never reach a sink. Heartbeat events
 * may; each implementation chooses whether to filter or pass them through based on what the
 * downstream understands.
 *
 * <h2>Threading</h2>
 *
 * The runtime invokes one sink instance from a single pump thread. Implementations need not be
 * thread-safe.
 */
public interface ChangeEventSink extends AutoCloseable {
  void appendEvents(List<ChangeEvent> events);

  /**
   * Cooperative shutdown signal sent before {@link #close()}. Long-running sinks should stop
   * accepting new work and unblock any in-flight write; short-running sinks may ignore it.
   * Idempotent.
   */
  default void requestStop() {}

  @Override
  default void close() throws Exception {}
}
