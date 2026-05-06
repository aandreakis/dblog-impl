package io.github.aandreakis.dblog.integration.adapter;

import io.github.aandreakis.dblog.adapter.api.SourceRuntime;
import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Cross-vendor helpers for the live-runtime integration tests
 * ({@code MySqlSourceAdapterLiveRuntimeIT}, {@code PostgresSourceAdapterLiveRuntimeIT}). These
 * helpers are vendor-agnostic — they speak only to the {@link SourceRuntime} surface every adapter
 * implements, so a hypothetical future vendor that exposes the same interface gets the helpers for
 * free, and vendor-specific tests stay where they are.
 *
 * <p>Vendor-specific helpers (binlog purge, slot-state polling, replication-user grants, etc.)
 * deliberately stay in their owning IT — they encode adapter-specific assumptions and have no
 * portable shape.
 */
public final class LiveRuntimeTestSupport {

  /**
   * Maximum wall time to wait for a single committed source transaction to surface. Generous so a
   * slow CI box's binlog / pgoutput tail latency does not cause flakes; the actual happy-path
   * latency is sub-second.
   */
  public static final Duration TRANSACTION_AWAIT_TIMEOUT = Duration.ofSeconds(20);

  /**
   * Polling cadence inside {@link #awaitTransaction} and {@link #drainEvents}. Tight because the
   * runtimes deliver transactions through an in-memory bounded queue — the actual hand-off is
   * sub-millisecond, so this is just upper-bounded by scheduling jitter.
   */
  public static final Duration TRANSACTION_POLL_INTERVAL = Duration.ofMillis(50);

  private LiveRuntimeTestSupport() {}

  /**
   * Polls {@code runtime.readPendingTransaction()} until a transaction surfaces or {@code timeout}
   * elapses. Throws {@link IllegalStateException} on timeout — the caller is asserting that the
   * runtime ought to be producing.
   */
  public static <TX extends SourceTransaction<?>> TX awaitTransaction(
      SourceRuntime<TX> runtime, Duration timeout) throws Exception {
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    do {
      Optional<TX> pending = runtime.readPendingTransaction();
      if (pending.isPresent()) {
        return pending.orElseThrow();
      }
      Thread.sleep(TRANSACTION_POLL_INTERVAL.toMillis());
    } while (System.nanoTime() < deadlineNanos);
    throw new IllegalStateException("Timed out waiting for a committed source transaction");
  }

  /**
   * Drains every transaction that surfaces from {@code runtime} for the given {@code duration},
   * acknowledging each one and accumulating its events. Returns an immutable list of all events
   * observed in the window. Used to verify "many small transactions" and "stream-after-replay"
   * style scenarios where the test wants the whole tail rather than a single transaction.
   */
  public static <TX extends SourceTransaction<?>> List<ChangeEvent> drainEvents(
      SourceRuntime<TX> runtime, Duration duration) throws Exception {
    long deadlineNanos = System.nanoTime() + duration.toNanos();
    List<ChangeEvent> events = new ArrayList<>();
    while (System.nanoTime() < deadlineNanos) {
      Optional<TX> pending = runtime.readPendingTransaction();
      if (pending.isPresent()) {
        TX transaction = pending.orElseThrow();
        events.addAll(transaction.events());
        runtime.acknowledge(transaction);
        continue;
      }
      Thread.sleep(TRANSACTION_POLL_INTERVAL.toMillis());
    }
    return List.copyOf(events);
  }

}
