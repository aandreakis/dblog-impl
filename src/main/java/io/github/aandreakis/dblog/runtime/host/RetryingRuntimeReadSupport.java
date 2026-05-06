package io.github.aandreakis.dblog.runtime.host;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared reconnect loop for runtime log-stream reads on the final runtime surface.
 *
 * <p>This deliberately retries only {@link SQLException} read/open failures. Other runtime
 * failures, such as protocol drift or schema corruption, continue to fail closed.
 */
public final class RetryingRuntimeReadSupport<H extends AutoCloseable> implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(RetryingRuntimeReadSupport.class);
  private static final java.util.Map<MeterRegistry, ConcurrentMap<String, AtomicInteger>>
      RECONNECTING_GAUGES = Collections.synchronizedMap(new WeakHashMap<>());

  private final String adapterLabel;
  private final Duration reconnectBackoff;
  private final RuntimeHandleOpener<H> opener;
  private final FailureClassifier failureClassifier;
  private final RetryObserver retryObserver;
  private final Sleeper sleeper;
  private final Counter readFailureCounter;
  private final Counter reconnectAttemptCounter;
  private final Counter reconnectSuccessCounter;
  private final Counter reconnectFailureCounter;
  private final Counter reconnectDowntimeMillisCounter;
  private final AtomicInteger reconnecting;

  private H currentHandle;

  public RetryingRuntimeReadSupport(
      MeterRegistry meterRegistry,
      String adapterLabel,
      H initialHandle,
      Duration reconnectBackoff,
      RuntimeHandleOpener<H> opener) {
    this(
        meterRegistry,
        adapterLabel,
        initialHandle,
        reconnectBackoff,
        opener,
        RuntimeFailureClassifier::classifySource,
        RetryObserver.noop(),
        Sleeper.threadSleep());
  }

  public RetryingRuntimeReadSupport(
      MeterRegistry meterRegistry,
      String adapterLabel,
      H initialHandle,
      Duration reconnectBackoff,
      RuntimeHandleOpener<H> opener,
      FailureClassifier failureClassifier,
      RetryObserver retryObserver,
      Sleeper sleeper) {
    MeterRegistry registry = Objects.requireNonNull(meterRegistry, "meterRegistry");
    this.adapterLabel = requireNonBlank(adapterLabel, "adapterLabel");
    this.currentHandle = Objects.requireNonNull(initialHandle, "initialHandle");
    this.reconnectBackoff = requirePositive(reconnectBackoff, "reconnectBackoff");
    this.opener = Objects.requireNonNull(opener, "opener");
    this.failureClassifier = Objects.requireNonNull(failureClassifier, "failureClassifier");
    this.retryObserver = Objects.requireNonNull(retryObserver, "retryObserver");
    this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
    Tags tags = Tags.of("adapter", this.adapterLabel);
    this.readFailureCounter =
        Counter.builder("dblog.runtime.source_log.read_failures.total")
            .tags(tags)
            .register(registry);
    this.reconnectAttemptCounter =
        Counter.builder("dblog.runtime.source_log.reconnect.attempts.total")
            .tags(tags)
            .register(registry);
    this.reconnectSuccessCounter =
        Counter.builder("dblog.runtime.source_log.reconnect.successes.total")
            .tags(tags)
            .register(registry);
    this.reconnectFailureCounter =
        Counter.builder("dblog.runtime.source_log.reconnect.failures.total")
            .tags(tags)
            .register(registry);
    this.reconnectDowntimeMillisCounter =
        Counter.builder("dblog.runtime.source_log.reconnect.downtime.millis.total")
            .tags(tags)
            .register(registry);
    this.reconnecting = reconnectingGauge(registry, this.adapterLabel);
  }

  public synchronized <T> T readWithRetry(SqlHandleWork<H, T> readWork) throws SQLException {
    return executeWithRetry(readWork);
  }

  public synchronized <T> T executeWithRetry(SqlHandleWork<H, T> work) throws SQLException {
    Objects.requireNonNull(work, "work");
    while (true) {
      try {
        return work.execute(currentHandle);
      } catch (SQLException failure) {
        recoverAfterFailure(failure);
      }
    }
  }

  public synchronized void recoverAfterFailure(SQLException failure) throws SQLException {
    Objects.requireNonNull(failure, "failure");
    RuntimeFailureDisposition disposition = failureClassifier.classify(failure);
    if (disposition == RuntimeFailureDisposition.FAIL_HARD_CONTRACT_BREACH) {
      reconnecting.set(0);
      retryObserver.failed(failure);
      throw failure;
    }
    reconnectAfterReadFailure(failure);
  }

  public synchronized H currentHandle() {
    return currentHandle;
  }

  @Override
  public synchronized void close() throws SQLException {
    reconnecting.set(0);
    closeCurrentQuietly(null);
  }

  private void reconnectAfterReadFailure(SQLException readFailure) throws SQLException {
    readFailureCounter.increment();
    retryObserver.retrying(readFailure);
    reconnecting.set(1);
    log.warn(
        "{} source runtime operation failed; retrying reconnect every {} until the source becomes reachable again",
        adapterLabel,
        reconnectBackoff,
        readFailure);
    closeCurrentQuietly(readFailure);

    long reconnectStartedAtNanos = System.nanoTime();
    int attempt = 0;
    try {
      while (currentHandle == null) {
        attempt++;
        reconnectAttemptCounter.increment();
        sleepBeforeRetry(readFailure);
        try {
          currentHandle = opener.open();
          reconnectSuccessCounter.increment();
          reconnecting.set(0);
          retryObserver.recovered();
          log.info(
              "{} source runtime reconnect succeeded after {} attempt(s)",
              adapterLabel,
              attempt);
        } catch (SQLException reopenFailure) {
          RuntimeFailureDisposition reopenDisposition =
              failureClassifier.classify(reopenFailure);
          if (reopenDisposition == RuntimeFailureDisposition.FAIL_HARD_CONTRACT_BREACH) {
            reconnecting.set(0);
            retryObserver.failed(reopenFailure);
            throw reopenFailure;
          }
          retryObserver.retrying(reopenFailure);
          reconnectFailureCounter.increment();
          log.warn(
              "{} source runtime reconnect attempt {} failed; retrying in {}",
              adapterLabel,
              attempt,
              reconnectBackoff,
              reopenFailure);
        }
      }
    } finally {
      reconnectDowntimeMillisCounter.increment(
          Math.max(0.0d, (System.nanoTime() - reconnectStartedAtNanos) / 1_000_000.0d));
    }
  }

  private void sleepBeforeRetry(SQLException originalFailure) throws SQLException {
    try {
      sleeper.sleep(reconnectBackoff);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      SQLException failure =
          new SQLException(
              adapterLabel + " source runtime reconnect loop was interrupted while waiting to retry",
              interrupted);
      failure.addSuppressed(originalFailure);
      throw failure;
    }
  }

  private void closeCurrentQuietly(SQLException originalFailure) throws SQLException {
    if (currentHandle == null) {
      return;
    }
    try {
      currentHandle.close();
    } catch (Exception closeFailure) {
      if (originalFailure == null) {
        if (closeFailure instanceof SQLException sqlException) {
          throw sqlException;
        }
        throw new SQLException(
            adapterLabel + " source runtime handle close failed during shutdown", closeFailure);
      }
      originalFailure.addSuppressed(closeFailure);
    } finally {
      currentHandle = null;
    }
  }

  private static AtomicInteger reconnectingGauge(
      MeterRegistry meterRegistry, String adapterLabel) {
    synchronized (RECONNECTING_GAUGES) {
      return RECONNECTING_GAUGES
          .computeIfAbsent(meterRegistry, ignored -> new ConcurrentHashMap<>())
          .computeIfAbsent(
              adapterLabel,
              ignored ->
                  meterRegistry.gauge(
                      "dblog.runtime.source_log.reconnecting",
                      Tags.of("adapter", adapterLabel),
                      new AtomicInteger(0)));
    }
  }

  @FunctionalInterface
  public interface FailureClassifier {
    RuntimeFailureDisposition classify(Throwable failure);
  }

  public interface RetryObserver {
    void retrying(Throwable failure);

    void recovered();

    void failed(Throwable failure);

    static RetryObserver noop() {
      return new RetryObserver() {
        @Override
        public void retrying(Throwable failure) {}

        @Override
        public void recovered() {}

        @Override
        public void failed(Throwable failure) {}
      };
    }
  }

  @FunctionalInterface
  public interface RuntimeHandleOpener<H> {
    H open() throws SQLException;
  }

  @FunctionalInterface
  public interface SqlHandleWork<H, T> {
    T execute(H handle) throws SQLException;
  }

  @FunctionalInterface
  public interface Sleeper {
    void sleep(Duration duration) throws InterruptedException;

    static Sleeper threadSleep() {
      return duration -> Thread.sleep(duration.toMillis());
    }
  }

  private static Duration requirePositive(Duration value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isZero() || value.isNegative()) {
      throw new IllegalArgumentException(name + " must be > 0");
    }
    return value;
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
