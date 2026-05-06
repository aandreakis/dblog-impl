package io.github.aandreakis.dblog.runtime.sql;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bounded producer/consumer queue for source-log events.
 *
 * <p>When the queue reaches capacity, producers block until consumers drain work. This applies
 * backpressure instead of dropping events or failing the runtime.
 *
 * <h2>Single-consumer invariant</h2>
 *
 * <p>This queue assumes exactly one consumer thread — the runtime orchestrator (see {@code
 * RuntimeRequestPump#runUntilStopped}). That single-thread invariant is load-bearing for the
 * watermark algorithm's correctness: the DBLog paper's Algorithm 1 step 1 says "pause log event
 * processing" before writing the low watermark, and this implementation satisfies that requirement
 * by having a single orchestrator thread which is either draining to the sink <em>or</em>
 * executing a watermark window (write LW → select chunk → write HW → reconcile), never both
 * concurrently. Multiple concurrent consumers would let log events leak to the sink during an open
 * watermark window, silently breaking history-order preservation.
 *
 * <p>Producers (the adapter's stream thread) may be any thread and may be multiple. Only {@link
 * #pollNow()} is single-consumer.
 *
 * <p>Set {@code -Ddblog.assertions=true} (or the static {@link #ASSERT_SINGLE_CONSUMER} flag) to
 * enable a runtime check that throws {@link IllegalStateException} if a second thread ever calls
 * {@link #pollNow()}. The check is off by default so production hot-path cost is zero.
 */
public final class BoundedSourceEventQueue<M> {
  private static final Duration OFFER_WAIT = Duration.ofMillis(100);

  /**
   * When true, {@link #pollNow()} verifies it is only ever called from a single consumer thread
   * and throws {@link IllegalStateException} otherwise. Toggled via the {@code dblog.assertions}
   * system property; intended for tests and local diagnostics, not production.
   */
  static final boolean ASSERT_SINGLE_CONSUMER = Boolean.getBoolean("dblog.assertions");

  private final Logger log;
  private final ArrayBlockingQueue<M> delegate;
  private final int capacity;
  private final int resumeDepthThreshold;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicReference<Throwable> failure = new AtomicReference<>();
  private final AtomicBoolean backpressureActive = new AtomicBoolean(false);
  private final AtomicReference<Thread> consumerThread = new AtomicReference<>();
  private final AtomicInteger blockedProducers = new AtomicInteger(0);
  private final AtomicInteger depth = new AtomicInteger(0);
  private final AtomicInteger maxDepth = new AtomicInteger(0);
  private final AtomicLong totalEnqueued = new AtomicLong(0L);
  private final AtomicLong totalDequeued = new AtomicLong(0L);
  private final AtomicLong totalPauseCount = new AtomicLong(0L);
  private final AtomicLong pausedSinceEpochMillis = new AtomicLong(0L);
  private final AtomicLong pausedSinceNanos = new AtomicLong(0L);
  private final AtomicLong totalPausedNanos = new AtomicLong(0L);
  private final AtomicLong lastSampleTimeNanos = new AtomicLong(System.nanoTime());
  private final AtomicLong lastSampleEnqueued = new AtomicLong(0L);
  private final AtomicLong lastSampleDequeued = new AtomicLong(0L);

  public BoundedSourceEventQueue(String adapterLabel, int capacity) {
    this.log =
        LoggerFactory.getLogger(
            "io.github.aandreakis.dblog.runtime."
                + requireNonBlank(adapterLabel, "adapterLabel")
                + ".sourceflow");
    this.capacity = requirePositive(capacity, "capacity");
    this.resumeDepthThreshold = Math.max(0, capacity - defaultResumeDrainCount(capacity));
    this.delegate = new ArrayBlockingQueue<>(capacity);
  }

  public boolean enqueue(M message) {
    Objects.requireNonNull(message, "message");
    boolean paused = false;
    while (!closed.get() && failure.get() == null) {
      try {
        boolean offered =
            paused
                ? delegate.offer(message, OFFER_WAIT.toMillis(), TimeUnit.MILLISECONDS)
                : delegate.offer(message);
        if (!offered) {
          if (!paused) {
            paused = true;
            producerPaused();
          }
          continue;
        }
        int currentDepth = depth.incrementAndGet();
        totalEnqueued.incrementAndGet();
        maxDepth.accumulateAndGet(currentDepth, Math::max);
        if (paused) {
          producerUnblocked();
        }
        return true;
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(
            "Interrupted while waiting for bounded source-event queue capacity", ex);
      }
    }
    if (paused) {
      producerUnblocked();
    }
    return false;
  }

  /**
   * Signals that the upstream producer has hit a fatal failure. Any producer thread currently
   * blocked in {@link #enqueue(Object)} observes this on its next offer-retry loop iteration and
   * exits (returns {@code false}) rather than continuing to block forever on a queue that will
   * never drain. The consumer continues to drain any already-buffered messages, and the existing
   * downstream surfaces (e.g. {@code MySqlTransactionStreamingSession.readMessage}) surface the
   * failure once the buffer is empty.
   *
   * <p>First call wins; subsequent calls are ignored. Calling with {@code null} is rejected so
   * callers don't accidentally clear an earlier failure.
   */
  public void markFailed(Throwable cause) {
    Objects.requireNonNull(cause, "cause");
    failure.compareAndSet(null, cause);
  }

  public Throwable failure() {
    return failure.get();
  }

  public Optional<M> pollNow() {
    if (ASSERT_SINGLE_CONSUMER) {
      assertSingleConsumer();
    }
    M message = delegate.poll();
    if (message != null) {
      depth.decrementAndGet();
      totalDequeued.incrementAndGet();
      maybeClearBackpressure();
    }
    return Optional.ofNullable(message);
  }

  /**
   * Verifies that {@link #pollNow()} is only ever called from a single thread. The first thread
   * to drain claims ownership; any subsequent caller from a different thread throws. Package
   * private so tests can exercise the invariant check without needing the JVM-level
   * {@code dblog.assertions} flag. On the hot path this is only invoked when {@link
   * #ASSERT_SINGLE_CONSUMER} is {@code true}.
   */
  void assertSingleConsumer() {
    Thread current = Thread.currentThread();
    Thread claimed = consumerThread.get();
    if (claimed == null) {
      if (consumerThread.compareAndSet(null, current)) {
        return;
      }
      claimed = consumerThread.get();
    }
    if (claimed != current) {
      throw new IllegalStateException(
          "BoundedSourceEventQueue violates its single-consumer invariant: already claimed by '"
              + claimed.getName()
              + "', but also drained by '"
              + current.getName()
              + "'. Only the runtime orchestrator thread may call readPendingTransaction(); "
              + "concurrent drains would break the watermark window's history-order guarantee.");
    }
  }

  public void close() {
    closed.set(true);
  }

  public int depth() {
    return depth.get();
  }

  /**
   * Returns a current flow-control snapshot. Counter-style fields ({@code queueDepth},
   * {@code maxDepth}, {@code totalEnqueued}, {@code totalDequeued}, backpressure state,
   * pause duration) are consistent under concurrent calls because each reads an
   * independent atomic. The per-second rate fields ({@code sourceEnqueueRatePerSecond},
   * {@code sourceDequeueRatePerSecond}) are computed as deltas against a "last sample"
   * baseline that this method destructively advances. Each caller therefore observes
   * rates over <em>whatever window elapsed since the previous snapshot call</em> — not
   * over a caller-controlled interval. When multiple callers (Micrometer scrape,
   * control-plane query, scenario telemetry) interleave, the rate fields reflect
   * different windows per call. This is an observability-UX limitation, not a
   * correctness bug; the rates are always valid rates over some real interval. If you
   * need stable rates over a known window, compute them externally from the cumulative
   * totals instead.
   */
  public SourceFlowControlSnapshot snapshot() {
    long nowNanos = System.nanoTime();
    long previousNanos = lastSampleTimeNanos.getAndSet(nowNanos);
    long previousEnqueued = lastSampleEnqueued.getAndSet(totalEnqueued.get());
    long previousDequeued = lastSampleDequeued.getAndSet(totalDequeued.get());
    double elapsedSeconds =
        previousNanos == 0L ? 0.0d : Math.max(0.0d, (nowNanos - previousNanos) / 1_000_000_000.0d);
    long enqueuedNow = totalEnqueued.get();
    long dequeuedNow = totalDequeued.get();
    long pausedSince = pausedSinceEpochMillis.get();
    long pausedSinceNanoValue = pausedSinceNanos.get();
    double totalPausedSeconds =
        Math.max(
            0.0d,
            (totalPausedNanos.get()
                    + (pausedSinceNanoValue <= 0L ? 0L : Math.max(0L, nowNanos - pausedSinceNanoValue)))
                / 1_000_000_000.0d);
    return new SourceFlowControlSnapshot(
        SourceFlowControlSnapshot.Mode.BOUNDED_QUEUE,
        capacity,
        depth.get(),
        maxDepth.get(),
        depth.get() >= capacity,
        backpressureActive.get(),
        pausedSince <= 0L ? null : java.time.Instant.ofEpochMilli(pausedSince).toString(),
        pausedSince <= 0L ? 0.0d : Math.max(0.0d, (System.currentTimeMillis() - pausedSince) / 1000.0d),
        totalPausedSeconds,
        enqueuedNow,
        dequeuedNow,
        totalPauseCount.get(),
        elapsedSeconds <= 0.0d ? 0.0d : Math.max(0.0d, (enqueuedNow - previousEnqueued) / elapsedSeconds),
        elapsedSeconds <= 0.0d ? 0.0d : Math.max(0.0d, (dequeuedNow - previousDequeued) / elapsedSeconds));
  }

  private void producerPaused() {
    blockedProducers.incrementAndGet();
    if (backpressureActive.compareAndSet(false, true)) {
      totalPauseCount.incrementAndGet();
      pausedSinceEpochMillis.compareAndSet(0L, System.currentTimeMillis());
      pausedSinceNanos.compareAndSet(0L, System.nanoTime());
      log.warn(
          "Source event fetching is paused because the bounded source queue is full (depth={} capacity={})",
          depth.get(),
          capacity);
    }
  }

  private void producerUnblocked() {
    blockedProducers.updateAndGet(current -> Math.max(0, current - 1));
    maybeClearBackpressure();
  }

  private void maybeClearBackpressure() {
    if (!backpressureActive.get()) {
      return;
    }
    if (blockedProducers.get() > 0) {
      return;
    }
    if (depth.get() > resumeDepthThreshold) {
      return;
    }
    if (backpressureActive.compareAndSet(true, false)) {
      long pausedSince = pausedSinceEpochMillis.getAndSet(0L);
      long pausedSinceNanoValue = pausedSinceNanos.getAndSet(0L);
      if (pausedSinceNanoValue > 0L) {
        totalPausedNanos.addAndGet(Math.max(0L, System.nanoTime() - pausedSinceNanoValue));
      }
      if (pausedSince > 0L) {
        double pausedSeconds = Math.max(0.0d, (System.currentTimeMillis() - pausedSince) / 1000.0d);
        log.info(
            "Source event fetching resumed after bounded-queue backpressure (depth={} capacity={} pausedSeconds={})",
            depth.get(),
            capacity,
            String.format("%.1f", pausedSeconds));
      }
    }
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static int requirePositive(int value, String name) {
    if (value <= 0) {
      throw new IllegalArgumentException(name + " must be > 0");
    }
    return value;
  }

  private static int defaultResumeDrainCount(int capacity) {
    if (capacity <= 1) {
      return 0;
    }
    return Math.min(1024, Math.max(1, capacity / 8));
  }
}
