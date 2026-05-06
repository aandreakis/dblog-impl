package io.github.aandreakis.dblog.tap;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bounded byte-array queue sitting between the pump thread (producer) and the HTTP streaming
 * thread (consumer). Deliberately small so the TUI's pace reaches back to the producer as
 * backpressure; {@link #put(byte[])} blocks when the queue is full so the pump stalls until the
 * subscriber drains space. That's the educational mechanism — it would be a terrible idea in
 * production.
 *
 * <p>The producer stores the moment {@link #put(byte[])} last found the queue full in
 * {@link #queueFullSinceNanos()} and clears it once the blocked put completes or a non-blocking
 * {@link #offer(byte[])} succeeds. The HTTP streaming thread polls that field to decide when to
 * emit the {@code stream.standby} / {@code stream.resumed} control events.
 *
 * <p>The first time the pump thread blocks on {@link #put(byte[])} we log a one-shot WARN so an
 * operator who enabled the tap and forgot to attach a subscriber gets a clear signal rather than
 * a silently stalled DBLog. The signal is deliberately rate-limited to one WARN per queue
 * lifetime; the subsequent {@code stream.standby} events on the wire handle recurring stalls.
 */
final class TapQueue {
  private static final Logger log = LoggerFactory.getLogger(TapQueue.class);
  private final BlockingQueue<byte[]> queue;
  private final int capacity;
  private volatile long queueFullSinceNanos = -1L;
  private final AtomicBoolean firstBlockWarned = new AtomicBoolean(false);

  TapQueue(int capacity) {
    if (capacity < 16) {
      throw new IllegalArgumentException("queue capacity must be >= 16, got " + capacity);
    }
    this.capacity = capacity;
    this.queue = new ArrayBlockingQueue<>(capacity);
  }

  int capacity() {
    return capacity;
  }

  int depth() {
    return queue.size();
  }

  /**
   * Blocking enqueue used by the pump thread. Blocks until space is available. Updates
   * {@link #queueFullSinceNanos()} so the standby detector can observe how long the producer has
   * been waiting. Logs a one-shot WARN the very first time the pump blocks on a full queue, so
   * an operator who enabled the tap and forgot to attach a subscriber gets a clear signal.
   * Subsequent stalls are surfaced on the wire via {@code stream.standby} events; no further
   * server-side logs are emitted to keep the log stream clean.
   */
  void put(byte[] line) throws InterruptedException {
    if (queue.offer(line)) {
      queueFullSinceNanos = -1L;
      return;
    }
    if (queueFullSinceNanos == -1L) {
      queueFullSinceNanos = System.nanoTime();
    }
    if (firstBlockWarned.compareAndSet(false, true)) {
      log.warn(
          "DBLog tap queue is full (capacity={}). The pump thread is now blocked waiting for the"
              + " subscriber to drain. This is the intended educational backpressure mechanism"
              + " — attach a reader to GET /api/v1/tap/stream, or turn the tap off with"
              + " dblog.tap.enabled=false. This WARN fires once per queue lifetime; subsequent"
              + " stalls surface as stream.standby events on the wire.",
          capacity);
    }
    try {
      queue.put(line);
    } finally {
      queueFullSinceNanos = -1L;
    }
  }

  /**
   * Non-blocking enqueue. Used for fire-and-forget events (heartbeats) where dropping on
   * contention is preferable to blocking the pump thread.
   */
  boolean offer(byte[] line) {
    boolean accepted = queue.offer(line);
    if (accepted) {
      queueFullSinceNanos = -1L;
    } else if (queueFullSinceNanos == -1L) {
      queueFullSinceNanos = System.nanoTime();
    }
    return accepted;
  }

  byte[] poll(long timeout, TimeUnit unit) throws InterruptedException {
    return queue.poll(timeout, unit);
  }

  long queueFullSinceNanos() {
    return queueFullSinceNanos;
  }
}
