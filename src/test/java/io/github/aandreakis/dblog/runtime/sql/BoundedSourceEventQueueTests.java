package io.github.aandreakis.dblog.runtime.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class BoundedSourceEventQueueTests {
  @Test
  void tracksQueueDepthAndSnapshotAccounting() {
    BoundedSourceEventQueue<String> queue = new BoundedSourceEventQueue<>("mysql", 4);

    assertThat(queue.enqueue("a")).isTrue();
    assertThat(queue.enqueue("b")).isTrue();
    assertThat(queue.depth()).isEqualTo(2);

    SourceFlowControlSnapshot snapshot = queue.snapshot();
    assertThat(snapshot.mode()).isEqualTo(SourceFlowControlSnapshot.Mode.BOUNDED_QUEUE);
    assertThat(snapshot.queueCapacity()).isEqualTo(4);
    assertThat(snapshot.queueDepth()).isEqualTo(2);
    assertThat(snapshot.totalEnqueued()).isEqualTo(2L);

    assertThat(queue.pollNow()).contains("a");
    assertThat(queue.depth()).isEqualTo(1);
    assertThat(queue.snapshot().totalDequeued()).isEqualTo(1L);
  }

  @Test
  void stopsAcceptingEventsAfterClose() {
    BoundedSourceEventQueue<String> queue = new BoundedSourceEventQueue<>("postgres", 2);

    queue.close();

    assertThat(queue.enqueue("a")).isFalse();
    assertThat(queue.pollNow()).isEmpty();
  }

  @Test
  void reportsPausedSourceFetchingWhenQueueIsFullUntilConsumerDrains() throws Exception {
    BoundedSourceEventQueue<String> queue = new BoundedSourceEventQueue<>("mysql", 1);
    assertThat(queue.enqueue("first")).isTrue();

    CountDownLatch producerStarted = new CountDownLatch(1);
    CountDownLatch producerFinished = new CountDownLatch(1);
    AtomicReference<Throwable> producerFailure = new AtomicReference<>();
    Thread producer =
        Thread.ofVirtual()
            .unstarted(
                () -> {
                  producerStarted.countDown();
                  try {
                    queue.enqueue("second");
                  } catch (Throwable failure) {
                    producerFailure.set(failure);
                  } finally {
                    producerFinished.countDown();
                  }
                });
    producer.start();
    producerStarted.await();

    awaitPaused(queue);
    SourceFlowControlSnapshot paused = queue.snapshot();
    assertThat(paused.mode()).isEqualTo(SourceFlowControlSnapshot.Mode.BOUNDED_QUEUE);
    assertThat(paused.queueFull()).isTrue();
    assertThat(paused.sourceFetchPaused()).isTrue();
    assertThat(paused.queueDepth()).isEqualTo(1);

    assertThat(queue.pollNow()).contains("first");
    producerFinished.await();
    assertThat(producerFailure.get()).isNull();

    assertThat(queue.pollNow()).contains("second");
    awaitResumed(queue);

    SourceFlowControlSnapshot resumed = queue.snapshot();
    assertThat(resumed.sourceFetchPaused()).isFalse();
    assertThat(resumed.totalPauseCount()).isEqualTo(1L);
    assertThat(resumed.totalPausedSeconds()).isGreaterThan(0.0d);
    assertThat(resumed.totalEnqueued()).isEqualTo(2L);
    assertThat(resumed.totalDequeued()).isEqualTo(2L);
  }

  @Test
  void keepsBackpressureLatchedUntilQueueDrainsBelowResumeThreshold() throws Exception {
    BoundedSourceEventQueue<String> queue = new BoundedSourceEventQueue<>("mysql", 100);
    for (int index = 0; index < 100; index++) {
      assertThat(queue.enqueue("seed-" + index)).isTrue();
    }

    CountDownLatch producerFinished = new CountDownLatch(1);
    Thread producer =
        Thread.ofVirtual()
            .unstarted(
                () -> {
                  try {
                    queue.enqueue("extra");
                  } finally {
                    producerFinished.countDown();
                  }
                });
    producer.start();

    awaitPaused(queue);
    assertThat(queue.pollNow()).contains("seed-0");
    producerFinished.await();

    SourceFlowControlSnapshot stillPaused = queue.snapshot();
    assertThat(stillPaused.sourceFetchPaused()).isTrue();
    assertThat(stillPaused.queueDepth()).isEqualTo(100);

    for (int index = 1; index <= 12; index++) {
      assertThat(queue.pollNow()).contains("seed-" + index);
    }
    awaitResumed(queue);
    SourceFlowControlSnapshot resumed = queue.snapshot();
    assertThat(resumed.sourceFetchPaused()).isFalse();
    assertThat(resumed.queueDepth()).isLessThanOrEqualTo(88);
    assertThat(resumed.totalPauseCount()).isEqualTo(1L);
  }

  @Test
  void rejectsSecondConsumerThreadWhenSingleConsumerInvariantIsAsserted() throws Exception {
    BoundedSourceEventQueue<String> queue = new BoundedSourceEventQueue<>("mysql", 2);
    // First caller (this test thread) claims the consumer slot.
    queue.assertSingleConsumer();
    queue.assertSingleConsumer(); // idempotent on same thread

    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread other =
        Thread.ofVirtual()
            .unstarted(
                () -> {
                  try {
                    queue.assertSingleConsumer();
                  } catch (Throwable t) {
                    failure.set(t);
                  }
                });
    other.start();
    other.join();

    assertThat(failure.get())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("single-consumer invariant");
  }

  @Test
  void singleConsumerAssertionIsOffByDefaultOnPollNow() {
    // Default JVM run must not have -Ddblog.assertions=true, so pollNow() must not trip
    // even when two different threads drain. This guards against accidentally enabling
    // the check in production builds.
    assertThat(BoundedSourceEventQueue.ASSERT_SINGLE_CONSUMER).isFalse();

    BoundedSourceEventQueue<String> queue = new BoundedSourceEventQueue<>("mysql", 2);
    queue.enqueue("a");
    queue.enqueue("b");

    assertThat(queue.pollNow()).contains("a");
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread other =
        Thread.ofVirtual()
            .unstarted(
                () -> {
                  try {
                    assertThat(queue.pollNow()).contains("b");
                  } catch (Throwable t) {
                    failure.set(t);
                  }
                });
    other.start();
    try {
      other.join();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new AssertionError(ex);
    }
    assertThat(failure.get()).isNull();
  }

  @Test
  void unblocksProducerPromptlyWhenMarkFailedIsCalledWhileQueueIsFull() throws Exception {
    // Reproduces the deadlock class where a producer thread is blocked on a full queue and the
    // upstream transport has hit a fatal failure. Without markFailed the producer would spin on
    // offer(..., 100ms) until close() is called externally — and close() is usually driven by
    // the consumer, which may itself be stuck. markFailed makes the producer exit on its next
    // offer-retry boundary.
    BoundedSourceEventQueue<String> queue = new BoundedSourceEventQueue<>("mysql", 1);
    assertThat(queue.enqueue("first")).isTrue();

    AtomicReference<Boolean> producerResult = new AtomicReference<>();
    CountDownLatch producerStarted = new CountDownLatch(1);
    Thread producer =
        Thread.ofVirtual()
            .unstarted(
                () -> {
                  producerStarted.countDown();
                  producerResult.set(queue.enqueue("second-should-block"));
                });
    producer.start();

    assertThat(producerStarted.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
    awaitPaused(queue);

    long t0 = System.nanoTime();
    queue.markFailed(new java.io.IOException("simulated disconnect"));

    producer.join(java.time.Duration.ofSeconds(2).toMillis());
    assertThat(producer.isAlive()).isFalse();
    long elapsedMillis = java.time.Duration.ofNanos(System.nanoTime() - t0).toMillis();

    assertThat(producerResult.get()).isFalse();
    // Should unblock within one offer-retry boundary (100ms) plus scheduler jitter. 800ms is a
    // generous ceiling that still catches regressions that reintroduce the deadlock.
    assertThat(elapsedMillis).isLessThan(800L);

    // After markFailed, fresh enqueues fail fast rather than block.
    long enqueueStart = System.nanoTime();
    assertThat(queue.enqueue("late")).isFalse();
    long enqueueElapsedMillis = java.time.Duration.ofNanos(System.nanoTime() - enqueueStart).toMillis();
    assertThat(enqueueElapsedMillis).isLessThan(100L);
  }

  private static void awaitPaused(BoundedSourceEventQueue<?> queue) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    while (System.nanoTime() < deadline) {
      if (queue.snapshot().sourceFetchPaused()) {
        return;
      }
      Thread.sleep(10L);
    }
    throw new IllegalStateException("Timed out waiting for bounded queue pause state");
  }

  private static void awaitResumed(BoundedSourceEventQueue<?> queue) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    while (System.nanoTime() < deadline) {
      if (!queue.snapshot().sourceFetchPaused()) {
        return;
      }
      Thread.sleep(10L);
    }
    throw new IllegalStateException("Timed out waiting for bounded queue resume state");
  }
}
