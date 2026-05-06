package io.github.aandreakis.dblog.controlplane.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.testsupport.ChangeEventTestFixtures;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class AsyncControlPlaneEventStoreTests {
  @Test
  void flushesPendingWritesBeforeServingReads() throws Exception {
    try (AsyncControlPlaneEventStore store =
        new AsyncControlPlaneEventStore(new InMemoryControlPlaneEventStore(10))) {
      store.record("stage-a", List.of(event(1)));

      assertThat(store.cumulativeEventsObserved()).isEqualTo(1L);
      assertThat(store.recentEventWindowSize()).isEqualTo(1);
      assertThat(store.recentEvents(10)).hasSize(1);
    }
  }

  @Test
  void recordReturnsWithoutBlockingOnSlowDelegate() throws Exception {
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    ControlPlaneEventStore slowDelegate =
        new ControlPlaneEventStore() {
          @Override
          public void record(String stageLabel, List<ChangeEvent> events) {
            started.countDown();
            try {
              release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
            }
          }

          @Override
          public List<RecordedEvent> recentEvents(int limit) {
            return List.of();
          }

          @Override
          public List<RecordedEvent> recentEventsForTable(String tableDisplayName, int limit) {
            return List.of();
          }

          @Override
          public List<TableEventSummary> tableSummaries() {
            return List.of();
          }

          @Override
          public int recentEventWindowSize() {
            return 0;
          }

          @Override
          public long cumulativeEventsObserved() {
            return 0L;
          }
        };
    try (AsyncControlPlaneEventStore store = new AsyncControlPlaneEventStore(slowDelegate)) {
      assertTimeoutPreemptively(
          Duration.ofMillis(250), () -> store.record("stage-a", List.of(event(1))));
      assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
      release.countDown();
    }
  }

  private static ChangeEvent event(int id) {
    return ChangeEventTestFixtures.fromRowMaps(
        new TableId("source", "appdb", "widgets"),
        OperationType.UPDATE,
        CaptureOrigin.LOG,
        Map.of("id", id),
        null,
        Map.of("id", id),
        new OpaqueSourcePosition("source:" + id),
        "tx-" + id,
        null);
  }
}
