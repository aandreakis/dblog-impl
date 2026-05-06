package io.github.aandreakis.dblog.controlplane.service;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.testsupport.ChangeEventTestFixtures;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ControlPlaneEventStoreTests {
  @Test
  void recordsRecentEventsAndTableSummariesWithCapacityLimit() {
    InMemoryControlPlaneEventStore store = new InMemoryControlPlaneEventStore(2);

    store.record(
        "stage-a",
        List.of(
            event("appdb.public.customers", 1),
            event("appdb.public.orders", 2)));
    store.record("stage-b", List.of(event("appdb.public.customers", 3)));

    // Three events observed cumulatively but only two fit in the bounded window.
    assertThat(store.cumulativeEventsObserved()).isEqualTo(3L);
    assertThat(store.recentEventWindowSize()).isEqualTo(2);
    assertThat(store.recentEvents(10)).hasSize(2);
    assertThat(store.tableSummaries())
        .extracting(ControlPlaneEventStore.TableEventSummary::tableDisplayName)
        .containsExactly("appdb.public.orders", "appdb.public.customers");
    assertThat(store.recentEventsForTable("appdb.public.customers", 10)).hasSize(1);
  }

  @Test
  void cumulativeCounterGrowsBeyondTheBoundedWindowCapacity() {
    InMemoryControlPlaneEventStore store = new InMemoryControlPlaneEventStore(4);

    for (int batch = 0; batch < 100; batch++) {
      store.record(
          "stage-bulk",
          List.of(
              event("appdb.public.customers", batch * 4),
              event("appdb.public.customers", batch * 4 + 1),
              event("appdb.public.customers", batch * 4 + 2),
              event("appdb.public.customers", batch * 4 + 3)));
    }

    assertThat(store.cumulativeEventsObserved()).isEqualTo(400L);
    assertThat(store.recentEventWindowSize()).isEqualTo(4);
  }

  private static ChangeEvent event(String displayName, int id) {
    String[] parts = displayName.split("\\.");
    return ChangeEventTestFixtures.fromRowMaps(
        new TableId(parts[0], parts[1], parts[2]),
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
