package io.github.aandreakis.dblog.controlplane.service;

import io.github.aandreakis.dblog.core.model.ChangeEvent;
import java.time.Instant;
import java.util.List;

/**
 * In-memory control-plane event store used by {@link ControlPlaneQueryService} to serve
 * the TUI and HTTP event-inspection surfaces.
 *
 * <p>The store keeps a bounded ring buffer of {@link RecordedEvent} records for recent
 * inspection. It additionally tracks a monotonic count of every event ever recorded so
 * operators can distinguish "how many events went through" (cumulative) from "how many
 * events are currently in the inspection window" (bounded).
 */
public interface ControlPlaneEventStore {
  void record(String stageLabel, List<ChangeEvent> events);

  List<RecordedEvent> recentEvents(int limit);

  List<RecordedEvent> recentEventsForTable(String tableDisplayName, int limit);

  List<TableEventSummary> tableSummaries();

  /** Current number of events retained in the bounded inspection window. */
  int recentEventWindowSize();

  /**
   * Total number of events ever observed by this store, including events evicted from the
   * bounded inspection window. This is the number operators typically mean by "how many
   * events have flowed through." Not persisted across process restarts.
   */
  long cumulativeEventsObserved();

  static ControlPlaneEventStore noop() {
    return new ControlPlaneEventStore() {
      @Override
      public void record(String stageLabel, List<ChangeEvent> events) {}

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
  }

  record RecordedEvent(String stageLabel, Instant recordedAt, ChangeEvent event) {}

  /**
   * Per-table event count within the bounded inspection window. Named to make clear it is
   * window-scoped, not cumulative — a table could have had millions of events observed
   * but only 12 still in the window if newer events from other tables have rolled them
   * off.
   */
  record TableEventSummary(String tableDisplayName, int recentEventCountInWindow) {}
}
