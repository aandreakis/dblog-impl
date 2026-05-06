package io.github.aandreakis.dblog.controlplane.service;

import io.github.aandreakis.dblog.core.model.ChangeEvent;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

public final class InMemoryControlPlaneEventStore implements ControlPlaneEventStore {
  private final int capacity;
  private final ArrayList<RecordedEvent> events = new ArrayList<>();
  private long cumulativeEventsObserved;

  public InMemoryControlPlaneEventStore(int capacity) {
    if (capacity <= 0) {
      throw new IllegalArgumentException("capacity must be > 0");
    }
    this.capacity = capacity;
  }

  @Override
  public synchronized void record(String stageLabel, List<ChangeEvent> events) {
    Objects.requireNonNull(stageLabel, "stageLabel");
    Objects.requireNonNull(events, "events");
    Instant recordedAt = Instant.now();
    for (ChangeEvent event : events) {
      this.events.add(new RecordedEvent(stageLabel, recordedAt, event));
      cumulativeEventsObserved++;
    }
    trimToCapacity();
  }

  @Override
  public synchronized List<RecordedEvent> recentEvents(int limit) {
    if (limit <= 0) {
      return List.of();
    }
    int fromIndex = Math.max(0, events.size() - limit);
    return List.copyOf(events.subList(fromIndex, events.size()));
  }

  @Override
  public synchronized List<RecordedEvent> recentEventsForTable(String tableDisplayName, int limit) {
    Objects.requireNonNull(tableDisplayName, "tableDisplayName");
    if (limit <= 0) {
      return List.of();
    }
    List<RecordedEvent> filtered =
        events.stream()
            .filter(recordedEvent -> recordedEvent.event().tableId().displayName().equals(tableDisplayName))
            .toList();
    int fromIndex = Math.max(0, filtered.size() - limit);
    return List.copyOf(filtered.subList(fromIndex, filtered.size()));
  }

  @Override
  public synchronized List<TableEventSummary> tableSummaries() {
    LinkedHashMap<String, Integer> counts = new LinkedHashMap<>();
    for (RecordedEvent recordedEvent : events) {
      String tableDisplayName = recordedEvent.event().tableId().displayName();
      counts.merge(tableDisplayName, 1, Integer::sum);
    }
    return counts.entrySet().stream()
        .map(entry -> new TableEventSummary(entry.getKey(), entry.getValue()))
        .toList();
  }

  @Override
  public synchronized int recentEventWindowSize() {
    return events.size();
  }

  @Override
  public synchronized long cumulativeEventsObserved() {
    return cumulativeEventsObserved;
  }

  private void trimToCapacity() {
    int overflow = events.size() - capacity;
    if (overflow <= 0) {
      return;
    }
    events.subList(0, overflow).clear();
  }
}
