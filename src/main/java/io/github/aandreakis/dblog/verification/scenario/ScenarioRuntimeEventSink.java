package io.github.aandreakis.dblog.verification.scenario;

import io.github.aandreakis.dblog.controlplane.service.ControlPlaneEventCapture;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.sink.api.ChangeEventBatchContext;
import io.github.aandreakis.dblog.sink.api.ContextualChangeEventSink;
import java.util.List;
import java.util.Objects;

/** Runtime-loop sink adapter for the persistent scenario store. */
public final class ScenarioRuntimeEventSink implements ContextualChangeEventSink {
  private final ScenarioStore store;
  private final String scenarioId;
  private final ControlPlaneEventCapture eventCapture;

  public ScenarioRuntimeEventSink(ScenarioStore store, String scenarioId) {
    this(store, scenarioId, new ControlPlaneEventCapture());
  }

  public ScenarioRuntimeEventSink(
      ScenarioStore store, String scenarioId, ControlPlaneEventCapture eventCapture) {
    this.store = Objects.requireNonNull(store, "store");
    this.scenarioId = Objects.requireNonNull(scenarioId, "scenarioId");
    this.eventCapture = Objects.requireNonNull(eventCapture, "eventCapture");
  }

  @Override
  public void appendEvents(List<ChangeEvent> events) {
    appendEvents(new ChangeEventBatchContext("runtime", null), events);
  }

  @Override
  public void appendEvents(ChangeEventBatchContext context, List<ChangeEvent> events) {
    ChangeEventBatchContext batchContext = Objects.requireNonNull(context, "context");
    List<ChangeEvent> emittedEvents = Objects.requireNonNull(events, "events");
    store.appendEvents(
        scenarioId,
        batchContext.stageLabel(),
        filterControlEvents(emittedEvents));
    eventCapture.recordEmitted(batchContext.stageLabel(), emittedEvents);
  }

  @Override
  public void close() throws Exception {
    // Nothing owned here beyond the wrapped store lifecycle outside the sink boundary.
  }

  private static List<ChangeEvent> filterControlEvents(List<ChangeEvent> events) {
    return events.stream()
        .filter(
            event ->
                event.operationType() != OperationType.WATERMARK
                    && event.operationType() != OperationType.HEARTBEAT)
        .toList();
  }
}
