package io.github.aandreakis.dblog.sink.api;

import io.github.aandreakis.dblog.core.model.ChangeEvent;
import java.util.List;

/**
 * Opt-in {@link ChangeEventSink} extension that receives the per-batch {@link
 * ChangeEventBatchContext} alongside the events. The runtime detects this interface via {@code
 * instanceof} and routes through the contextual overload when available; sinks that do not
 * implement it still receive batches through {@link ChangeEventSink#appendEvents(List)}.
 */
public interface ContextualChangeEventSink extends ChangeEventSink {
  void appendEvents(ChangeEventBatchContext context, List<ChangeEvent> events);
}
