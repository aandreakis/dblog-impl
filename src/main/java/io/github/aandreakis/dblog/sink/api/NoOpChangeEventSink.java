package io.github.aandreakis.dblog.sink.api;

import io.github.aandreakis.dblog.core.model.ChangeEvent;
import java.util.List;
import java.util.Objects;

/** Explicit discard sink for runs that intentionally want to drop emitted events. */
public final class NoOpChangeEventSink implements ChangeEventSink {
  private static final NoOpChangeEventSink INSTANCE = new NoOpChangeEventSink();

  private NoOpChangeEventSink() {}

  public static NoOpChangeEventSink instance() {
    return INSTANCE;
  }

  @Override
  public void appendEvents(List<ChangeEvent> events) {
    Objects.requireNonNull(events, "events");
  }
}
