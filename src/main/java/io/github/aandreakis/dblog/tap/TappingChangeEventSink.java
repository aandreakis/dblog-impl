package io.github.aandreakis.dblog.tap;

import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.sink.api.ChangeEventBatchContext;
import io.github.aandreakis.dblog.sink.api.ChangeEventSink;
import io.github.aandreakis.dblog.sink.api.ContextualChangeEventSink;
import io.github.aandreakis.dblog.sink.api.SinkSchemaValidator;
import java.util.List;
import java.util.Objects;

/**
 * Decorates a single sink delegate so each successfully appended event produces one
 * {@code tap.onSinkEvent(event, sinkName)} call. If the delegate throws, no tap event is emitted,
 * matching spec §8 "preserve sink-failure semantics".
 *
 * <p>One instance per delegate sink. For a pipeline with N sinks, each event produces N tap
 * events (one per sink).
 */
public final class TappingChangeEventSink implements ContextualChangeEventSink, SinkSchemaValidator {
  private final ChangeEventSink delegate;
  private final String sinkName;
  private final Tap tap;

  public TappingChangeEventSink(ChangeEventSink delegate, String sinkName, Tap tap) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    if (sinkName == null || sinkName.isBlank()) {
      throw new IllegalArgumentException("sinkName must be non-blank");
    }
    this.sinkName = sinkName;
    this.tap = Objects.requireNonNull(tap, "tap");
  }

  @Override
  public void appendEvents(List<ChangeEvent> events) {
    Objects.requireNonNull(events, "events");
    delegate.appendEvents(events);
    emitTapEvents(events);
  }

  @Override
  public void appendEvents(ChangeEventBatchContext context, List<ChangeEvent> events) {
    Objects.requireNonNull(context, "context");
    Objects.requireNonNull(events, "events");
    if (delegate instanceof ContextualChangeEventSink contextual) {
      contextual.appendEvents(context, events);
    } else {
      delegate.appendEvents(events);
    }
    emitTapEvents(events);
  }

  @Override
  public void validateCapturedSchemas(List<TableSchema> capturedSchemas) {
    if (delegate instanceof SinkSchemaValidator validator) {
      validator.validateCapturedSchemas(capturedSchemas);
    }
  }

  @Override
  public void requestStop() {
    delegate.requestStop();
  }

  @Override
  public void close() throws Exception {
    delegate.close();
  }

  private void emitTapEvents(List<ChangeEvent> events) {
    for (ChangeEvent event : events) {
      tap.onSinkEvent(event, sinkName);
    }
  }
}
