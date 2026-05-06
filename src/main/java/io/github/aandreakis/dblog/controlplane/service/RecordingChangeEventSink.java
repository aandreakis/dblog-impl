package io.github.aandreakis.dblog.controlplane.service;

import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.sink.api.ChangeEventBatchContext;
import io.github.aandreakis.dblog.sink.api.ChangeEventSink;
import io.github.aandreakis.dblog.sink.api.ContextualChangeEventSink;
import io.github.aandreakis.dblog.sink.api.SinkSchemaValidator;
import java.util.List;
import java.util.Objects;

public final class RecordingChangeEventSink
    implements ContextualChangeEventSink, SinkSchemaValidator {
  private final ChangeEventSink delegate;
  private final ControlPlaneEventStore eventStore;

  public RecordingChangeEventSink(ChangeEventSink delegate, ControlPlaneEventStore eventStore) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.eventStore = Objects.requireNonNull(eventStore, "eventStore");
  }

  @Override
  public void appendEvents(List<ChangeEvent> events) {
    appendEvents(new ChangeEventBatchContext("unspecified", null), events);
  }

  @Override
  public void appendEvents(ChangeEventBatchContext context, List<ChangeEvent> events) {
    Objects.requireNonNull(context, "context");
    delegate.appendEvents(events);
    eventStore.record(context.stageLabel(), events);
  }

  // The bootstrap calls validateCapturedSchemas exactly once on the sink it was handed; if we
  // (the recording wrapper) do not forward it, schema-aware delegates such as the typed-h2 sink
  // never learn the captured schemas, never build their mirror tables, and silently drop every
  // emitted event. Every event-capture-enabled DbLogApplication wraps the configured
  // sink in this class, so this forwarder is on the production path.
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
}
