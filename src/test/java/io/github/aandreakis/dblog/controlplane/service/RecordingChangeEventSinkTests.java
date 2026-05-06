package io.github.aandreakis.dblog.controlplane.service;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.sink.api.ChangeEventBatchContext;
import io.github.aandreakis.dblog.sink.api.ChangeEventSink;
import io.github.aandreakis.dblog.sink.api.SinkSchemaValidator;
import io.github.aandreakis.dblog.testsupport.ChangeEventTestFixtures;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class RecordingChangeEventSinkTests {
  @Test
  void recordsContextStageLabelIntoEventStore() {
    InMemoryControlPlaneEventStore eventStore = new InMemoryControlPlaneEventStore(10);
    List<ChangeEvent> delegated = new ArrayList<>();
    RecordingChangeEventSink sink =
        new RecordingChangeEventSink(delegated::addAll, eventStore);

    sink.appendEvents(
        new ChangeEventBatchContext("request-batch-42", Instant.parse("2026-04-10T00:00:00Z")),
        List.of(event(1)));

    assertThat(delegated).hasSize(1);
    assertThat(eventStore.recentEvents(10)).singleElement()
        .extracting(io.github.aandreakis.dblog.controlplane.service.ControlPlaneEventStore.RecordedEvent::stageLabel)
        .isEqualTo("request-batch-42");
  }

  @Test
  void forwardsValidateCapturedSchemasToSchemaAwareDelegate() throws Exception {
    // Regression: when the runtime wraps a SinkSchemaValidator (e.g. JdbcTypedChangeEventSink)
    // in this recording wrapper, the bootstrap-time schema announcement must reach the wrapped
    // sink — otherwise the typed sink never builds its mirror tables and silently drops every
    // event the rest of the pipeline emits.
    AtomicReference<List<TableSchema>> received = new AtomicReference<>();
    class SchemaAwareSink implements ChangeEventSink, SinkSchemaValidator {
      @Override
      public void appendEvents(List<ChangeEvent> events) {}

      @Override
      public void validateCapturedSchemas(List<TableSchema> capturedSchemas) {
        received.set(capturedSchemas);
      }

      @Override
      public void close() {}
    }
    SchemaAwareSink delegate = new SchemaAwareSink();
    RecordingChangeEventSink wrapper =
        new RecordingChangeEventSink(delegate, new InMemoryControlPlaneEventStore(8));

    TableSchema schema =
        TableSchema.create(
            new TableId("source", "appdb", "widgets"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false)),
            Instant.parse("2026-04-18T00:00:00Z"));
    wrapper.validateCapturedSchemas(List.of(schema));

    assertThat(received.get()).containsExactly(schema);
  }

  @Test
  void validateCapturedSchemasNoOpsWhenDelegateIsNotSchemaAware() throws Exception {
    // A non-schema-aware delegate (e.g. NDJSON, NoOp) must not cause the call to throw — the
    // recording wrapper should silently no-op.
    RecordingChangeEventSink wrapper =
        new RecordingChangeEventSink(events -> {}, new InMemoryControlPlaneEventStore(4));

    wrapper.validateCapturedSchemas(List.of());
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
