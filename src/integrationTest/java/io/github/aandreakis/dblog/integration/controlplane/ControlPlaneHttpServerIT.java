package io.github.aandreakis.dblog.integration.controlplane;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.controlplane.http.ControlPlaneHttpServer;
import io.github.aandreakis.dblog.controlplane.service.ControlPlaneCommandService;
import io.github.aandreakis.dblog.controlplane.service.ControlPlaneQueryService;
import io.github.aandreakis.dblog.controlplane.service.RuntimeStatusProvider;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.core.request.DumpRequestStatus;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.FullDumpRequiredSignal;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.SchemaUncertaintySignal;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag("integration-core")
class ControlPlaneHttpServerIT {
  @TempDir Path tempDir;

  @Test
  void servesRuntimeRequestAndSchemaRoutesOverHttpAgainstRealH2State() throws Exception {
    Path statePath = tempDir.resolve("next-http-it-state");
    TableId tableId = new TableId("sourceA", "appdb", "customers");
    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(statePath)) {
      DumpRequest active = new DumpRequest("42", DumpScope.TABLE, tableId, List.of());
      DumpRequest queued = new DumpRequest("45", DumpScope.ALL_TABLES, null, List.of());
      stateStore.dumpRequests().upsert(active);
      stateStore.dumpRequests().upsert(queued);
      stateStore.dumpRequests().saveStatus(DumpRequestStatus.active(active));
      stateStore.schemas().saveContractSchema(contractSchema(tableId));
      stateStore.schemas().saveObservedSchema(observedSchema(tableId));
      stateStore.schemas()
          .saveFullDumpRequiredSignal(
              new FullDumpRequiredSignal(
                  "sourceA",
                  tableId,
                  "full dump required because data loss was detected",
                  Instant.parse("2026-04-01T00:06:00Z")));
      stateStore.schemas()
          .saveSchemaUncertaintySignal(
              new SchemaUncertaintySignal(
                  "sourceA",
                  tableId,
                  "schema continuity is still being reconciled",
                  Instant.parse("2026-04-01T00:07:00Z"),
                  Instant.parse("2026-04-01T00:08:00Z"),
                  2));

      SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
      meterRegistry.counter("dblog.runtime.source_capture.polls.total", "adapter", "mysql").increment(2.0d);
      ControlPlaneQueryService queryService =
          new ControlPlaneQueryService(
              stateStore,
              () ->
                  new RuntimeStatusProvider.RuntimeStatusSnapshot(
                      "runtime",
                      "mysql",
                      "UP",
                      true,
                      "request submission available",
                      "sourceA",
                      "mysql-bin.000001:42",
                      0,
                      1),
              io.github.aandreakis.dblog.controlplane.service.ControlPlaneEventStore.noop(),
              meterRegistry,
              "mysql");
      ControlPlaneCommandService commandService =
          new ControlPlaneCommandService(
              stateStore,
              () ->
                  new RuntimeStatusProvider.RuntimeStatusSnapshot(
                      "runtime",
                      "mysql",
                      "UP",
                      true,
                      "request submission available",
                      "sourceA",
                      "mysql-bin.000001:42",
                      0,
                      1),
              (scope, submittedTableId, primaryKeyLiterals) ->
                  stateStore.dumpRequests().createGenerated(scope, submittedTableId, primaryKeyLiterals));

      ControlPlaneHttpServer server =
          new ControlPlaneHttpServer(queryService, commandService, "127.0.0.1", 0);
      server.start();
      try {
        String baseUrl = "http://127.0.0.1:" + server.boundPort();

        HttpResponse<String> runtimeResponse = get(baseUrl + "/api/v1/runtime");
        assertThat(runtimeResponse.statusCode()).isEqualTo(200);
        assertThat(runtimeResponse.body())
            .contains("\"mode\":\"runtime\"")
            .contains("\"adapter\":\"mysql\"")
            .contains("\"lastAcknowledgedCheckpoint\":\"mysql-bin.000001:42\"");

        HttpResponse<String> runtimeStatusResponse = get(baseUrl + "/api/v1/runtime/status");
        assertThat(runtimeStatusResponse.statusCode()).isEqualTo(200);
        assertThat(runtimeStatusResponse.body())
            .contains("\"queuedCount\":1")
            .contains("\"activeCount\":1")
            .contains("\"pendingCount\":2");

        HttpResponse<String> metricsResponse = get(baseUrl + "/api/v1/metrics");
        assertThat(metricsResponse.statusCode()).isEqualTo(200);
        assertThat(metricsResponse.body())
            .contains("\"runtimeMeasurement\"")
            .contains("\"sourceCapture\"")
            .contains("\"polls\":2.0");

        HttpResponse<String> requestsResponse = get(baseUrl + "/api/v1/requests");
        assertThat(requestsResponse.statusCode()).isEqualTo(200);
        assertThat(requestsResponse.body())
            .contains("\"requestId\":\"42\"")
            .contains("\"requestId\":\"45\"")
            .contains("\"countsByState\"")
            .contains("\"createdAt\"")
            .contains("\"updatedAt\"");

        HttpResponse<String> schemasResponse = get(baseUrl + "/api/v1/runtime/schemas");
        assertThat(schemasResponse.statusCode()).isEqualTo(200);
        assertThat(schemasResponse.body())
            .contains("\"FULL_DUMP_REQUIRED\"")
            .contains("\"refreshedAt\":\"2026-04-01T00:05:00Z\"")
            .doesNotContain("\"refreshedAt\":\"1970-01-01T00:00:00Z\"")
            .contains("legacy_note");

        HttpResponse<String> schemaIssuesResponse = get(baseUrl + "/api/v1/runtime/schema-issues");
        assertThat(schemaIssuesResponse.statusCode()).isEqualTo(200);
        assertThat(schemaIssuesResponse.body())
            .contains("full dump required because data loss was detected")
            .contains("schema continuity is still being reconciled");

        HttpResponse<String> submitResponse =
            postJson(
                baseUrl + "/api/v1/requests",
                """
                {"scope":"PRIMARY_KEYS","table":{"databaseName":"sourceA","schemaName":"appdb","tableName":"customers"},"primaryKeyLiterals":["7","8"]}
                """);
        assertThat(submitResponse.statusCode()).isEqualTo(201);
        assertThat(submitResponse.body())
            .contains("\"accepted\":true")
            .contains("\"scope\":\"PRIMARY_KEYS\"");
      } finally {
        server.stop();
        meterRegistry.close();
      }
    }
  }

  @Test
  void rejectsRequestBodiesLargerThanConfiguredLimitWith413() throws Exception {
    Path statePath = tempDir.resolve("next-http-it-too-large");
    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(statePath)) {
      // Use a tiny 256-byte limit to keep the test body small and fast.
      int bodyLimit = 256;

      SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
      ControlPlaneHttpServer server =
          new ControlPlaneHttpServer(
              defaultQueryService(stateStore, meterRegistry),
              defaultCommandService(stateStore),
              "127.0.0.1",
              0,
              false,
              4,
              8,
              bodyLimit);
      server.start();
      try {
        String baseUrl = "http://127.0.0.1:" + server.boundPort();

        // Build an oversized body: JSON object whose primaryKeyLiterals list is padded past
        // the configured limit. The content of the keys is not what matters — the server
        // short-circuits on body size before parsing.
        StringBuilder body = new StringBuilder("{\"scope\":\"PRIMARY_KEYS\",\"primaryKeyLiterals\":[");
        for (int i = 0; i < 200; i++) {
          if (i > 0) {
            body.append(',');
          }
          body.append('"').append("padding-").append(i).append('"');
        }
        body.append("]}");
        assertThat(body.length()).isGreaterThan(bodyLimit);

        HttpResponse<String> response = postJson(baseUrl + "/api/v1/requests", body.toString());

        assertThat(response.statusCode()).isEqualTo(413);
        assertThat(response.body())
            .contains("\"error\":\"request_too_large\"")
            .contains(String.valueOf(bodyLimit));
      } finally {
        server.stop();
        meterRegistry.close();
      }
    }
  }

  @Test
  void rejectsMalformedJsonBodiesWith400() throws Exception {
    Path statePath = tempDir.resolve("next-http-it-malformed");
    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(statePath)) {
      SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
      ControlPlaneHttpServer server =
          new ControlPlaneHttpServer(
              defaultQueryService(stateStore, meterRegistry),
              defaultCommandService(stateStore),
              "127.0.0.1",
              0);
      server.start();
      try {
        String baseUrl = "http://127.0.0.1:" + server.boundPort();
        // Deliberately broken JSON — unmatched bracket, trailing comma, truncated.
        HttpResponse<String> response =
            postJson(baseUrl + "/api/v1/requests", "{\"scope\":\"ALL_TABLES\",");

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("\"error\":\"bad_request\"");
      } finally {
        server.stop();
        meterRegistry.close();
      }
    }
  }

  @Test
  void returns503WhenRuntimeDoesNotAcceptRequestSubmission() throws Exception {
    Path statePath = tempDir.resolve("next-http-it-unavailable");
    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(statePath)) {
      SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
      ControlPlaneQueryService queryService =
          new ControlPlaneQueryService(
              stateStore,
              () ->
                  new RuntimeStatusProvider.RuntimeStatusSnapshot(
                      "runtime",
                      "mysql",
                      "DOWN",
                      false, // request submission is NOT available
                      "runtime still coming up",
                      "sourceA",
                      null,
                      0,
                      0),
              io.github.aandreakis.dblog.controlplane.service.ControlPlaneEventStore.noop(),
              meterRegistry,
              "mysql");
      // Command service also sees the runtime as unavailable.
      ControlPlaneCommandService commandService =
          new ControlPlaneCommandService(
              stateStore,
              () ->
                  new RuntimeStatusProvider.RuntimeStatusSnapshot(
                      "runtime",
                      "mysql",
                      "DOWN",
                      false,
                      "runtime still coming up",
                      "sourceA",
                      null,
                      0,
                      0),
              (scope, submittedTableId, primaryKeyLiterals) ->
                  stateStore
                      .dumpRequests()
                      .createGenerated(scope, submittedTableId, primaryKeyLiterals));

      ControlPlaneHttpServer server =
          new ControlPlaneHttpServer(queryService, commandService, "127.0.0.1", 0);
      server.start();
      try {
        String baseUrl = "http://127.0.0.1:" + server.boundPort();
        HttpResponse<String> response =
            postJson(
                baseUrl + "/api/v1/requests",
                "{\"scope\":\"ALL_TABLES\"}");

        assertThat(response.statusCode()).isEqualTo(503);
        assertThat(response.body())
            .contains("\"error\":\"service_unavailable\"")
            .contains("runtime still coming up");
      } finally {
        server.stop();
        meterRegistry.close();
      }
    }
  }

  private static ControlPlaneQueryService defaultQueryService(
      H2RuntimeStateStore stateStore, SimpleMeterRegistry meterRegistry) {
    return new ControlPlaneQueryService(
        stateStore,
        () ->
            new RuntimeStatusProvider.RuntimeStatusSnapshot(
                "runtime", "mysql", "UP", true, "ok", "sourceA", "mysql-bin.000001:42", 0, 0),
        io.github.aandreakis.dblog.controlplane.service.ControlPlaneEventStore.noop(),
        meterRegistry,
        "mysql");
  }

  private static ControlPlaneCommandService defaultCommandService(H2RuntimeStateStore stateStore) {
    return new ControlPlaneCommandService(
        stateStore,
        () ->
            new RuntimeStatusProvider.RuntimeStatusSnapshot(
                "runtime", "mysql", "UP", true, "ok", "sourceA", "mysql-bin.000001:42", 0, 0),
        (scope, submittedTableId, primaryKeyLiterals) ->
            stateStore.dumpRequests().createGenerated(scope, submittedTableId, primaryKeyLiterals));
  }

  private static HttpResponse<String> get(String url) throws Exception {
    return request(url, "GET", null);
  }

  private static HttpResponse<String> postJson(String url, String body) throws Exception {
    return request(url, "POST", body);
  }

  private static HttpResponse<String> request(String url, String method, String body)
      throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(url))
            .header("Content-Type", "application/json")
            .method(
                method,
                body == null
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofString(body))
            .build();
    return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
  }

  private static TableSchema contractSchema(TableId tableId) {
    return TableSchema.create(
        tableId,
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
        Instant.parse("2026-04-01T00:00:00Z"));
  }

  private static TableSchema observedSchema(TableId tableId) {
    return TableSchema.create(
        tableId,
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true),
            new ColumnDefinition("legacy_note", "json", NeutralColumnType.UNSUPPORTED, false, true)),
        Instant.parse("2026-04-01T00:05:00Z"));
  }
}
