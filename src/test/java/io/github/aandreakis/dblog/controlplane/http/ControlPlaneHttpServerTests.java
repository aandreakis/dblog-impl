package io.github.aandreakis.dblog.controlplane.http;

import static org.assertj.core.api.Assertions.assertThat;

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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ControlPlaneHttpServerTests {
  @TempDir Path tempDir;

  @Test
  void exposesRuntimeRequestAndSchemaRoutesOverHttp() throws Exception {
    Path statePath = tempDir.resolve("next-http-state");
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
      meterRegistry.counter("dblog.runtime.source_capture.polls.total", "adapter", "mysql").increment(3.0d);
      ControlPlaneQueryService queryService =
          new ControlPlaneQueryService(
              stateStore,
              () ->
                  new RuntimeStatusProvider.RuntimeStatusSnapshot(
                      "runtime", "mysql", "UP", true, null),
              io.github.aandreakis.dblog.controlplane.service.ControlPlaneEventStore.noop(),
              meterRegistry,
              "mysql");
      ControlPlaneCommandService commandService =
          new ControlPlaneCommandService(
              stateStore,
              () ->
                  new RuntimeStatusProvider.RuntimeStatusSnapshot(
                      "runtime", "mysql", "UP", true, null),
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
            .contains("\"submissionAvailable\":true");

        HttpResponse<String> runtimeStatusResponse = get(baseUrl + "/api/v1/runtime/status");
        assertThat(runtimeStatusResponse.statusCode()).isEqualTo(200);
        assertThat(runtimeStatusResponse.body())
            .contains("\"mode\":\"runtime\"")
            .contains("\"adapter\":\"mysql\"")
            .contains("\"queuedCount\":1")
            .contains("\"activeCount\":1")
            .contains("\"sourceRuntime\"");

        HttpResponse<String> metricsResponse = get(baseUrl + "/api/v1/metrics");
        assertThat(metricsResponse.statusCode()).isEqualTo(200);
        assertThat(metricsResponse.body())
            .contains("\"runtimeMeasurement\"")
            .contains("\"sourceCapture\"")
            .contains("\"polls\":3.0");

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
            .contains("\"count\":1")
            .contains("\"FULL_DUMP_REQUIRED\"")
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
  void returnsExpectedStatusCodesForCommonHttpFailures() throws Exception {
    Path statePath = tempDir.resolve("next-http-errors");
    TableId tableId = new TableId("sourceA", "appdb", "customers");
    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(statePath)) {
      ControlPlaneQueryService queryService =
          new ControlPlaneQueryService(stateStore, RuntimeStatusProvider.idle());
      ControlPlaneCommandService commandService =
          new ControlPlaneCommandService(
              stateStore,
              RuntimeStatusProvider.idle(),
              (scope, submittedTableId, primaryKeyLiterals) ->
                  stateStore.dumpRequests().createGenerated(scope, submittedTableId, primaryKeyLiterals));
      ControlPlaneHttpServer server =
          new ControlPlaneHttpServer(queryService, commandService, "127.0.0.1", 0);
      server.start();
      try {
        String baseUrl = "http://127.0.0.1:" + server.boundPort();

        HttpResponse<String> methodNotAllowed = request(baseUrl + "/api/v1/runtime", "PUT", null);
        assertThat(methodNotAllowed.statusCode()).isEqualTo(405);
        assertThat(methodNotAllowed.body()).contains("\"error\":\"method_not_allowed\"");
        assertThat(methodNotAllowed.headers().firstValue("Allow")).hasValue("GET");

        HttpResponse<String> malformedJson =
            postJson(baseUrl + "/api/v1/requests", "{\"scope\":");
        assertThat(malformedJson.statusCode()).isEqualTo(400);
        assertThat(malformedJson.body()).contains("\"error\":\"bad_request\"");

        HttpResponse<String> trailingJson =
            postJson(baseUrl + "/api/v1/requests", "{\"scope\":\"ALL_TABLES\"} trailing");
        assertThat(trailingJson.statusCode()).isEqualTo(400);
        assertThat(trailingJson.body()).contains("\"error\":\"bad_request\"");

        HttpResponse<String> serviceUnavailable =
            postJson(baseUrl + "/api/v1/requests", "{\"scope\":\"ALL_TABLES\"}");
        assertThat(serviceUnavailable.statusCode()).isEqualTo(503);
        assertThat(serviceUnavailable.body()).contains("\"error\":\"service_unavailable\"");
      } finally {
        server.stop();
      }
    }
  }

  @Test
  void rejectsOversizedRequestBodies() throws Exception {
    Path statePath = tempDir.resolve("next-http-too-large");
    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(statePath)) {
      ControlPlaneQueryService queryService =
          new ControlPlaneQueryService(stateStore, RuntimeStatusProvider.idle());
      ControlPlaneCommandService commandService =
          new ControlPlaneCommandService(
              stateStore,
              RuntimeStatusProvider.idle(),
              (scope, submittedTableId, primaryKeyLiterals) ->
                  stateStore.dumpRequests().createGenerated(scope, submittedTableId, primaryKeyLiterals));
      ControlPlaneHttpServer server =
          new ControlPlaneHttpServer(queryService, commandService, "127.0.0.1", 0, false, 2, 4, 64);
      server.start();
      try {
        String baseUrl = "http://127.0.0.1:" + server.boundPort();
        String oversizedBody =
            "{\"scope\":\"PRIMARY_KEYS\",\"primaryKeyLiterals\":[\""
                + "x".repeat(128)
                + "\"]}";

        HttpResponse<String> tooLarge = postJson(baseUrl + "/api/v1/requests", oversizedBody);
        assertThat(tooLarge.statusCode()).isEqualTo(413);
        assertThat(tooLarge.body()).contains("\"error\":\"request_too_large\"");
      } finally {
        server.stop();
      }
    }
  }

  @Test
  void rejectsMalformedPrimaryKeyLiteralShapesInsteadOfSilentlyTreatingThemAsMissing()
      throws Exception {
    Path statePath = tempDir.resolve("next-http-primary-key-shape");
    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(statePath)) {
      ControlPlaneQueryService queryService =
          new ControlPlaneQueryService(
              stateStore,
              () ->
                  new RuntimeStatusProvider.RuntimeStatusSnapshot(
                      "runtime", "mysql", "UP", true, "ok", "sourceA", "mysql-bin.000001:42", 0, 1));
      ControlPlaneCommandService commandService =
          new ControlPlaneCommandService(
              stateStore,
              () ->
                  new RuntimeStatusProvider.RuntimeStatusSnapshot(
                      "runtime", "mysql", "UP", true, "ok", "sourceA", "mysql-bin.000001:42", 0, 1),
              (scope, submittedTableId, primaryKeyLiterals) ->
                  stateStore.dumpRequests().createGenerated(scope, submittedTableId, primaryKeyLiterals));
      ControlPlaneHttpServer server =
          new ControlPlaneHttpServer(queryService, commandService, "127.0.0.1", 0);
      server.start();
      try {
        String baseUrl = "http://127.0.0.1:" + server.boundPort();

        HttpResponse<String> malformedPrimaryKeys =
            postJson(
                baseUrl + "/api/v1/requests",
                """
                {"scope":"TABLE","table":{"databaseName":"sourceA","schemaName":"appdb","tableName":"customers"},"primaryKeyLiterals":"42"}
                """);

        assertThat(malformedPrimaryKeys.statusCode()).isEqualTo(400);
        assertThat(malformedPrimaryKeys.body())
            .contains("\"error\":\"bad_request\"")
            .contains("primaryKeyLiterals must be a JSON array");
      } finally {
        server.stop();
      }
    }
  }

  @Test
  void rejectsUnknownRequestStateFilters() throws Exception {
    Path statePath = tempDir.resolve("next-http-invalid-state-filter");
    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(statePath)) {
      ControlPlaneQueryService queryService =
          new ControlPlaneQueryService(
              stateStore,
              () ->
                  new RuntimeStatusProvider.RuntimeStatusSnapshot(
                      "runtime", "mysql", "UP", true, "ok", "sourceA", "mysql-bin.000001:42", 0, 1));
      ControlPlaneCommandService commandService =
          new ControlPlaneCommandService(
              stateStore,
              () ->
                  new RuntimeStatusProvider.RuntimeStatusSnapshot(
                      "runtime", "mysql", "UP", true, "ok", "sourceA", "mysql-bin.000001:42", 0, 1),
              (scope, submittedTableId, primaryKeyLiterals) ->
                  stateStore.dumpRequests().createGenerated(scope, submittedTableId, primaryKeyLiterals));
      ControlPlaneHttpServer server =
          new ControlPlaneHttpServer(queryService, commandService, "127.0.0.1", 0);
      server.start();
      try {
        String baseUrl = "http://127.0.0.1:" + server.boundPort();

        HttpResponse<String> invalidState =
            get(baseUrl + "/api/v1/requests?state=RUNNING_FOREVER");

        assertThat(invalidState.statusCode()).isEqualTo(400);
        assertThat(invalidState.body())
            .contains("\"error\":\"bad_request\"")
            .contains("state must be one of QUEUED, ACTIVE, COMPLETED, FAILED");
      } finally {
        server.stop();
      }
    }
  }

  @Test
  void rejectsPrimaryKeyLiteralsThatFailSchemaAwareCanonicalization() throws Exception {
    Path statePath = tempDir.resolve("next-http-invalid-primary-key-literal");
    TableId tableId = new TableId("sourceA", "appdb", "customers");
    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(statePath)) {
      stateStore.schemas().saveContractSchema(contractSchema(tableId));
      ControlPlaneQueryService queryService =
          new ControlPlaneQueryService(
              stateStore,
              () ->
                  new RuntimeStatusProvider.RuntimeStatusSnapshot(
                      "runtime", "mysql", "UP", true, "ok", "sourceA", "mysql-bin.000001:42", 0, 1));
      ControlPlaneCommandService commandService =
          new ControlPlaneCommandService(
              stateStore,
              () ->
                  new RuntimeStatusProvider.RuntimeStatusSnapshot(
                      "runtime", "mysql", "UP", true, "ok", "sourceA", "mysql-bin.000001:42", 0, 1),
              (scope, submittedTableId, primaryKeyLiterals) ->
                  stateStore.dumpRequests().createGenerated(scope, submittedTableId, primaryKeyLiterals));
      ControlPlaneHttpServer server =
          new ControlPlaneHttpServer(queryService, commandService, "127.0.0.1", 0);
      server.start();
      try {
        String baseUrl = "http://127.0.0.1:" + server.boundPort();

        HttpResponse<String> invalidLiteral =
            postJson(
                baseUrl + "/api/v1/requests",
                """
                {"scope":"PRIMARY_KEYS","table":{"databaseName":"sourceA","schemaName":"appdb","tableName":"customers"},"primaryKeyLiterals":["abc"]}
                """);

        assertThat(invalidLiteral.statusCode()).isEqualTo(400);
        assertThat(invalidLiteral.body())
            .contains("\"error\":\"bad_request\"")
            .contains("invalid integer primary key literal");
      } finally {
        server.stop();
      }
    }
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
