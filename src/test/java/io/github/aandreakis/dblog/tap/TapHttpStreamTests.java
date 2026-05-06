package io.github.aandreakis.dblog.tap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.aandreakis.dblog.controlplane.http.ControlPlaneHttpServer;
import io.github.aandreakis.dblog.controlplane.service.ControlPlaneCommandService;
import io.github.aandreakis.dblog.controlplane.service.ControlPlaneQueryService;
import io.github.aandreakis.dblog.controlplane.service.RuntimeStatusProvider;
import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.model.WatermarkToken;
import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.request.DumpRequestState;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import io.github.aandreakis.dblog.support.NextTestFixtures;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TapHttpStreamTests {
  @TempDir Path tempDir;

  @Test
  void streamsPumpEventsAsNdjson() throws Exception {
    Path statePath = tempDir.resolve("tap-stream-state");
    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(statePath)) {
      ControlPlaneQueryService queryService =
          new ControlPlaneQueryService(stateStore, RuntimeStatusProvider.idle());
      ControlPlaneCommandService commandService =
          new ControlPlaneCommandService(
              stateStore,
              RuntimeStatusProvider.idle(),
              (scope, submittedTableId, primaryKeyLiterals) ->
                  stateStore
                      .dumpRequests()
                      .createGenerated(scope, submittedTableId, primaryKeyLiterals));
      ControlPlaneHttpServer server =
          new ControlPlaneHttpServer(queryService, commandService, "127.0.0.1", 0);
      TapConfig tapConfig = new TapConfig();
      tapConfig.setEnabled(true);
      tapConfig.setQueueCapacity(256);
      tapConfig.setStandbyThresholdMs(500);
      ActiveTap tap = new ActiveTap(tapConfig, "run-abc", "source-x");
      server.attachTap(new TapHttpHandler(tap));
      server.start();
      String baseUrl = "http://127.0.0.1:" + server.boundPort();
      List<String> receivedLines = new CopyOnWriteArrayList<>();
      CompletableFuture<Void> readerDone =
          CompletableFuture.runAsync(
              () -> readChunkedNdjson(baseUrl + "/api/v1/tap/stream", receivedLines));
      try {
        TableId table = new TableId("db", "public", "orders");
        ChangeEvent cdcEvent =
            logEvent(OperationType.INSERT, table, "2044", "binlog.000042:12345");

        tap.onSinkBatchStart();
        tap.onCdcBatch(List.of(cdcEvent));
        tap.onSinkEvent(cdcEvent, "ndjson");
        tap.onWatermarkWritten(
            Tap.WatermarkLevel.LOW,
            new WatermarkToken("a71c-0001-0000-0000-000000000001"));
        tap.onWatermarkReceived(
            Tap.WatermarkLevel.LOW,
            new WatermarkToken("a71c-0001-0000-0000-000000000001"),
            new OpaqueSourcePosition("binlog.000042:12350"));
        tap.onCheckpointAdvanced(
            new OpaqueSourcePosition("binlog.000042:12360"), 0, "batched-threshold");
        tap.onSinkBatchCommit();

        await()
            .atMost(Duration.ofSeconds(5))
            .until(
                () ->
                    receivedLines.stream().anyMatch(line -> line.contains("\"kind\":\"cdc\""))
                        && receivedLines.stream()
                            .anyMatch(line -> line.contains("\"kind\":\"watermark.written\""))
                        && receivedLines.stream()
                            .anyMatch(line -> line.contains("\"kind\":\"sink.event\""))
                        && receivedLines.stream()
                            .anyMatch(line -> line.contains("\"kind\":\"checkpoint.advanced\"")));

        String cdcLine =
            receivedLines.stream()
                .filter(line -> line.contains("\"kind\":\"cdc\""))
                .findFirst()
                .orElseThrow();
        assertThat(cdcLine).contains("\"v\":1");
        assertThat(cdcLine).contains("\"run_id\":\"run-abc\"");
        assertThat(cdcLine).contains("\"source_id\":\"source-x\"");
        assertThat(cdcLine).contains("\"lsn\":\"binlog.000042:12345\"");
        assertThat(cdcLine).contains("\"op\":\"INSERT\"");
        assertThat(cdcLine).contains("\"table\":\"db.public.orders\"");
        assertThat(cdcLine).contains("\"pk\":\"2044\"");

        String sinkLine =
            receivedLines.stream()
                .filter(line -> line.contains("\"kind\":\"sink.event\""))
                .findFirst()
                .orElseThrow();
        assertThat(sinkLine).contains("\"sink_name\":\"ndjson\"");
        assertThat(sinkLine).contains("\"origin\":\"LOG\"");
      } finally {
        server.stop();
      }
      readerDone.orTimeout(5, java.util.concurrent.TimeUnit.SECONDS).join();
    }
  }

  @Test
  void returns503WhenHandlerIsAttachedToNoopTap() throws Exception {
    // Mirrors the Spring-bean dispatch: when dblog.tap.enabled=false, the @Bean returns NoopTap
    // and the handler is still attached. Requests must then surface the "tap not enabled" 503
    // rather than spin up a stream.
    Path statePath = tempDir.resolve("tap-stream-disabled");
    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(statePath)) {
      ControlPlaneQueryService queryService =
          new ControlPlaneQueryService(stateStore, RuntimeStatusProvider.idle());
      ControlPlaneCommandService commandService =
          new ControlPlaneCommandService(
              stateStore,
              RuntimeStatusProvider.idle(),
              (scope, submittedTableId, primaryKeyLiterals) ->
                  stateStore
                      .dumpRequests()
                      .createGenerated(scope, submittedTableId, primaryKeyLiterals));
      ControlPlaneHttpServer server =
          new ControlPlaneHttpServer(queryService, commandService, "127.0.0.1", 0);
      server.attachTap(new TapHttpHandler(NoopTap.INSTANCE));
      server.start();
      try {
        HttpResponse<String> response =
            HttpClient.newHttpClient()
                .send(
                    HttpRequest.newBuilder(
                            URI.create(
                                "http://127.0.0.1:" + server.boundPort() + "/api/v1/tap/stream"))
                        .GET()
                        .build(),
                    HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(503);
        assertThat(response.body()).contains("tap_not_enabled");
      } finally {
        server.stop();
      }
    }
  }

  @Test
  void returns503WhenTapIsNotAttached() throws Exception {
    Path statePath = tempDir.resolve("tap-stream-no-tap");
    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(statePath)) {
      ControlPlaneQueryService queryService =
          new ControlPlaneQueryService(stateStore, RuntimeStatusProvider.idle());
      ControlPlaneCommandService commandService =
          new ControlPlaneCommandService(
              stateStore,
              RuntimeStatusProvider.idle(),
              (scope, submittedTableId, primaryKeyLiterals) ->
                  stateStore
                      .dumpRequests()
                      .createGenerated(scope, submittedTableId, primaryKeyLiterals));
      ControlPlaneHttpServer server =
          new ControlPlaneHttpServer(queryService, commandService, "127.0.0.1", 0);
      server.start();
      try {
        HttpResponse<String> response =
            HttpClient.newHttpClient()
                .send(
                    HttpRequest.newBuilder(
                            URI.create(
                                "http://127.0.0.1:" + server.boundPort() + "/api/v1/tap/stream"))
                        .GET()
                        .build(),
                    HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(503);
        assertThat(response.body()).contains("tap_not_enabled");
      } finally {
        server.stop();
      }
    }
  }

  @Test
  void secondSubscriberDisplacesTheFirst() throws Exception {
    Path statePath = tempDir.resolve("tap-multi-subscriber");
    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(statePath)) {
      ControlPlaneQueryService queryService =
          new ControlPlaneQueryService(stateStore, RuntimeStatusProvider.idle());
      ControlPlaneCommandService commandService =
          new ControlPlaneCommandService(
              stateStore,
              RuntimeStatusProvider.idle(),
              (scope, submittedTableId, primaryKeyLiterals) ->
                  stateStore
                      .dumpRequests()
                      .createGenerated(scope, submittedTableId, primaryKeyLiterals));
      ControlPlaneHttpServer server =
          new ControlPlaneHttpServer(queryService, commandService, "127.0.0.1", 0);
      TapConfig tapConfig = new TapConfig();
      tapConfig.setEnabled(true);
      tapConfig.setQueueCapacity(128);
      ActiveTap tap = new ActiveTap(tapConfig, "run-multi", "source-m");
      server.attachTap(new TapHttpHandler(tap));
      server.start();
      String baseUrl = "http://127.0.0.1:" + server.boundPort();
      TableId table = new TableId("db", "public", "orders");

      List<String> firstLines = new CopyOnWriteArrayList<>();
      CompletableFuture<Void> firstReader =
          CompletableFuture.runAsync(
              () -> readChunkedNdjson(baseUrl + "/api/v1/tap/stream", firstLines));
      List<String> secondLines = new CopyOnWriteArrayList<>();
      CompletableFuture<Void> secondReader = null;
      try {
        // Push a CDC envelope so the first subscriber definitely has its HTTP response active
        // before the second subscriber races to displace it.
        publishCdc(tap, table, "pk-first", "binlog.000042:10");
        await()
            .atMost(Duration.ofSeconds(5))
            .until(() -> firstLines.stream().anyMatch(line -> line.contains("pk-first")));

        secondReader =
            CompletableFuture.runAsync(
                () -> readChunkedNdjson(baseUrl + "/api/v1/tap/stream", secondLines));

        // The second subscriber's arrival displaces the first; the first's reader unwinds when
        // the handler loop exits and the socket closes.
        firstReader.orTimeout(3, java.util.concurrent.TimeUnit.SECONDS).join();

        publishCdc(tap, table, "pk-second", "binlog.000042:20");
        await()
            .atMost(Duration.ofSeconds(5))
            .until(() -> secondLines.stream().anyMatch(line -> line.contains("pk-second")));
      } finally {
        server.stop();
      }
      if (secondReader != null) {
        secondReader.orTimeout(5, java.util.concurrent.TimeUnit.SECONDS).join();
      }
    }
  }

  private static void publishCdc(ActiveTap tap, TableId table, String pk, String lsn) {
    ChangeEvent event = logEvent(OperationType.INSERT, table, pk, lsn);
    tap.onSinkBatchStart();
    tap.onCdcBatch(List.of(event));
    tap.onSinkEvent(event, "ndjson");
    tap.onSinkBatchCommit();
  }

  @Test
  void onRequestTransitionEmitsRequestTransitionEnvelope() throws Exception {
    Path statePath = tempDir.resolve("tap-request-transition");
    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(statePath)) {
      ControlPlaneQueryService queryService =
          new ControlPlaneQueryService(stateStore, RuntimeStatusProvider.idle());
      ControlPlaneCommandService commandService =
          new ControlPlaneCommandService(
              stateStore,
              RuntimeStatusProvider.idle(),
              (scope, submittedTableId, primaryKeyLiterals) ->
                  stateStore
                      .dumpRequests()
                      .createGenerated(scope, submittedTableId, primaryKeyLiterals));
      ControlPlaneHttpServer server =
          new ControlPlaneHttpServer(queryService, commandService, "127.0.0.1", 0);
      TapConfig tapConfig = new TapConfig();
      tapConfig.setEnabled(true);
      tapConfig.setQueueCapacity(128);
      ActiveTap tap = new ActiveTap(tapConfig, "run-rt", "source-rt");
      server.attachTap(new TapHttpHandler(tap));
      server.start();
      String baseUrl = "http://127.0.0.1:" + server.boundPort();
      List<String> receivedLines = new CopyOnWriteArrayList<>();
      CompletableFuture<Void> readerDone =
          CompletableFuture.runAsync(
              () -> readChunkedNdjson(baseUrl + "/api/v1/tap/stream", receivedLines));
      try {
        TableId table = new TableId("db", "public", "orders");
        // First-time transition: no previous state, no reason.
        tap.onRequestTransition("req-1", DumpScope.TABLE, table, null, DumpRequestState.ACTIVE, null);
        // Failure transition carries a reason.
        tap.onRequestTransition(
            "req-2",
            DumpScope.PRIMARY_KEYS,
            table,
            DumpRequestState.ACTIVE,
            DumpRequestState.FAILED,
            "schema drift");
        // ALL_TABLES: table is null.
        tap.onRequestTransition(
            "req-3",
            DumpScope.ALL_TABLES,
            null,
            DumpRequestState.ACTIVE,
            DumpRequestState.COMPLETED,
            null);

        await()
            .atMost(Duration.ofSeconds(5))
            .until(
                () ->
                    receivedLines.stream().anyMatch(line -> line.contains("\"request_id\":\"req-1\""))
                        && receivedLines.stream()
                            .anyMatch(line -> line.contains("\"request_id\":\"req-2\""))
                        && receivedLines.stream()
                            .anyMatch(line -> line.contains("\"request_id\":\"req-3\"")));

        String first =
            receivedLines.stream()
                .filter(line -> line.contains("\"request_id\":\"req-1\""))
                .findFirst()
                .orElseThrow();
        assertThat(first).contains("\"kind\":\"request.transition\"");
        assertThat(first).contains("\"scope\":\"TABLE\"");
        assertThat(first).contains("\"state\":\"ACTIVE\"");
        assertThat(first).contains("\"table\":\"db.public.orders\"");
        // First transition: no prev_state field.
        assertThat(first).doesNotContain("\"prev_state\"");
        assertThat(first).doesNotContain("\"reason\"");

        String failed =
            receivedLines.stream()
                .filter(line -> line.contains("\"request_id\":\"req-2\""))
                .findFirst()
                .orElseThrow();
        assertThat(failed).contains("\"scope\":\"PRIMARY_KEYS\"");
        assertThat(failed).contains("\"state\":\"FAILED\"");
        assertThat(failed).contains("\"prev_state\":\"ACTIVE\"");
        assertThat(failed).contains("\"reason\":\"schema drift\"");

        String allTables =
            receivedLines.stream()
                .filter(line -> line.contains("\"request_id\":\"req-3\""))
                .findFirst()
                .orElseThrow();
        assertThat(allTables).contains("\"scope\":\"ALL_TABLES\"");
        assertThat(allTables).contains("\"state\":\"COMPLETED\"");
        assertThat(allTables).doesNotContain("\"table\"");
      } finally {
        server.stop();
      }
      readerDone.orTimeout(5, java.util.concurrent.TimeUnit.SECONDS).join();
    }
  }

  @Test
  void onRequestTransitionFlushesPendingBufferBeforeWriting() throws Exception {
    // Regression test for the seq-ordering bug: onRequestTransition used to writeEnvelopeDirect
    // without flushing the per-work-unit buffer first, so a transition fired mid-batch would
    // leapfrog its pending predecessors on the wire. Fixed by mirroring onError's
    // flushPendingBuffer()-then-writeEnvelopeDirect ordering.
    TapConfig tapConfig = new TapConfig();
    tapConfig.setEnabled(true);
    tapConfig.setQueueCapacity(64);
    ActiveTap tap = new ActiveTap(tapConfig, "run-seq", "source-seq");
    TableId table = new TableId("db", "public", "orders");
    ChangeEvent first = logEvent(OperationType.INSERT, table, "1", "binlog.000042:10");
    ChangeEvent second = logEvent(OperationType.UPDATE, table, "2", "binlog.000042:20");

    tap.onSinkBatchStart();
    tap.onCdcBatch(List.of(first, second));
    tap.onSinkEvent(first, "ndjson");
    tap.onSinkEvent(second, "ndjson");
    // Transition fires mid-batch — must not jump ahead of the buffered events.
    tap.onRequestTransition(
        "req-X",
        DumpScope.TABLE,
        table,
        DumpRequestState.ACTIVE,
        DumpRequestState.FAILED,
        "schema drift during batch");
    tap.onSinkBatchCommit();

    List<String> wireLines = drainQueue(tap, 10);
    List<Long> seqs = wireLines.stream().map(TapHttpStreamTests::extractSeq).toList();

    // Seq is monotonically increasing on the wire — no leapfrog.
    for (int index = 1; index < seqs.size(); index++) {
      assertThat(seqs.get(index)).isGreaterThan(seqs.get(index - 1));
    }

    // Transition sits AFTER the buffered events, not before.
    int transitionIndex = -1;
    for (int index = 0; index < wireLines.size(); index++) {
      if (wireLines.get(index).contains("\"kind\":\"request.transition\"")) {
        transitionIndex = index;
        break;
      }
    }
    assertThat(transitionIndex).isGreaterThanOrEqualTo(2);
    // Every line before the transition is a buffered cdc / sink.event, never a direct-put kind.
    for (int index = 0; index < transitionIndex; index++) {
      String line = wireLines.get(index);
      assertThat(line)
          .satisfiesAnyOf(
              l -> assertThat(l.toString()).contains("\"kind\":\"cdc\""),
              l -> assertThat(l.toString()).contains("\"kind\":\"sink.event\""));
    }
  }

  @Test
  void primaryKeyLiteralCanonicalisesBinaryAndDecimal() throws Exception {
    // Regression test for the pk-literal bug: onCdcBatch / onSinkEvent used String.valueOf on the
    // raw PK value, which printed byte[] as "[B@…" and BigDecimal with trailing zeros intact.
    // Spec §3.8 requires lowercase hex and trailing-zero-stripped plain-string form respectively.
    TapConfig tapConfig = new TapConfig();
    tapConfig.setEnabled(true);
    tapConfig.setQueueCapacity(64);
    ActiveTap tap = new ActiveTap(tapConfig, "run-pk", "source-pk");
    TableId table = new TableId("db", "public", "orders");

    byte[] binaryPk = {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF};
    ChangeEvent binaryEvent = customPkEvent(table, "id", binaryPk, "binlog.000042:100");

    BigDecimal decimalPk = new BigDecimal("10.5000");
    ChangeEvent decimalEvent = customPkEvent(table, "id", decimalPk, "binlog.000042:200");

    BigDecimal zeroPk = new BigDecimal("0.000");
    ChangeEvent zeroEvent = customPkEvent(table, "id", zeroPk, "binlog.000042:300");

    tap.onSinkBatchStart();
    tap.onCdcBatch(List.of(binaryEvent, decimalEvent, zeroEvent));
    tap.onSinkEvent(binaryEvent, "ndjson");
    tap.onSinkEvent(decimalEvent, "ndjson");
    tap.onSinkEvent(zeroEvent, "ndjson");
    tap.onSinkBatchCommit();

    List<String> wireLines = drainQueue(tap, 12);
    assertThat(wireLines).anyMatch(line -> line.contains("\"pk\":\"deadbeef\""));
    assertThat(wireLines).anyMatch(line -> line.contains("\"pk\":\"10.5\""));
    assertThat(wireLines).anyMatch(line -> line.contains("\"pk\":\"0\""));
    // Sanity: the canonical form never leaks Java's raw toString for either type.
    assertThat(wireLines).noneMatch(line -> line.contains("[B@"));
    assertThat(wireLines).noneMatch(line -> line.contains("\"pk\":\"10.5000\""));
  }

  private static List<String> drainQueue(ActiveTap tap, int maxLines) throws InterruptedException {
    List<String> collected = new ArrayList<>();
    for (int index = 0; index < maxLines; index++) {
      byte[] line = tap.queue().poll(500L, TimeUnit.MILLISECONDS);
      if (line == null) {
        break;
      }
      // Strip the trailing newline to keep the assertions tight.
      int length = line.length;
      while (length > 0 && line[length - 1] == '\n') {
        length--;
      }
      collected.add(new String(line, 0, length, StandardCharsets.UTF_8));
    }
    return collected;
  }

  private static long extractSeq(String jsonLine) {
    int start = jsonLine.indexOf("\"seq\":") + "\"seq\":".length();
    int end = start;
    while (end < jsonLine.length()
        && (Character.isDigit(jsonLine.charAt(end)) || jsonLine.charAt(end) == '-')) {
      end++;
    }
    return Long.parseLong(jsonLine.substring(start, end));
  }

  private static ChangeEvent customPkEvent(TableId tableId, String column, Object pk, String lsn) {
    SourcePosition position = new OpaqueSourcePosition(lsn);
    LinkedHashMap<String, Object> pkMap = new LinkedHashMap<>();
    pkMap.put(column, pk);
    ImmutableRowImage pkImage = ImmutableRowImage.of(pkMap);
    return new ChangeEvent(
        tableId,
        OperationType.INSERT,
        CaptureOrigin.LOG,
        pkImage,
        null,
        pkImage,
        position,
        "tx-pk",
        null);
  }

  @Test
  void standbyAndResumedPayloadShapesMatchSpec() {
    // The handler thread is responsible for writing stream.standby / stream.resumed directly
    // to the HTTP response. Full integration coverage is fragile because producer blocking and
    // socket send-buffer backpressure race each other. Instead, exercise the envelope builders
    // directly — they encode the spec's §3.4 out-of-band shape.
    TapConfig tapConfig = new TapConfig();
    tapConfig.setEnabled(true);
    tapConfig.setQueueCapacity(32);
    tapConfig.setStandbyThresholdMs(100);
    ActiveTap tap = new ActiveTap(tapConfig, "run-envelope", "source-env");

    String standby = new String(tap.buildStandbyPayload(1_250L), StandardCharsets.UTF_8);
    assertThat(standby).contains("\"kind\":\"stream.standby\"");
    assertThat(standby).contains("\"seq\":null");
    assertThat(standby).contains("\"queue_full_ms\":1250");
    assertThat(standby).contains("\"queue_capacity\":32");
    assertThat(standby).contains("\"reason\":\"queue_full\"");
    assertThat(standby).contains("DBLog is on standby");
    assertThat(standby).contains("\"run_id\":\"run-envelope\"");
    assertThat(standby).contains("\"source_id\":\"source-env\"");

    String resumed = new String(tap.buildResumedPayload(1_400L), StandardCharsets.UTF_8);
    assertThat(resumed).contains("\"kind\":\"stream.resumed\"");
    assertThat(resumed).contains("\"seq\":null");
    assertThat(resumed).contains("\"standby_total_ms\":1400");
    assertThat(resumed).contains("\"run_id\":\"run-envelope\"");
    assertThat(resumed).contains("\"source_id\":\"source-env\"");
  }

  @Test
  void onChunkSelectedEmitsRangeChunkEnvelope() throws Exception {
    TapConfig tapConfig = new TapConfig();
    tapConfig.setEnabled(true);
    tapConfig.setQueueCapacity(64);
    ActiveTap tap = new ActiveTap(tapConfig, "run-cs", "source-cs");
    TableSchema schema = NextTestFixtures.schema();

    // Populated chunk — pk_min, pk_max, start_after_pk all present on the wire.
    tap.onSinkBatchStart();
    tap.onWatermarkWritten(Tap.WatermarkLevel.LOW, new WatermarkToken("lw-cs-1"));
    tap.onChunkSelected(
        "req-cs",
        Chunk.fromMapRows(
            "dump-cs-1",
            schema.tableId().displayName(),
            schema,
            "0",
            List.of(NextTestFixtures.row("1", "alice"), NextTestFixtures.row("2", "bob")),
            "2",
            false));
    tap.onSinkBatchCommit();

    List<String> wire = drainQueue(tap, 4);
    String selected =
        wire.stream()
            .filter(line -> line.contains("\"kind\":\"chunk.selected\""))
            .findFirst()
            .orElseThrow();
    assertThat(selected).contains("\"chunk_id\":1");
    assertThat(selected).contains("\"request_id\":\"req-cs\"");
    assertThat(selected).contains("\"dump_id\":\"dump-cs-1\"");
    assertThat(selected).contains("\"table\":\"app.public.users\"");
    assertThat(selected).contains("\"mode\":\"range\"");
    assertThat(selected).contains("\"pk_min\":\"1\"");
    assertThat(selected).contains("\"pk_max\":\"2\"");
    assertThat(selected).contains("\"start_after_pk\":\"0\"");
    assertThat(selected).contains("\"row_count\":2");
    assertThat(selected).contains("\"final_chunk\":false");
    assertThat(selected).contains("\"fingerprint\":\"");

    // Empty chunk — pk_min / pk_max / start_after_pk absent under Jackson NON_NULL.
    tap.onSinkBatchStart();
    tap.onWatermarkWritten(Tap.WatermarkLevel.LOW, new WatermarkToken("lw-cs-2"));
    tap.onChunkSelected(
        "req-cs-2",
        Chunk.fromMapRows(
            "dump-cs-2",
            schema.tableId().displayName(),
            schema,
            (String) null,
            List.of(),
            (String) null,
            true));
    tap.onSinkBatchCommit();

    List<String> wire2 = drainQueue(tap, 4);
    String emptySelected =
        wire2.stream()
            .filter(line -> line.contains("\"kind\":\"chunk.selected\""))
            .findFirst()
            .orElseThrow();
    assertThat(emptySelected).doesNotContain("\"pk_min\"");
    assertThat(emptySelected).doesNotContain("\"pk_max\"");
    assertThat(emptySelected).doesNotContain("\"start_after_pk\"");
    assertThat(emptySelected).contains("\"row_count\":0");
    assertThat(emptySelected).contains("\"final_chunk\":true");
  }

  @Test
  void onChunkCompletedEmitsWithTimingAndCounts() throws Exception {
    TapConfig tapConfig = new TapConfig();
    tapConfig.setEnabled(true);
    tapConfig.setQueueCapacity(64);
    ActiveTap tap = new ActiveTap(tapConfig, "run-cc", "source-cc");
    TableSchema schema = NextTestFixtures.schema();
    TableId tableId = schema.tableId();

    Chunk chunk =
        Chunk.fromMapRows(
            "dump-cc",
            tableId.displayName(),
            schema,
            (String) null,
            List.of(NextTestFixtures.row("10", "x"), NextTestFixtures.row("11", "y")),
            "11",
            true);

    tap.onSinkBatchStart();
    tap.onWatermarkWritten(Tap.WatermarkLevel.LOW, new WatermarkToken("lw-cc"));
    tap.onChunkSelected("req-cc", chunk);
    tap.onWatermarkWritten(Tap.WatermarkLevel.HIGH, new WatermarkToken("hw-cc"));
    // One of the two chunk rows survived reconciliation as a SELECT refresh row; the other
    // would have been dropped by an in-window CDC event, so excluded == 1.
    List<ChangeEvent> emitted =
        List.of(
            new ChangeEvent(
                tableId,
                OperationType.INSERT,
                CaptureOrigin.SELECT,
                ImmutableRowImage.of(Map.of("id", "10")),
                null,
                ImmutableRowImage.of(Map.of("id", "10", "name", "x")),
                new OpaqueSourcePosition("wm:hw-cc"),
                null,
                "dump-cc"));
    tap.onChunkCompleted("req-cc", chunk, emitted, new OpaqueSourcePosition("wm:hw-cc"));
    tap.onSinkBatchCommit();

    List<String> wire = drainQueue(tap, 6);
    String completed =
        wire.stream()
            .filter(line -> line.contains("\"kind\":\"chunk.completed\""))
            .findFirst()
            .orElseThrow();
    assertThat(completed).contains("\"chunk_id\":1");
    assertThat(completed).contains("\"request_id\":\"req-cc\"");
    assertThat(completed).contains("\"table\":\"app.public.users\"");
    assertThat(completed).contains("\"emitted\":1");
    assertThat(completed).contains("\"excluded\":1");
    assertThat(completed).contains("\"last_pk\":\"11\"");
    assertThat(completed).contains("\"lsn\":\"wm:hw-cc\"");
    assertThat(completed).containsPattern("\"duration_ms\":\\d+");
    assertThat(completed).contains("\"final_chunk\":true");
  }

  @Test
  void onChunkCollisionEmitsWithCauseMetadata() throws Exception {
    TapConfig tapConfig = new TapConfig();
    tapConfig.setEnabled(true);
    tapConfig.setQueueCapacity(64);
    ActiveTap tap = new ActiveTap(tapConfig, "run-xc", "source-xc");
    TableSchema schema = NextTestFixtures.schema();
    TableId tableId = schema.tableId();

    tap.onSinkBatchStart();
    tap.onWatermarkWritten(Tap.WatermarkLevel.LOW, new WatermarkToken("lw-xc"));

    // Schema present → excluded_pk emitted; cause has tx id → cause_tx_id emitted.
    tap.onChunkCollision(
        schema,
        ImmutableRowImage.of(Map.of("id", "42", "name", "stale")),
        NextTestFixtures.logUpdate(tableId, "42", "fresh", 55));

    // Schema null → excluded_pk absent; cause without tx id → cause_tx_id absent.
    tap.onChunkCollision(
        null,
        ImmutableRowImage.of(Map.of("id", "43", "name", "old")),
        new ChangeEvent(
            tableId,
            OperationType.DELETE,
            CaptureOrigin.LOG,
            ImmutableRowImage.of(Map.of("id", "43")),
            ImmutableRowImage.of(Map.of("id", "43", "name", "old")),
            null,
            new OpaqueSourcePosition("lsn:99"),
            null,
            null));
    tap.onSinkBatchCommit();

    List<String> collisions =
        drainQueue(tap, 4).stream()
            .filter(line -> line.contains("\"kind\":\"chunk.collision\""))
            .toList();
    assertThat(collisions).hasSize(2);

    String first = collisions.get(0);
    assertThat(first).contains("\"chunk_id\":1");
    assertThat(first).contains("\"excluded_pk\":\"42\"");
    assertThat(first).contains("\"cause_lsn\":\"lsn:55\"");
    assertThat(first).contains("\"cause_op\":\"UPDATE\"");
    assertThat(first).contains("\"cause_tx_id\":\"tx-55\"");

    String second = collisions.get(1);
    assertThat(second).contains("\"cause_op\":\"DELETE\"");
    assertThat(second).contains("\"cause_lsn\":\"lsn:99\"");
    assertThat(second).doesNotContain("\"excluded_pk\"");
    assertThat(second).doesNotContain("\"cause_tx_id\"");
  }

  @Test
  void onErrorEmitsErrorEnvelopeWithContext() throws Exception {
    TapConfig tapConfig = new TapConfig();
    tapConfig.setEnabled(true);
    tapConfig.setQueueCapacity(64);
    ActiveTap tap = new ActiveTap(tapConfig, "run-err", "source-err");

    // Populated context — serialised as a nested JSON object.
    LinkedHashMap<String, Object> context = new LinkedHashMap<>();
    context.put("stage", "pump");
    context.put("lsn", "binlog:42");
    tap.onError("WatermarkSequenceException", "LOW arrived after HIGH", context);

    // Empty context — collapsed to null via ActiveTap's guard, then omitted by NON_NULL.
    tap.onError("NullPointerException", "nothing", Map.of());

    // Null exception class / message / context — ActiveTap substitutes defaults.
    tap.onError(null, null, null);

    List<String> wire = drainQueue(tap, 4);
    assertThat(wire).hasSize(3);

    String populated = wire.get(0);
    assertThat(populated).contains("\"kind\":\"error\"");
    assertThat(populated).contains("\"class\":\"WatermarkSequenceException\"");
    assertThat(populated).contains("\"message\":\"LOW arrived after HIGH\"");
    assertThat(populated).contains("\"context\":{");
    assertThat(populated).contains("\"stage\":\"pump\"");
    assertThat(populated).contains("\"lsn\":\"binlog:42\"");

    String emptyCtx = wire.get(1);
    assertThat(emptyCtx).contains("\"class\":\"NullPointerException\"");
    assertThat(emptyCtx).doesNotContain("\"context\"");

    String defaulted = wire.get(2);
    assertThat(defaulted).contains("\"class\":\"UnknownException\"");
    assertThat(defaulted).contains("\"message\":\"\"");
    assertThat(defaulted).doesNotContain("\"context\"");
  }

  @Test
  void heartbeatEmittedAfterIntervalElapses() throws Exception {
    TapConfig tapConfig = new TapConfig();
    tapConfig.setEnabled(true);
    tapConfig.setQueueCapacity(64);
    // 1ns interval — any subsequent batch commit will exceed it, so the pump emits a heartbeat
    // on the next commit after a CDC line lands in the queue.
    tapConfig.setHeartbeatInterval(Duration.ofNanos(1));
    ActiveTap tap = new ActiveTap(tapConfig, "run-hb", "source-hb");
    TableId table = new TableId("db", "public", "orders");

    tap.onSinkBatchStart();
    tap.onCdcBatch(List.of(logEvent(OperationType.INSERT, table, "1", "binlog.000042:10")));
    tap.onSinkBatchCommit();
    tap.onSinkBatchStart();
    tap.onCdcBatch(List.of(logEvent(OperationType.INSERT, table, "2", "binlog.000042:20")));
    tap.onSinkBatchCommit();

    List<String> wire = drainQueue(tap, 6);
    String heartbeat =
        wire.stream()
            .filter(line -> line.contains("\"kind\":\"stream.heartbeat\""))
            .findFirst()
            .orElseThrow();
    assertThat(heartbeat).contains("\"run_id\":\"run-hb\"");
    assertThat(heartbeat).contains("\"source_id\":\"source-hb\"");
    assertThat(heartbeat).containsPattern("\"queue_depth\":\\d+");
    assertThat(heartbeat).contains("\"queue_capacity\":64");
  }


  @Test
  void preBatchEventsReachSubscriberAcrossFollowingBatch() throws Exception {
    // Regression test for the real-runtime call order: the dump coordinator emits
    // onWatermarkWritten / onChunkSelected / onWatermarkReceived / onChunkCollision
    // BEFORE the RuntimeRequestPump calls appendThroughEventSink (which is what invokes
    // onSinkBatchStart). If onSinkBatchStart clears the pending buffer unconditionally,
    // all of those algorithm-defining events get wiped before they reach the subscriber.
    TapConfig tapConfig = new TapConfig();
    tapConfig.setEnabled(true);
    tapConfig.setQueueCapacity(128);
    ActiveTap tap = new ActiveTap(tapConfig, "run-pre", "source-pre");
    TableSchema schema = NextTestFixtures.schema();
    TableId tableId = schema.tableId();

    Chunk chunk =
        Chunk.fromMapRows(
            "dump-pre",
            tableId.displayName(),
            schema,
            (String) null,
            List.of(NextTestFixtures.row("10", "x"), NextTestFixtures.row("11", "y")),
            "11",
            false);

    // --- Outside any sink batch: coordinator-level bookkeeping ---
    tap.onWatermarkWritten(Tap.WatermarkLevel.LOW, new WatermarkToken("lw-pre"));
    tap.onChunkSelected("req-pre", chunk);
    tap.onWatermarkReceived(
        Tap.WatermarkLevel.LOW,
        new WatermarkToken("lw-pre"),
        new OpaqueSourcePosition("wm:lw-pre"));
    tap.onChunkCollision(
        schema,
        ImmutableRowImage.of(Map.of("id", "10", "name", "stale")),
        NextTestFixtures.logUpdate(tableId, "10", "fresh", 1));
    tap.onWatermarkWritten(Tap.WatermarkLevel.HIGH, new WatermarkToken("hw-pre"));
    tap.onWatermarkReceived(
        Tap.WatermarkLevel.HIGH,
        new WatermarkToken("hw-pre"),
        new OpaqueSourcePosition("wm:hw-pre"));

    // --- Sink batch for the reconciled refresh rows ---
    tap.onSinkBatchStart();
    tap.onCdcBatch(List.of(logEvent(OperationType.INSERT, tableId, "11", "binlog.000042:55")));
    tap.onSinkBatchCommit();

    // --- Post-batch bookkeeping ---
    tap.onChunkCompleted(
        "req-pre",
        chunk,
        List.of(
            new ChangeEvent(
                tableId,
                OperationType.UPDATE,
                CaptureOrigin.SELECT,
                ImmutableRowImage.of(Map.of("id", "11")),
                null,
                ImmutableRowImage.of(Map.of("id", "11", "name", "y")),
                new OpaqueSourcePosition("wm:hw-pre"),
                null,
                "dump-pre")),
        new OpaqueSourcePosition("wm:hw-pre"));

    // Force a flush of the post-batch chunk.completed by starting another empty batch; this
    // mirrors how the next streaming-pump iteration would deliver it.
    tap.onSinkBatchStart();
    tap.onSinkBatchCommit();

    List<String> wire = drainQueue(tap, 16);
    List<String> kinds = wire.stream().map(TapHttpStreamTests::extractKind).toList();

    // All the pre-batch algorithm events must appear in the queue in seq order,
    // ahead of the subsequent CDC batch.
    assertThat(kinds)
        .containsSequence(
            "watermark.written",
            "chunk.selected",
            "watermark.received",
            "chunk.collision",
            "watermark.written",
            "watermark.received",
            "cdc");
    // chunk.completed, emitted after the sink batch commits, must also survive.
    assertThat(kinds).contains("chunk.completed");

    // Seqs must be strictly monotonic across the full run.
    List<Long> seqs =
        wire.stream().map(TapHttpStreamTests::extractSeq).toList();
    for (int i = 1; i < seqs.size(); i++) {
      assertThat(seqs.get(i)).isGreaterThan(seqs.get(i - 1));
    }
  }

  private static String extractKind(String jsonLine) {
    int start = jsonLine.indexOf("\"kind\":\"") + "\"kind\":\"".length();
    int end = jsonLine.indexOf('"', start);
    return jsonLine.substring(start, end);
  }

  private static ChangeEvent logEvent(
      OperationType operation, TableId tableId, String primaryKey, String lsn) {
    SourcePosition position = new OpaqueSourcePosition(lsn);
    ImmutableRowImage pkImage = ImmutableRowImage.of(Map.of("id", primaryKey));
    ImmutableRowImage rowImage = ImmutableRowImage.of(Map.of("id", primaryKey, "amount", "10.00"));
    return new ChangeEvent(
        tableId,
        operation,
        CaptureOrigin.LOG,
        pkImage,
        operation == OperationType.INSERT ? null : rowImage,
        operation == OperationType.DELETE ? null : rowImage,
        position,
        "tx-42",
        null);
  }

  private static void readChunkedNdjson(String url, List<String> collected) {
    try {
      HttpClient client =
          HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
      HttpResponse<java.io.InputStream> response =
          client.send(
              HttpRequest.newBuilder(URI.create(url)).GET().build(),
              HttpResponse.BodyHandlers.ofInputStream());
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(response.body(), StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          if (!line.isEmpty()) {
            collected.add(line);
          }
        }
      }
    } catch (Exception ex) {
      // Client sees socket close on stop/shutdown — expected termination.
      collected.add("reader-exit:" + ex.getClass().getSimpleName());
    }
  }

}
