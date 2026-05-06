package io.github.aandreakis.dblog.core.reconcile;

import static io.github.aandreakis.dblog.support.NextTestFixtures.logDelete;
import static io.github.aandreakis.dblog.support.NextTestFixtures.logInsert;
import static io.github.aandreakis.dblog.support.NextTestFixtures.logUpdate;
import static io.github.aandreakis.dblog.support.NextTestFixtures.ordersSchema;
import static io.github.aandreakis.dblog.support.NextTestFixtures.row;
import static io.github.aandreakis.dblog.support.NextTestFixtures.schema;
import static io.github.aandreakis.dblog.support.NextTestFixtures.watermark;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.model.WatermarkToken;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.tap.NoopTap;
import io.github.aandreakis.dblog.testsupport.ChangeEventTestFixtures;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class WindowReconcilerTests {
  @Test
  void removesSnapshotRowsThatCollideWithinWindow() {
    TableSchema schema = schema();
    Chunk chunk =
        Chunk.fromMapRows(
            "job-1",
            schema.tableId().displayName(),
            schema,
            null,
            List.of(row("1", "old-a"), row("2", "old-b")),
            "2",
            false);
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw-1"), new WatermarkToken("hw-1"));

    List<ChangeEvent> logEvents =
        List.of(
            watermark(schema.tableId(), "lw-1"),
            logUpdate(schema.tableId(), "2", "new-b", 10),
            watermark(schema.tableId(), "hw-1"));

    ReconciliationResult result = new WindowReconciler(NoopTap.INSTANCE).reconcile(chunk, window, logEvents);

    assertThat(result.chunkCompleted()).isTrue();
    assertThat(result.emittedEvents()).hasSize(2);
    assertThat(result.emittedEvents().get(0).operationType()).isEqualTo(OperationType.UPDATE);
    assertThat(result.emittedEvents().get(1).operationType()).isEqualTo(OperationType.UPDATE);
    assertThat(result.emittedEvents().get(1).captureOrigin()).isEqualTo(CaptureOrigin.SELECT);
    assertThat(result.emittedEvents().get(1).sourcePosition().displayValue()).isEqualTo("wm:hw-1");
    assertThat(String.valueOf(result.emittedEvents().get(1).primaryKey().get("id"))).isEqualTo("1");
  }

  @Test
  void preservesOrderedEmissionWithoutTimeTravel() {
    TableSchema schema = schema();
    Chunk chunk =
        Chunk.fromMapRows(
            "job-1",
            schema.tableId().displayName(),
            schema,
            null,
            List.of(row("1", "old-a"), row("2", "old-b"), row("3", "old-c")),
            "3",
            true);
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw-2"), new WatermarkToken("hw-2"));

    List<ChangeEvent> logEvents =
        List.of(
            logInsert(schema.tableId(), "0", "already-live", 1),
            watermark(schema.tableId(), "lw-2"),
            logUpdate(schema.tableId(), "2", "fresh-b", 2),
            logDelete(schema.tableId(), "3", 3),
            watermark(schema.tableId(), "hw-2"),
            logInsert(schema.tableId(), "4", "future-d", 4));

    ReconciliationResult result = new WindowReconciler(NoopTap.INSTANCE).reconcile(chunk, window, logEvents);
    List<ChangeEvent> emitted = result.emittedEvents();

    assertThat(emitted).hasSize(5);
    assertThat(String.valueOf(emitted.get(0).primaryKey().get("id"))).isEqualTo("0");
    assertThat(String.valueOf(emitted.get(1).primaryKey().get("id"))).isEqualTo("2");
    assertThat(String.valueOf(emitted.get(2).primaryKey().get("id"))).isEqualTo("3");
    assertThat(emitted.get(3).operationType()).isEqualTo(OperationType.UPDATE);
    assertThat(String.valueOf(emitted.get(3).primaryKey().get("id"))).isEqualTo("1");
    assertThat(String.valueOf(emitted.get(4).primaryKey().get("id"))).isEqualTo("4");
  }

  @Test
  void keepsSnapshotRowsWhenOtherTablesChangeInsideWindow() {
    TableSchema schema = schema();
    TableSchema orders = ordersSchema();
    Chunk chunk =
        Chunk.fromMapRows(
            "job-7",
            schema.tableId().displayName(),
            schema,
            null,
            List.of(row("1", "old-a"), row("2", "old-b")),
            "2",
            false);
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw-7"), new WatermarkToken("hw-7"));

    List<ChangeEvent> logEvents =
        List.of(
            watermark(schema.tableId(), "lw-7"),
            logUpdate(orders.tableId(), "2", "order-2", 70),
            watermark(schema.tableId(), "hw-7"));

    ReconciliationResult result = new WindowReconciler(NoopTap.INSTANCE).reconcile(chunk, window, logEvents);

    assertThat(result.chunkCompleted()).isTrue();
    assertThat(result.emittedEvents()).hasSize(3);
    assertThat(result.emittedEvents().get(0).tableId().tableName()).isEqualTo("orders");
    assertThat(result.emittedEvents().get(1).operationType()).isEqualTo(OperationType.UPDATE);
    assertThat(String.valueOf(result.emittedEvents().get(1).primaryKey().get("id"))).isEqualTo("1");
    assertThat(String.valueOf(result.emittedEvents().get(2).primaryKey().get("id"))).isEqualTo("2");
  }

  @Test
  void failsClosedWhenUnexpectedWatermarkTokenAppearsInsideActiveWindow() {
    TableSchema schema = schema();
    Chunk chunk =
        Chunk.fromMapRows(
            "job-9",
            schema.tableId().displayName(),
            schema,
            null,
            List.of(row("7", "old-z")),
            "7",
            true);
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw-9"), new WatermarkToken("hw-9"));

    assertThatThrownBy(
            () ->
                new WindowReconciler(NoopTap.INSTANCE)
                    .reconcile(chunk, window, List.of(watermark(schema.tableId(), "unexpected"))))
        .isInstanceOf(WatermarkSequenceException.class)
        .hasMessageContaining("unexpected watermark token");
  }

  @Test
  void completesWithNoEmissionsOnEmptyChunk() {
    TableSchema schema = schema();
    Chunk chunk =
        Chunk.fromMapRows(
            "job-empty",
            schema.tableId().displayName(),
            schema,
            (String) null,
            List.of(),
            (String) null,
            true);
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw-e"), new WatermarkToken("hw-e"));

    ReconciliationResult result =
        new WindowReconciler(NoopTap.INSTANCE)
            .reconcile(
                chunk,
                window,
                List.of(watermark(schema.tableId(), "lw-e"), watermark(schema.tableId(), "hw-e")));

    assertThat(result.chunkCompleted()).isTrue();
    assertThat(result.emittedEvents()).isEmpty();
    assertThat(result.remainingSnapshotRows()).isEmpty();
  }

  @Test
  void completesWithAllRowsSuppressedWhenEveryKeyCollides() {
    TableSchema schema = schema();
    Chunk chunk =
        Chunk.fromMapRows(
            "job-coll",
            schema.tableId().displayName(),
            schema,
            null,
            List.of(row("1", "old-a"), row("2", "old-b"), row("3", "old-c")),
            "3",
            true);
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw-c"), new WatermarkToken("hw-c"));

    ReconciliationResult result =
        new WindowReconciler(NoopTap.INSTANCE)
            .reconcile(
                chunk,
                window,
                List.of(
                    watermark(schema.tableId(), "lw-c"),
                    logUpdate(schema.tableId(), "1", "fresh-a", 10),
                    logUpdate(schema.tableId(), "2", "fresh-b", 11),
                    logDelete(schema.tableId(), "3", 12),
                    watermark(schema.tableId(), "hw-c")));

    assertThat(result.chunkCompleted()).isTrue();
    // Three in-window events emitted; no snapshot remainder because every key collided.
    assertThat(result.emittedEvents()).hasSize(3);
    assertThat(result.emittedEvents())
        .allSatisfy(event -> assertThat(event.captureOrigin()).isEqualTo(CaptureOrigin.LOG));
    assertThat(result.remainingSnapshotRows()).isEmpty();
  }

  @Test
  void handlesCompositePrimaryKeysInCollisionMatching() {
    TableSchema schema = compositePkSchema();
    Map<String, Object> rowA1 = compositeRow("acct-1", "EU", "old-1");
    Map<String, Object> rowA2 = compositeRow("acct-1", "US", "old-2");
    Map<String, Object> rowB1 = compositeRow("acct-2", "EU", "old-3");
    Chunk chunk =
        Chunk.fromMapRows(
            "job-comp",
            schema.tableId().displayName(),
            schema,
            (String) null,
            List.of(rowA1, rowA2, rowB1),
            (String) null,
            true);
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw-p"), new WatermarkToken("hw-p"));

    // Update only (acct-1, US); the other two should flush as SELECT-origin UPDATEs on HW.
    ChangeEvent inWindowUpdate =
        ChangeEventTestFixtures.fromRowMaps(
            schema.tableId(),
            OperationType.UPDATE,
            CaptureOrigin.LOG,
            Map.of("account_id", "acct-1", "region", "US"),
            Map.of("account_id", "acct-1", "region", "US", "name", "stale"),
            Map.of("account_id", "acct-1", "region", "US", "name", "fresh"),
            new OpaqueSourcePosition("lsn:100"),
            "tx-100",
            null);

    ReconciliationResult result =
        new WindowReconciler(NoopTap.INSTANCE)
            .reconcile(
                chunk,
                window,
                List.of(
                    watermark(schema.tableId(), "lw-p"),
                    inWindowUpdate,
                    watermark(schema.tableId(), "hw-p")));

    assertThat(result.chunkCompleted()).isTrue();
    List<ChangeEvent> emitted = result.emittedEvents();
    assertThat(emitted).hasSize(3); // one in-window log UPDATE + two SELECT-origin remainder
    assertThat(emitted.get(0).captureOrigin()).isEqualTo(CaptureOrigin.LOG);
    assertThat(emitted.get(0).primaryKey().asMap())
        .isEqualTo(Map.of("account_id", "acct-1", "region", "US"));
    // Remaining snapshot rows are flushed in insertion order; (acct-1,US) was removed.
    assertThat(emitted.get(1).captureOrigin()).isEqualTo(CaptureOrigin.SELECT);
    assertThat(emitted.get(1).primaryKey().asMap())
        .isEqualTo(Map.of("account_id", "acct-1", "region", "EU"));
    assertThat(emitted.get(2).captureOrigin()).isEqualTo(CaptureOrigin.SELECT);
    assertThat(emitted.get(2).primaryKey().asMap())
        .isEqualTo(Map.of("account_id", "acct-2", "region", "EU"));
  }

  @Test
  void handlesLargeInWindowBurstWithoutOrderLoss() {
    TableSchema schema = schema();
    List<Map<String, Object>> seed = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      seed.add(row(Integer.toString(i), "old-" + i));
    }
    Chunk chunk =
        Chunk.fromMapRows(
            "job-burst",
            schema.tableId().displayName(),
            schema,
            null,
            seed,
            "49",
            true);
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw-b"), new WatermarkToken("hw-b"));

    List<ChangeEvent> events = new ArrayList<>();
    events.add(watermark(schema.tableId(), "lw-b"));
    // 40 in-window events covering half of the chunk's keys plus 10 non-colliding keys.
    for (int i = 0; i < 30; i++) {
      events.add(logUpdate(schema.tableId(), Integer.toString(i), "fresh-" + i, 1000L + i));
    }
    for (int i = 100; i < 110; i++) {
      events.add(logInsert(schema.tableId(), Integer.toString(i), "new-" + i, 2000L + i));
    }
    events.add(watermark(schema.tableId(), "hw-b"));

    ReconciliationResult result = new WindowReconciler(NoopTap.INSTANCE).reconcile(chunk, window, events);

    assertThat(result.chunkCompleted()).isTrue();
    List<ChangeEvent> emitted = result.emittedEvents();
    // 40 in-window log events + 20 snapshot remainders (50 original - 30 collisions).
    assertThat(emitted).hasSize(60);

    // All log-origin events come before any SELECT-origin remainder.
    int lastLogIndex = -1;
    int firstSelectIndex = -1;
    for (int i = 0; i < emitted.size(); i++) {
      if (emitted.get(i).captureOrigin() == CaptureOrigin.LOG) {
        lastLogIndex = i;
      } else if (firstSelectIndex < 0) {
        firstSelectIndex = i;
      }
    }
    assertThat(firstSelectIndex).isGreaterThan(lastLogIndex);

    // Log-origin events preserve arrival order.
    List<Object> emittedLogKeysInOrder = new ArrayList<>();
    for (int i = 0; i <= lastLogIndex; i++) {
      emittedLogKeysInOrder.add(emitted.get(i).primaryKey().get("id"));
    }
    List<Object> expectedLogKeys = new ArrayList<>();
    for (int i = 0; i < 30; i++) expectedLogKeys.add(Integer.toString(i));
    for (int i = 100; i < 110; i++) expectedLogKeys.add(Integer.toString(i));
    assertThat(emittedLogKeysInOrder).containsExactlyElementsOf(expectedLogKeys);

    // Snapshot remainder is exactly the 20 non-collided keys, in original chunk order.
    List<Object> emittedSelectKeysInOrder = new ArrayList<>();
    for (int i = firstSelectIndex; i < emitted.size(); i++) {
      assertThat(emitted.get(i).captureOrigin()).isEqualTo(CaptureOrigin.SELECT);
      emittedSelectKeysInOrder.add(emitted.get(i).primaryKey().get("id"));
    }
    List<Object> expectedSelectKeys = new ArrayList<>();
    for (int i = 30; i < 50; i++) expectedSelectKeys.add(Integer.toString(i));
    assertThat(emittedSelectKeysInOrder).containsExactlyElementsOf(expectedSelectKeys);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidWatermarkSequences")
  void failsClosedOnInvalidWatermarkSequence(
      String caseName, List<ChangeEvent> events, String expectedMessageFragment) {
    TableSchema schema = schema();
    Chunk chunk =
        Chunk.fromMapRows(
            "job-fc",
            schema.tableId().displayName(),
            schema,
            null,
            List.of(row("1", "old-a")),
            "1",
            true);
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw-fc"), new WatermarkToken("hw-fc"));

    assertThatThrownBy(() -> new WindowReconciler(NoopTap.INSTANCE).reconcile(chunk, window, events))
        .isInstanceOf(WatermarkSequenceException.class)
        .hasMessageContaining(expectedMessageFragment);
  }

  @SuppressWarnings("unused") // used via @MethodSource
  private static Stream<Arguments> invalidWatermarkSequences() {
    TableSchema schema = schema();
    TableId metaTableId = WatermarkMetadata.tableIdFor(schema.tableId().databaseName());
    return Stream.of(
        Arguments.of(
            "duplicate low watermark",
            List.of(
                watermark(schema.tableId(), "lw-fc"),
                watermark(schema.tableId(), "lw-fc")),
            "duplicate low watermark"),
        Arguments.of(
            "duplicate high watermark",
            List.of(
                watermark(schema.tableId(), "lw-fc"),
                watermark(schema.tableId(), "hw-fc"),
                watermark(schema.tableId(), "hw-fc")),
            "duplicate high watermark"),
        Arguments.of(
            "high before low",
            List.of(watermark(schema.tableId(), "hw-fc")),
            "high watermark arrived before the low watermark"),
        Arguments.of(
            "low after high",
            List.of(
                watermark(schema.tableId(), "lw-fc"),
                watermark(schema.tableId(), "hw-fc"),
                watermark(schema.tableId(), "lw-fc")),
            "low watermark arrived after the high watermark"),
        Arguments.of(
            "unknown token before low watermark",
            List.of(watermark(schema.tableId(), "bogus-token")),
            "unexpected watermark token"),
        Arguments.of(
            "watermark with non-LOG capture origin",
            List.of(
                new ChangeEvent(
                    metaTableId,
                    OperationType.WATERMARK,
                    CaptureOrigin.SELECT,
                    WatermarkMetadata.singletonPrimaryKey(),
                    null,
                    ImmutableRowImage.of(Map.of(WatermarkMetadata.TOKEN_COLUMN, "lw-fc")),
                    new OpaqueSourcePosition("wm:lw-fc"),
                    null,
                    null)),
            "watermark event must use LOG capture origin"),
        Arguments.of(
            "watermark on non-metadata table",
            List.of(
                new ChangeEvent(
                    schema.tableId(),
                    OperationType.WATERMARK,
                    CaptureOrigin.LOG,
                    WatermarkMetadata.singletonPrimaryKey(),
                    null,
                    ImmutableRowImage.of(Map.of(WatermarkMetadata.TOKEN_COLUMN, "lw-fc")),
                    new OpaqueSourcePosition("wm:lw-fc"),
                    null,
                    null)),
            "dedicated metadata table"),
        Arguments.of(
            "watermark with non-singleton primary key",
            List.of(
                new ChangeEvent(
                    metaTableId,
                    OperationType.WATERMARK,
                    CaptureOrigin.LOG,
                    ImmutableRowImage.of(Map.of(WatermarkMetadata.PRIMARY_KEY_COLUMN, 42L)),
                    null,
                    ImmutableRowImage.of(Map.of(WatermarkMetadata.TOKEN_COLUMN, "lw-fc")),
                    new OpaqueSourcePosition("wm:lw-fc"),
                    null,
                    null)),
            "singleton metadata row"),
        Arguments.of(
            "watermark missing afterRow payload",
            List.of(
                new ChangeEvent(
                    metaTableId,
                    OperationType.WATERMARK,
                    CaptureOrigin.LOG,
                    WatermarkMetadata.singletonPrimaryKey(),
                    null,
                    null,
                    new OpaqueSourcePosition("wm:lw-fc"),
                    null,
                    null)),
            "afterRow"),
        Arguments.of(
            "watermark with blank token column",
            List.of(
                new ChangeEvent(
                    metaTableId,
                    OperationType.WATERMARK,
                    CaptureOrigin.LOG,
                    WatermarkMetadata.singletonPrimaryKey(),
                    null,
                    ImmutableRowImage.of(Map.of(WatermarkMetadata.TOKEN_COLUMN, "   ")),
                    new OpaqueSourcePosition("wm:blank"),
                    null,
                    null)),
            "non-blank token column"),
        Arguments.of(
            "non-watermark operation on metadata table",
            List.of(
                new ChangeEvent(
                    metaTableId,
                    OperationType.UPDATE,
                    CaptureOrigin.LOG,
                    WatermarkMetadata.singletonPrimaryKey(),
                    null,
                    ImmutableRowImage.of(Map.of(WatermarkMetadata.TOKEN_COLUMN, "lw-fc")),
                    new OpaqueSourcePosition("lsn:meta-update"),
                    "tx-meta",
                    null)),
            "metadata table changes must be surfaced as WATERMARK events"),
        Arguments.of(
            "unknown token inside active window",
            List.of(
                watermark(schema.tableId(), "lw-fc"),
                watermark(schema.tableId(), "bogus-token")),
            "unexpected watermark token"));
  }

  private static TableSchema compositePkSchema() {
    return TableSchema.create(
        new TableId("app", "public", "accounts_by_region"),
        List.of(
            new ColumnDefinition(
                "account_id", "varchar(32)", NeutralColumnType.STRING, true, 1, false),
            new ColumnDefinition("region", "varchar(8)", NeutralColumnType.STRING, true, 2, false),
            new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true)),
        Instant.parse("2026-03-17T00:00:05Z"));
  }

  private static Map<String, Object> compositeRow(String accountId, String region, String name) {
    LinkedHashMap<String, Object> row = new LinkedHashMap<>();
    row.put("account_id", accountId);
    row.put("region", region);
    row.put("name", name);
    return row;
  }
}
