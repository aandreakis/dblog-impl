package io.github.aandreakis.dblog.sink.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.reconcile.HeartbeatMetadata;
import io.github.aandreakis.dblog.core.reconcile.WatermarkMetadata;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.sink.jdbc.JdbcTypedChangeEventSink.EventSummary;
import io.github.aandreakis.dblog.testsupport.ChangeEventTestFixtures;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JdbcTypedChangeEventSinkTests {
  @TempDir Path tempDir;

  private static final TableId WIDGETS = new TableId("appdb", "public", "widgets");
  private static final TableId PAYMENTS = new TableId("appdb", "public", "payments");

  @Test
  void mirrorsInsertedRowsToTargetTablePerSourceSchema() {
    try (JdbcTypedChangeEventSink sink =
        JdbcTypedChangeEventSink.forH2(tempDir.resolve("typed-mirror-insert"))) {
      sink.validateCapturedSchemas(List.of(widgetsSchema()));

      sink.appendEvents(List.of(insertWidget(1L, "alpha", true)));

      assertThat(sink.countRows(WIDGETS)).isEqualTo(1L);
      Map<String, Object> row = sink.loadCurrentRow(WIDGETS, primaryKey("id", 1L)).orElseThrow();
      assertThat(row).containsEntry("name", "alpha").containsEntry("enabled", true);
    }
  }

  @Test
  void upsertsLatestStateOnUpdateAndRemovesRowOnDelete() {
    try (JdbcTypedChangeEventSink sink =
        JdbcTypedChangeEventSink.forH2(tempDir.resolve("typed-mirror-upsert"))) {
      sink.validateCapturedSchemas(List.of(widgetsSchema()));

      sink.appendEvents(List.of(insertWidget(7L, "before", false)));
      sink.appendEvents(List.of(updateWidget(7L, "after", true)));

      Map<String, Object> updated = sink.loadCurrentRow(WIDGETS, primaryKey("id", 7L)).orElseThrow();
      assertThat(updated).containsEntry("name", "after").containsEntry("enabled", true);

      sink.appendEvents(List.of(deleteWidget(7L)));
      assertThat(sink.loadCurrentRow(WIDGETS, primaryKey("id", 7L))).isEmpty();
      assertThat(sink.countRows(WIDGETS)).isZero();
    }
  }

  @Test
  void filtersWatermarkAndHeartbeatEventsFromMirrorAndCounters() {
    try (JdbcTypedChangeEventSink sink =
        JdbcTypedChangeEventSink.forH2(tempDir.resolve("typed-mirror-filter"))) {
      sink.validateCapturedSchemas(List.of(widgetsSchema()));

      sink.appendEvents(
          List.of(
              watermarkControlEvent(),
              heartbeatControlEvent(),
              insertWidget(11L, "kept", true)));

      assertThat(sink.countRows(WIDGETS)).isEqualTo(1L);
      EventSummary summary = sink.summarizeEvents();
      assertThat(summary.totalEvents()).isEqualTo(1L);
      assertThat(summary.logEvents()).isEqualTo(1L);
      assertThat(summary.selectEvents()).isZero();
    }
  }

  @Test
  void summarizesEventsByCaptureOriginAcrossMultipleAppendCalls() {
    try (JdbcTypedChangeEventSink sink =
        JdbcTypedChangeEventSink.forH2(tempDir.resolve("typed-mirror-summary"))) {
      sink.validateCapturedSchemas(List.of(widgetsSchema(), paymentsSchema()));

      sink.appendEvents(
          List.of(
              selectOriginRefreshWidget(1L, "from-select"),
              insertWidget(2L, "from-log", true),
              insertPayment(3L, new BigDecimal("9.9900"))));

      EventSummary summary = sink.summarizeEvents();
      assertThat(summary.totalEvents()).isEqualTo(3L);
      assertThat(summary.logEvents()).isEqualTo(2L);
      assertThat(summary.selectEvents()).isEqualTo(1L);

      assertThat(sink.countRows(WIDGETS)).isEqualTo(2L);
      assertThat(sink.countRows(PAYMENTS)).isEqualTo(1L);
    }
  }

  @Test
  void loadFirstNonNullAfterValueReturnsValueFromMirrorTable() {
    try (JdbcTypedChangeEventSink sink =
        JdbcTypedChangeEventSink.forH2(tempDir.resolve("typed-mirror-firstvalue"))) {
      sink.validateCapturedSchemas(List.of(widgetsSchema(), paymentsSchema()));
      sink.appendEvents(
          List.of(insertWidget(5L, "alpha", true), insertPayment(6L, new BigDecimal("12.3500"))));

      assertThat(sink.loadFirstNonNullAfterValue(WIDGETS, "name")).contains("alpha");
      assertThat(sink.loadFirstNonNullAfterValue(PAYMENTS, "amount"))
          .contains(new BigDecimal("12.3500"));
      assertThat(sink.loadFirstNonNullAfterValue(PAYMENTS, "missing_column")).isEmpty();
    }
  }

  @Test
  void closeSurfacesTypedSinkCleanupFailures() throws Exception {
    String jdbcUrl = JdbcTypedChangeEventSink.h2JdbcUrl(tempDir.resolve("typed-close-failure"));

    JdbcTypedChangeEventSink sink =
        new JdbcTypedChangeEventSink(
            "org.h2.Driver",
            jdbcUrl,
            new JdbcTypedChangeEventSink.SqlConnectionSource() {
              @Override
              public Connection open() throws SQLException {
                return DriverManager.getConnection(jdbcUrl);
              }

              @Override
              public void close() {
                throw new RuntimeException("typed close boom");
              }
            });

    assertThatThrownBy(sink::close)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Failed to close typed JDBC sink")
        .hasRootCauseMessage("typed close boom");
  }

  // ---- fixtures ------------------------------------------------------------------------------

  private static TableSchema widgetsSchema() {
    return TableSchema.create(
        WIDGETS,
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true),
            new ColumnDefinition("enabled", "boolean", NeutralColumnType.BOOLEAN, false, true)),
        Instant.parse("2026-04-10T00:00:00Z"));
  }

  private static TableSchema paymentsSchema() {
    return TableSchema.create(
        PAYMENTS,
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
            new ColumnDefinition("amount", "decimal(18,4)", NeutralColumnType.DECIMAL, false, true)),
        Instant.parse("2026-04-10T00:00:00Z"));
  }

  private static ChangeEvent insertWidget(long id, String name, boolean enabled) {
    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>();
    primaryKey.put("id", id);
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put("id", id);
    afterRow.put("name", name);
    afterRow.put("enabled", enabled);
    return ChangeEventTestFixtures.fromRowMaps(
        WIDGETS,
        OperationType.INSERT,
        CaptureOrigin.LOG,
        primaryKey,
        null,
        afterRow,
        new OpaqueSourcePosition("widgets:" + id + ":insert"),
        "tx-" + id,
        null);
  }

  private static ChangeEvent updateWidget(long id, String name, boolean enabled) {
    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>();
    primaryKey.put("id", id);
    LinkedHashMap<String, Object> beforeRow = new LinkedHashMap<>();
    beforeRow.put("id", id);
    beforeRow.put("name", "before");
    beforeRow.put("enabled", false);
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put("id", id);
    afterRow.put("name", name);
    afterRow.put("enabled", enabled);
    return ChangeEventTestFixtures.fromRowMaps(
        WIDGETS,
        OperationType.UPDATE,
        CaptureOrigin.LOG,
        primaryKey,
        beforeRow,
        afterRow,
        new OpaqueSourcePosition("widgets:" + id + ":update"),
        "tx-" + id + "-up",
        null);
  }

  private static ChangeEvent deleteWidget(long id) {
    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>();
    primaryKey.put("id", id);
    LinkedHashMap<String, Object> beforeRow = new LinkedHashMap<>();
    beforeRow.put("id", id);
    beforeRow.put("name", "after");
    beforeRow.put("enabled", true);
    return ChangeEventTestFixtures.fromRowMaps(
        WIDGETS,
        OperationType.DELETE,
        CaptureOrigin.LOG,
        primaryKey,
        beforeRow,
        null,
        new OpaqueSourcePosition("widgets:" + id + ":delete"),
        "tx-" + id + "-del",
        null);
  }

  private static ChangeEvent selectOriginRefreshWidget(long id, String name) {
    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>();
    primaryKey.put("id", id);
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put("id", id);
    afterRow.put("name", name);
    afterRow.put("enabled", true);
    return ChangeEventTestFixtures.fromRowMaps(
        WIDGETS,
        OperationType.UPDATE,
        CaptureOrigin.SELECT,
        primaryKey,
        null,
        afterRow,
        new OpaqueSourcePosition("widgets:" + id + ":select"),
        null,
        "dump-" + id);
  }

  private static ChangeEvent insertPayment(long id, BigDecimal amount) {
    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>();
    primaryKey.put("id", id);
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put("id", id);
    afterRow.put("amount", amount);
    return ChangeEventTestFixtures.fromRowMaps(
        PAYMENTS,
        OperationType.INSERT,
        CaptureOrigin.LOG,
        primaryKey,
        null,
        afterRow,
        new OpaqueSourcePosition("payments:" + id + ":insert"),
        "tx-pay-" + id,
        null);
  }

  private static ChangeEvent watermarkControlEvent() {
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put(WatermarkMetadata.RUN_ID_COLUMN, "run-1");
    afterRow.put(WatermarkMetadata.TOKEN_COLUMN, "token-x");
    return ChangeEventTestFixtures.fromRowMaps(
        WatermarkMetadata.tableIdFor("appdb"),
        OperationType.WATERMARK,
        CaptureOrigin.LOG,
        new LinkedHashMap<>(Map.of(WatermarkMetadata.PRIMARY_KEY_COLUMN, 1L)),
        null,
        afterRow,
        new OpaqueSourcePosition("wm:1"),
        "tx-wm",
        null);
  }

  private static ChangeEvent heartbeatControlEvent() {
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put(HeartbeatMetadata.RUN_ID_COLUMN, "run-1");
    afterRow.put(HeartbeatMetadata.SOURCE_STREAM_ID_COLUMN, "stream-1");
    afterRow.put(HeartbeatMetadata.TIMESTAMP_COLUMN, "2026-04-10T00:00:01Z");
    return ChangeEventTestFixtures.fromRowMaps(
        HeartbeatMetadata.tableIdFor("appdb"),
        OperationType.HEARTBEAT,
        CaptureOrigin.LOG,
        new LinkedHashMap<>(Map.of(HeartbeatMetadata.PRIMARY_KEY_COLUMN, 1L)),
        null,
        afterRow,
        new OpaqueSourcePosition("hb:1"),
        "tx-hb",
        null);
  }

  private static ImmutableRowImage primaryKey(String column, Object value) {
    LinkedHashMap<String, Object> map = new LinkedHashMap<>();
    map.put(column, value);
    return ImmutableRowImage.of(map);
  }
}
