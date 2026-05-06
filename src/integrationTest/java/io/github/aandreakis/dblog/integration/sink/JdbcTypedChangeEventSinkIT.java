package io.github.aandreakis.dblog.integration.sink;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.sink.jdbc.JdbcTypedChangeEventSink;
import io.github.aandreakis.dblog.testsupport.ChangeEventTestFixtures;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag("integration-core")
class JdbcTypedChangeEventSinkIT {
  @TempDir Path tempDir;

  private static final TableId WIDGETS = new TableId("appdb", "public", "widgets");
  private static final TableId PAYMENTS = new TableId("appdb", "public", "payments");

  @Test
  void preservesMirroredStateAcrossSinkReopen() {
    Path databasePath = tempDir.resolve("typed-sink-it-mirror");
    List<TableSchema> schemas = List.of(widgetsSchema(), paymentsSchema());

    try (JdbcTypedChangeEventSink sink = JdbcTypedChangeEventSink.forH2(databasePath)) {
      sink.validateCapturedSchemas(schemas);
      sink.appendEvents(
          List.of(
              insertWidget(1L, "alpha", true),
              insertPayment(2L, new BigDecimal("12.3400"))));
    }

    try (JdbcTypedChangeEventSink sink = JdbcTypedChangeEventSink.forH2(databasePath)) {
      // Reopening the sink rediscovers schemas — IF NOT EXISTS keeps existing tables, and the
      // mirror state from the prior session is still present.
      sink.validateCapturedSchemas(schemas);
      sink.appendEvents(List.of(updatePayment(2L, new BigDecimal("12.3500"))));

      assertThat(sink.countRows(WIDGETS)).isEqualTo(1L);
      Map<String, Object> widget =
          sink.loadCurrentRow(WIDGETS, primaryKey("id", 1L)).orElseThrow();
      assertThat(widget).containsEntry("name", "alpha").containsEntry("enabled", true);

      assertThat(sink.countRows(PAYMENTS)).isEqualTo(1L);
      Map<String, Object> payment =
          sink.loadCurrentRow(PAYMENTS, primaryKey("id", 2L)).orElseThrow();
      assertThat(payment).containsEntry("amount", new BigDecimal("12.3500"));
    }
  }

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

  private static ChangeEvent updatePayment(long id, BigDecimal amount) {
    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>();
    primaryKey.put("id", id);
    LinkedHashMap<String, Object> beforeRow = new LinkedHashMap<>();
    beforeRow.put("id", id);
    beforeRow.put("amount", new BigDecimal("12.3400"));
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put("id", id);
    afterRow.put("amount", amount);
    return ChangeEventTestFixtures.fromRowMaps(
        PAYMENTS,
        OperationType.UPDATE,
        CaptureOrigin.LOG,
        primaryKey,
        beforeRow,
        afterRow,
        new OpaqueSourcePosition("payments:" + id + ":update"),
        "tx-pay-" + id + "-up",
        null);
  }

  private static ImmutableRowImage primaryKey(String column, Object value) {
    LinkedHashMap<String, Object> map = new LinkedHashMap<>();
    map.put(column, value);
    return ImmutableRowImage.of(map);
  }
}
