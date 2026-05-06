package io.github.aandreakis.dblog.adapter.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.api.HeartbeatMetadataWriter;
import io.github.aandreakis.dblog.adapter.api.WatermarkMetadataWriter;
import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.model.WatermarkToken;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.testsupport.ChangeEventTestFixtures;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class MySqlBufferedStreamingRuntimeTests {
  @Test
  void queuesCommittedTransactionsAndSyntheticMetadataTransactions() throws Exception {
    TableSchema schema =
        TableSchema.create(
            new TableId("sourceA", "appdb", "customers"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-10T00:00:00Z"));

    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:mysql_buffered_runtime;DB_CLOSE_DELAY=-1")) {
      RecordingWatermarkWriter watermarkWriter = new RecordingWatermarkWriter();
      RecordingHeartbeatWriter heartbeatWriter = new RecordingHeartbeatWriter();
      MySqlBufferedStreamingRuntime runtime =
          new MySqlBufferedStreamingRuntime(
              connection,
              "sourceA",
              "stream-1",
              List.of(schema),
              watermarkWriter,
              heartbeatWriter);

      MySqlBinlogTransaction committed =
          new MySqlBinlogTransaction(
              "tx-1",
              null,
              new MySqlSourcePosition("mysql-bin.000001", 42L, null),
              Instant.parse("2026-04-10T00:00:01Z"),
              List.of(
                  ChangeEventTestFixtures.fromRowMaps(
                      schema.tableId(),
                      OperationType.UPDATE,
                      CaptureOrigin.LOG,
                      Map.of("id", 1),
                      null,
                      Map.of("id", 1),
                      new MySqlSourcePosition("mysql-bin.000001", 42L, null),
                      "tx-1",
                      null)));
      runtime.enqueueCommittedTransaction(committed);

      runtime.executeWithinWatermarkWindow((sqlConnection, window) -> "ok");
      runtime.emitHeartbeatIfDue(Instant.parse("2026-04-10T00:00:05Z"), Duration.ofSeconds(5));

      assertThat(runtime.readPendingTransaction()).contains(committed);
      assertThat(runtime.readPendingTransaction().orElseThrow().events())
          .extracting(ChangeEvent::operationType)
          .containsExactly(OperationType.WATERMARK, OperationType.WATERMARK);
      assertThat(runtime.readPendingTransaction().orElseThrow().events())
          .extracting(ChangeEvent::operationType)
          .containsExactly(OperationType.HEARTBEAT);
      assertThat(watermarkWriter.tokens).hasSize(2);
      assertThat(watermarkWriter.ensureCalls).isEqualTo(1);
      assertThat(heartbeatWriter.ensureCalls).isEqualTo(1);
      assertThat(heartbeatWriter.writeCalls).isEqualTo(1);
    }
  }

  @Test
  void seedsAndPersistsAcknowledgedCheckpointState() throws Exception {
    TableSchema schema =
        TableSchema.create(
            new TableId("sourceA", "appdb", "customers"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-10T00:00:00Z"));

    try (Connection connection =
        DriverManager.getConnection("jdbc:h2:mem:mysql_buffered_runtime_state;DB_CLOSE_DELAY=-1")) {
      AtomicReference<MySqlSourcePosition> persisted = new AtomicReference<>();
      MySqlBufferedStreamingRuntime runtime =
          new MySqlBufferedStreamingRuntime(
              connection,
              "sourceA",
              "stream-1",
              List.of(schema),
              new RecordingWatermarkWriter(),
              new RecordingHeartbeatWriter(),
              persisted::set,
              new MySqlSourcePosition("mysql-bin.000001", 41L, null));

      assertThat(runtime.lastAcknowledgedCheckpointDisplayValue()).isEqualTo("mysql-bin.000001:41");

      MySqlBinlogTransaction committed =
          new MySqlBinlogTransaction(
              "tx-1",
              null,
              new MySqlSourcePosition("mysql-bin.000001", 42L, null),
              Instant.parse("2026-04-10T00:00:01Z"),
              List.of());

      runtime.acknowledge(committed);

      assertThat(persisted.get()).isEqualTo(new MySqlSourcePosition("mysql-bin.000001", 42L, null));
      assertThat(runtime.lastAcknowledgedCheckpointDisplayValue()).isEqualTo("mysql-bin.000001:42");
    }
  }

  private static final class RecordingWatermarkWriter implements WatermarkMetadataWriter {
    private int ensureCalls;
    private final List<WatermarkToken> tokens = new ArrayList<>();

    @Override
    public void ensureMetadataTable(Connection connection) {
      ensureCalls++;
    }

    @Override
    public void writeWatermark(Connection connection, String runId, WatermarkToken token) {
      tokens.add(token);
    }
  }

  private static final class RecordingHeartbeatWriter implements HeartbeatMetadataWriter {
    private int ensureCalls;
    private int writeCalls;

    @Override
    public void ensureHeartbeatTable(Connection connection) {
      ensureCalls++;
    }

    @Override
    public boolean writeHeartbeatIfDue(
        Connection connection,
        String runId,
        String sourceStreamId,
        Instant heartbeatTime,
        Duration minimumInterval) {
      writeCalls++;
      return true;
    }
  }
}
