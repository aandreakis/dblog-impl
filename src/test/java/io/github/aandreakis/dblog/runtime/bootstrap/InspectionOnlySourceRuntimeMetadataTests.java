package io.github.aandreakis.dblog.runtime.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.api.HeartbeatMetadataWriter;
import io.github.aandreakis.dblog.adapter.api.WatermarkMetadataWriter;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.model.WatermarkToken;
import io.github.aandreakis.dblog.core.reconcile.WatermarkWindow;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class InspectionOnlySourceRuntimeMetadataTests {
  @Test
  void writesMetadataAndEmitsSyntheticWatermarkAndHeartbeatTransactions() throws Exception {
    TableSchema schema =
        TableSchema.create(
            new TableId("sourceA", "appdb", "customers"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-10T00:00:00Z"));

    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:inspection_meta;DB_CLOSE_DELAY=-1")) {
      RecordingWatermarkWriter watermarkWriter = new RecordingWatermarkWriter();
      RecordingHeartbeatWriter heartbeatWriter = new RecordingHeartbeatWriter();
      InspectionOnlySourceRuntime runtime =
          new InspectionOnlySourceRuntime(
              "Fake",
              connection,
              List.of(schema),
              "stream-1",
              watermarkWriter,
              heartbeatWriter);

      runtime.executeWithinWatermarkWindow((sqlConnection, window) -> "ok");
      runtime.emitHeartbeatIfDue(Instant.parse("2026-04-10T00:00:05Z"), Duration.ofSeconds(5));

      assertThat(watermarkWriter.ensureCalls).isEqualTo(1);
      assertThat(heartbeatWriter.ensureCalls).isEqualTo(1);
      assertThat(watermarkWriter.tokens).hasSize(2);
      assertThat(watermarkWriter.tokens.getFirst()).isInstanceOf(WatermarkToken.class);
      assertThat(heartbeatWriter.writeCalls).isEqualTo(1);

      assertThat(runtime.readPendingTransaction()).isPresent();
      assertThat(runtime.readPendingTransaction().orElseThrow().events())
          .extracting(io.github.aandreakis.dblog.core.model.ChangeEvent::operationType)
          .containsExactly(OperationType.HEARTBEAT);
    }
  }

  @Test
  void skipsHeartbeatWritesUntilTheInMemoryDeadlineExpires() throws Exception {
    TableSchema schema =
        TableSchema.create(
            new TableId("sourceA", "appdb", "customers"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-10T00:00:00Z"));

    try (Connection connection =
        DriverManager.getConnection("jdbc:h2:mem:inspection_meta_heartbeat;DB_CLOSE_DELAY=-1")) {
      RecordingWatermarkWriter watermarkWriter = new RecordingWatermarkWriter();
      RecordingHeartbeatWriter heartbeatWriter = new RecordingHeartbeatWriter();
      InspectionOnlySourceRuntime runtime =
          new InspectionOnlySourceRuntime(
              "Fake",
              connection,
              List.of(schema),
              "stream-1",
              watermarkWriter,
              heartbeatWriter);

      assertThat(runtime.emitHeartbeatIfDue(Instant.parse("2026-04-10T00:00:05Z"), Duration.ofSeconds(5)))
          .isTrue();
      assertThat(runtime.emitHeartbeatIfDue(Instant.parse("2026-04-10T00:00:09Z"), Duration.ofSeconds(5)))
          .isFalse();
      assertThat(runtime.emitHeartbeatIfDue(Instant.parse("2026-04-10T00:00:10Z"), Duration.ofSeconds(5)))
          .isTrue();

      assertThat(watermarkWriter.ensureCalls).isEqualTo(1);
      assertThat(heartbeatWriter.ensureCalls).isEqualTo(1);
      assertThat(heartbeatWriter.writeCalls).isEqualTo(2);
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
