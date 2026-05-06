package io.github.aandreakis.dblog.adapter.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.api.HeartbeatMetadataWriter;
import io.github.aandreakis.dblog.adapter.api.WatermarkMetadataWriter;
import io.github.aandreakis.dblog.adapter.mysql.internal.MySqlTransactionStream;
import io.github.aandreakis.dblog.adapter.mysql.internal.MySqlTransactionStreamingSession;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.runtime.sql.SourceFlowControlSnapshot;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import io.github.aandreakis.dblog.tap.NoopTap;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MySqlLiveStreamingRuntimeTests {
  @TempDir Path tempDir;

  @Test
  void exposesCheckpointAndFlowControlThroughStreamBackedSession() throws Exception {
    TableSchema schema =
        TableSchema.create(
            new TableId("sourceA", "appdb", "widgets"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-11T00:00:00Z"));
    MySqlBinlogTransaction transaction =
        new MySqlBinlogTransaction(
            "tx-1",
            null,
            new MySqlSourcePosition("mysql-bin.000001", 42L, null),
            Instant.parse("2026-04-11T00:00:01Z"),
            List.<ChangeEvent>of());

    try (H2RuntimeStateStore stateStore = new H2RuntimeStateStore(tempDir.resolve("mysql-live-runtime"));
        Connection connection = DriverManager.getConnection("jdbc:h2:mem:mysql_live_runtime;DB_CLOSE_DELAY=-1")) {
      MySqlSourceCheckpointStore checkpointStore = new MySqlSourceCheckpointStore(stateStore);
      MySqlTransactionStreamingSession session =
          new MySqlTransactionStreamingSession(
              "sourceA",
              List.of(schema),
              new MySqlTransactionStream() {
                private boolean returned;

                @Override
                public Optional<MySqlBinlogTransaction> readPendingTransaction() {
                  if (returned) {
                    return Optional.empty();
                  }
                  returned = true;
                  return Optional.of(transaction);
                }

                @Override
                public SourceFlowControlSnapshot sourceFlowControlSnapshot() {
                  return SourceFlowControlSnapshot.directPoll();
                }
              },
              checkpointStore);
      MySqlLiveStreamingRuntime runtime =
          new MySqlLiveStreamingRuntime(
              connection,
              "sourceA",
              "mysql-live",
              checkpointStore,
              new WatermarkMetadataWriter() {
                @Override
                public void ensureMetadataTable(Connection connection) {}

                @Override
                public void writeWatermark(
                    Connection sqlConnection,
                    String runId,
                    io.github.aandreakis.dblog.core.model.WatermarkToken token) {}
              },
              new HeartbeatMetadataWriter() {
                @Override
                public void ensureHeartbeatTable(Connection connection) {}

                @Override
                public boolean writeHeartbeatIfDue(
                    Connection connection,
                    String runId,
                    String sourceStreamId,
                    Instant heartbeatTime,
                    java.time.Duration minimumInterval) {
                  return false;
                }
              },
              session, NoopTap.INSTANCE);

      assertThat(runtime.readPendingTransaction()).contains(transaction);
      runtime.acknowledge(transaction);
      assertThat(runtime.lastAcknowledgedCheckpointDisplayValue()).isEqualTo("mysql-bin.000001:42");
      assertThat(runtime.sourceFlowControlSnapshot().mode())
          .isEqualTo(SourceFlowControlSnapshot.Mode.DIRECT_POLL);
      assertThat(runtime.capturedTableCount()).isEqualTo(1);
    }
  }

  @Test
  void cachesMetadataInitializationAndSkipsHeartbeatWritesUntilDue() throws Exception {
    TableSchema schema =
        TableSchema.create(
            new TableId("sourceA", "appdb", "widgets"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-11T00:00:00Z"));

    try (H2RuntimeStateStore stateStore =
            new H2RuntimeStateStore(tempDir.resolve("mysql-live-runtime-heartbeat"));
        Connection connection =
            DriverManager.getConnection("jdbc:h2:mem:mysql_live_runtime_heartbeat;DB_CLOSE_DELAY=-1")) {
      RecordingWatermarkWriter watermarkWriter = new RecordingWatermarkWriter();
      RecordingHeartbeatWriter heartbeatWriter = new RecordingHeartbeatWriter(true);
      MySqlLiveStreamingRuntime runtime =
          new MySqlLiveStreamingRuntime(
              connection,
              "sourceA",
              "mysql-live",
              new MySqlSourceCheckpointStore(stateStore),
              watermarkWriter,
              heartbeatWriter,
              emptySession("sourceA", schema, new MySqlSourceCheckpointStore(stateStore)), NoopTap.INSTANCE);

      runtime.executeWithinWatermarkWindow((sqlConnection, window) -> "ok");
      assertThat(runtime.emitHeartbeatIfDue(Instant.parse("2026-04-11T00:00:05Z"), Duration.ofSeconds(5)))
          .isTrue();
      assertThat(runtime.emitHeartbeatIfDue(Instant.parse("2026-04-11T00:00:09Z"), Duration.ofSeconds(5)))
          .isFalse();
      assertThat(runtime.emitHeartbeatIfDue(Instant.parse("2026-04-11T00:00:10Z"), Duration.ofSeconds(5)))
          .isTrue();

      assertThat(watermarkWriter.ensureCalls).isEqualTo(1);
      assertThat(heartbeatWriter.ensureCalls).isEqualTo(1);
      assertThat(watermarkWriter.tokens).hasSize(2);
      assertThat(heartbeatWriter.writeCalls).isEqualTo(2);
    }
  }

  private static MySqlTransactionStreamingSession emptySession(
      String sourceId, TableSchema schema, MySqlSourceCheckpointStore checkpointStore) {
    return new MySqlTransactionStreamingSession(
        sourceId,
        List.of(schema),
        new MySqlTransactionStream() {
          @Override
          public Optional<MySqlBinlogTransaction> readPendingTransaction() {
            return Optional.empty();
          }

          @Override
          public SourceFlowControlSnapshot sourceFlowControlSnapshot() {
            return SourceFlowControlSnapshot.directPoll();
          }
        },
        checkpointStore);
  }

  private static final class RecordingWatermarkWriter implements WatermarkMetadataWriter {
    private int ensureCalls;
    private final List<io.github.aandreakis.dblog.core.model.WatermarkToken> tokens = new ArrayList<>();

    @Override
    public void ensureMetadataTable(Connection connection) {
      ensureCalls++;
    }

    @Override
    public void writeWatermark(
        Connection sqlConnection,
        String runId,
        io.github.aandreakis.dblog.core.model.WatermarkToken token) {
      tokens.add(token);
    }
  }

  private static final class RecordingHeartbeatWriter implements HeartbeatMetadataWriter {
    private final boolean result;
    private int ensureCalls;
    private int writeCalls;

    private RecordingHeartbeatWriter(boolean result) {
      this.result = result;
    }

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
      return result;
    }
  }
}
