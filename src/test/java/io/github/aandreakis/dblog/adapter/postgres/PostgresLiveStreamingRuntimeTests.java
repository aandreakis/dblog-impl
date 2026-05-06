package io.github.aandreakis.dblog.adapter.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.github.aandreakis.dblog.adapter.api.HeartbeatMetadataWriter;
import io.github.aandreakis.dblog.adapter.api.WatermarkMetadataWriter;
import io.github.aandreakis.dblog.adapter.postgres.internal.PostgresPgoutputStream;
import io.github.aandreakis.dblog.adapter.postgres.internal.PostgresTransactionStreamingSession;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PostgresLiveStreamingRuntimeTests {
  @TempDir Path tempDir;

  @Test
  void exposesCheckpointAndFlowControlThroughPgoutputSession() throws Exception {
    TableSchema schema =
        TableSchema.create(
            new TableId("appdb", "public", "widgets"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-11T00:00:00Z"));
    RecordingStream stream = new RecordingStream();
    PostgresPgoutputTransaction transaction =
        new PostgresPgoutputTransaction(
            "tx-1",
            PostgresLsn.parse("0/29"),
            PostgresLsn.parse("0/2A"),
            PostgresLsn.parse("0/2B"),
            PostgresLsn.parse("0/2B"),
            Instant.parse("2026-04-11T00:00:01Z"),
            List.of());

    try (H2RuntimeStateStore stateStore =
            new H2RuntimeStateStore(tempDir.resolve("postgres-live-runtime"));
        Connection sqlConnection =
            DriverManager.getConnection("jdbc:h2:mem:postgres_live_runtime;DB_CLOSE_DELAY=-1");
        PostgresLiveStreamingRuntime runtime =
            new PostgresLiveStreamingRuntime(
                sqlConnection,
                mock(Connection.class),
                "sourceA",
                "slot_sourcea",
                new PostgresSourceCheckpointStore(stateStore),
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
                new PostgresTransactionStreamingSession(
                    "appdb",
                    "test-run",
                    "slot_sourcea",
                    "sourceA",
                    List.of(schema),
                    stream,
                    new PostgresSourceCheckpointStore(stateStore)), NoopTap.INSTANCE)) {

      runtime.acknowledge(transaction);

      assertThat(runtime.lastAcknowledgedCheckpointDisplayValue()).isEqualTo("0/2B");
      assertThat(runtime.sourceFlowControlSnapshot().mode())
          .isEqualTo(SourceFlowControlSnapshot.Mode.DIRECT_POLL);
      assertThat(runtime.capturedTableCount()).isEqualTo(1);
      assertThat(stream.appliedLsn).isEqualTo(PostgresLsn.parse("0/2B"));
      assertThat(stream.flushedLsn).isEqualTo(PostgresLsn.parse("0/2B"));
    }
  }

  @Test
  void cachesMetadataInitializationAndSkipsHeartbeatWritesUntilDue() throws Exception {
    TableSchema schema =
        TableSchema.create(
            new TableId("appdb", "public", "widgets"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-11T00:00:00Z"));

    try (H2RuntimeStateStore stateStore =
            new H2RuntimeStateStore(tempDir.resolve("postgres-live-runtime-heartbeat"));
        Connection sqlConnection =
            DriverManager.getConnection("jdbc:h2:mem:postgres_live_runtime_heartbeat;DB_CLOSE_DELAY=-1")) {
      PostgresSourceCheckpointStore checkpointStore = new PostgresSourceCheckpointStore(stateStore);
      RecordingWatermarkWriter watermarkWriter = new RecordingWatermarkWriter();
      RecordingHeartbeatWriter heartbeatWriter = new RecordingHeartbeatWriter(true);
      try (PostgresLiveStreamingRuntime runtime =
          new PostgresLiveStreamingRuntime(
              sqlConnection,
              mock(Connection.class),
              "sourceA",
              "slot_sourcea",
              checkpointStore,
              watermarkWriter,
              heartbeatWriter,
              new PostgresTransactionStreamingSession(
                  "appdb",
                  "test-run",
                  "slot_sourcea",
                  "sourceA",
                  List.of(schema),
                  new RecordingStream(),
                  checkpointStore), NoopTap.INSTANCE)) {
      runtime.executeWithinWatermarkWindow((connection, window) -> "ok");
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

  private static final class RecordingStream implements PostgresPgoutputStream {
    private PostgresLsn appliedLsn;
    private PostgresLsn flushedLsn;

    @Override
    public java.util.Optional<java.nio.ByteBuffer> readPending() {
      return java.util.Optional.empty();
    }

    @Override
    public java.util.Optional<PostgresLsn> lastReceiveLsn() {
      return java.util.Optional.empty();
    }

    @Override
    public void setAppliedLsn(PostgresLsn lsn) {
      appliedLsn = lsn;
    }

    @Override
    public void setFlushedLsn(PostgresLsn lsn) {
      flushedLsn = lsn;
    }

    @Override
    public void forceUpdateStatus() {}

    @Override
    public void close() {}
  }
}
