package io.github.aandreakis.dblog.adapter.postgres.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.postgres.PostgresLiveStreamingRuntime;
import io.github.aandreakis.dblog.adapter.postgres.PostgresLsn;
import io.github.aandreakis.dblog.adapter.postgres.PostgresSourceCheckpointStore;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import io.github.aandreakis.dblog.tap.NoopTap;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PostgresLiveRuntimeFactoryTests {
  @TempDir Path tempDir;

  @Test
  void buildsStreamRequestFromConfigAndLoadedCheckpoint() throws Exception {
    AtomicReference<PostgresPgoutputStreamRequest> capturedRequest = new AtomicReference<>();
    Connection replicationConnection = mock(Connection.class);
    PostgresLiveRuntimeFactory factory =
        new PostgresLiveRuntimeFactory(
            (connection, request) -> {
              assertThat(connection).isSameAs(replicationConnection);
              capturedRequest.set(request);
              return new PostgresPgoutputStream() {
                @Override
                public java.util.Optional<java.nio.ByteBuffer> readPending() {
                  return java.util.Optional.empty();
                }

                @Override
                public java.util.Optional<PostgresLsn> lastReceiveLsn() {
                  return java.util.Optional.empty();
                }

                @Override
                public void setAppliedLsn(PostgresLsn lsn) {}

                @Override
                public void setFlushedLsn(PostgresLsn lsn) {}

                @Override
                public void forceUpdateStatus() {}

                @Override
                public void close() {}
              };
            },
            new PostgresReplicationResourcesPreflight(
                new PostgresPublicationManager() {
                  @Override
                  public java.util.Optional<PostgresPublicationState> readPublication(
                      java.sql.Connection connection, String databaseName, String publicationName) {
                    return java.util.Optional.empty();
                  }

                  @Override
                  public PostgresPublicationState ensurePublication(
                      java.sql.Connection connection, PostgresPublicationConfig config) {
                    return new PostgresPublicationState(
                        config.databaseName(),
                        config.publicationName(),
                        false,
                        false,
                        true,
                        true,
                        true,
                        false,
                        false,
                        config.capturedTables().stream()
                            .map(tableId -> new PostgresPublicationTableState(tableId, false, false))
                            .toList());
                  }
                },
                new PostgresReplicationSlotManager() {
                  @Override
                  public java.util.Optional<PostgresReplicationSlotState> readSlot(
                      java.sql.Connection connection, String slotName) {
                    return java.util.Optional.empty();
                  }

                  @Override
                  public PostgresReplicationSlotState ensureLogicalSlot(
                      java.sql.Connection connection, PostgresReplicationSlotConfig config) {
                    return new PostgresReplicationSlotState(
                        config.slotName(),
                        "logical",
                        config.databaseName(),
                        config.pluginName(),
                        config.temporary(),
                        false,
                        config.twoPhase(),
                        config.failover(),
                        java.util.Optional.of(PostgresLsn.parse("0/2A")),
                        java.util.Optional.empty(),
                        "",
                        "");
                  }
                }),
            new JdbcPostgresReplicaIdentityInspector() {
              @Override
              public java.util.Optional<PostgresReplicaIdentityState> readReplicaIdentity(
                  java.sql.Connection connection,
                  io.github.aandreakis.dblog.core.model.TableId tableId) {
                return java.util.Optional.of(
                    new PostgresReplicaIdentityState(tableId, PostgresReplicaIdentity.FULL));
              }
            },
            new PostgresReplicaIdentityPolicy());
    TableSchema schema =
        TableSchema.create(
            new io.github.aandreakis.dblog.core.model.TableId("appdb", "public", "customers"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-11T00:00:00Z"));

    try (H2RuntimeStateStore stateStore =
            new H2RuntimeStateStore(tempDir.resolve("postgres-live-runtime-factory"));
        Connection sqlConnection =
            DriverManager.getConnection("jdbc:h2:mem:postgres_live_factory;DB_CLOSE_DELAY=-1")) {
      PostgresSourceCheckpointStore checkpointStore = new PostgresSourceCheckpointStore(stateStore);
      checkpointStore.save("sourceA", PostgresLsn.parse("0/2A"));

      var opened =
          factory.open(
              new RelationalSourceConfig(
                  "sourceA",
                  "jdbc:postgresql://127.0.0.1:5432/appdb",
                  "postgres",
                  "secret",
                  "appdb",
                  List.of("public.customers"),
                  Map.of(
                      "postgres.publicationName", "runtime_pub",
                      "postgres.slotName", "runtime_slot",
                      "postgres.statusInterval", "PT4S"),
                  false),
              sqlConnection,
              replicationConnection,
              List.of(schema),
              new JdbcPostgresChunkReader(),
              new JdbcPostgresWatermarkTableHelper(),
              new JdbcPostgresHeartbeatTableHelper(),
              checkpointStore, NoopTap.INSTANCE);

      assertThat(opened.runtime()).isInstanceOf(PostgresLiveStreamingRuntime.class);
      assertThat(opened.loadedCheckpointDisplayValue()).isEqualTo("0/2A");
      assertThat(capturedRequest.get()).isNotNull();
      assertThat(capturedRequest.get().slotName()).isEqualTo("runtime_slot");
      assertThat(capturedRequest.get().publicationName()).isEqualTo("runtime_pub");
      assertThat(capturedRequest.get().startLsn()).isEqualTo(PostgresLsn.parse("0/2A"));
      assertThat(capturedRequest.get().statusInterval()).isEqualTo(java.time.Duration.ofSeconds(4));
    }
  }

  /**
   * When the stream factory throws, the factory must NOT close the caller-provided replication
   * connection. Ownership stays with the caller (the adapter) until the runtime is successfully
   * returned; the adapter then closes via {@link
   * io.github.aandreakis.dblog.adapter.api.SourceConnections#close()}.
   */
  @Test
  void doesNotCloseCallerProvidedReplicationConnectionWhenStreamFactoryThrows() throws Exception {
    java.util.concurrent.atomic.AtomicBoolean replicationConnectionClosed =
        new java.util.concurrent.atomic.AtomicBoolean(false);
    Connection replicationConnection = mock(Connection.class);
    org.mockito.Mockito.doAnswer(
            invocation -> {
              replicationConnectionClosed.set(true);
              return null;
            })
        .when(replicationConnection)
        .close();
    PostgresLiveRuntimeFactory factory =
        new PostgresLiveRuntimeFactory(
            (connection, request) -> {
              throw new java.sql.SQLException("simulated stream-open failure");
            },
            new PostgresReplicationResourcesPreflight(
                new PostgresPublicationManager() {
                  @Override
                  public java.util.Optional<PostgresPublicationState> readPublication(
                      java.sql.Connection c, String d, String p) {
                    return java.util.Optional.empty();
                  }
                  @Override
                  public PostgresPublicationState ensurePublication(
                      java.sql.Connection c, PostgresPublicationConfig cfg) {
                    return new PostgresPublicationState(
                        cfg.databaseName(), cfg.publicationName(), false, false, true, true, true,
                        false, false,
                        cfg.capturedTables().stream()
                            .map(t -> new PostgresPublicationTableState(t, false, false))
                            .toList());
                  }
                },
                new PostgresReplicationSlotManager() {
                  @Override
                  public java.util.Optional<PostgresReplicationSlotState> readSlot(
                      java.sql.Connection c, String s) {
                    return java.util.Optional.empty();
                  }
                  @Override
                  public PostgresReplicationSlotState ensureLogicalSlot(
                      java.sql.Connection c, PostgresReplicationSlotConfig cfg) {
                    return new PostgresReplicationSlotState(
                        cfg.slotName(), "logical", cfg.databaseName(), cfg.pluginName(),
                        cfg.temporary(), false, cfg.twoPhase(), cfg.failover(),
                        java.util.Optional.of(PostgresLsn.parse("0/2A")),
                        java.util.Optional.empty(), "", "");
                  }
                }),
            new JdbcPostgresReplicaIdentityInspector() {
              @Override
              public java.util.Optional<PostgresReplicaIdentityState> readReplicaIdentity(
                  java.sql.Connection c, io.github.aandreakis.dblog.core.model.TableId t) {
                return java.util.Optional.of(
                    new PostgresReplicaIdentityState(t, PostgresReplicaIdentity.FULL));
              }
            },
            new PostgresReplicaIdentityPolicy());
    TableSchema schema =
        TableSchema.create(
            new io.github.aandreakis.dblog.core.model.TableId("appdb", "public", "customers"),
            List.of(
                new ColumnDefinition(
                    "id", "bigint", NeutralColumnType.INTEGER, true, false)),
            Instant.parse("2026-04-11T00:00:00Z"));

    try (H2RuntimeStateStore stateStore =
            new H2RuntimeStateStore(tempDir.resolve("stream-failure"));
        Connection sqlConnection =
            DriverManager.getConnection("jdbc:h2:mem:pg_stream_fail;DB_CLOSE_DELAY=-1")) {
      PostgresSourceCheckpointStore checkpointStore = new PostgresSourceCheckpointStore(stateStore);
      checkpointStore.save("sourceA", PostgresLsn.parse("0/2A"));

      org.assertj.core.api.Assertions.assertThatThrownBy(
              () ->
                  factory.open(
                      new RelationalSourceConfig(
                          "sourceA",
                          "jdbc:postgresql://127.0.0.1:5432/appdb",
                          "postgres",
                          "secret",
                          "appdb",
                          List.of("public.customers"),
                          Map.of("postgres.publicationName", "pub", "postgres.slotName", "slot"),
                          false),
                      sqlConnection,
                      replicationConnection,
                      List.of(schema),
                      new JdbcPostgresChunkReader(),
                      new JdbcPostgresWatermarkTableHelper(),
                      new JdbcPostgresHeartbeatTableHelper(),
                      checkpointStore, NoopTap.INSTANCE))
          .isInstanceOf(Exception.class);

      assertThat(replicationConnectionClosed.get())
          .as(
              "factory must not close the caller-provided replication connection; the caller owns"
                  + " it until a runtime is returned")
          .isFalse();
    }
  }
}
