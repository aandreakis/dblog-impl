package io.github.aandreakis.dblog.adapter.postgres.internal;

import io.github.aandreakis.dblog.adapter.api.HeartbeatMetadataWriter;
import io.github.aandreakis.dblog.adapter.api.OpenedSourceRuntime;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfigValidator;
import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.api.WatermarkMetadataWriter;
import io.github.aandreakis.dblog.adapter.postgres.PostgresDialect;
import io.github.aandreakis.dblog.adapter.postgres.PostgresLiveStreamingRuntime;
import io.github.aandreakis.dblog.adapter.postgres.PostgresLsn;
import io.github.aandreakis.dblog.adapter.postgres.PostgresPgoutputTransaction;
import io.github.aandreakis.dblog.adapter.postgres.PostgresSourceCheckpointStore;
import io.github.aandreakis.dblog.config.PostgresResourceOwnership;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.reconcile.HeartbeatMetadata;
import io.github.aandreakis.dblog.core.reconcile.WatermarkMetadata;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.tap.Tap;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** Builds the first real stream-fed PostgreSQL runtime path. */
public final class PostgresLiveRuntimeFactory {
  private static final int POSTGRES_IDENTIFIER_MAX_LENGTH = 63;

  private final PostgresPgoutputStreamFactory streamFactory;
  private final PostgresReplicationResourcesPreflight replicationResourcesPreflight;
  private final JdbcPostgresReplicaIdentityInspector replicaIdentityInspector;
  private final PostgresReplicaIdentityPolicy replicaIdentityPolicy;

  public PostgresLiveRuntimeFactory() {
    this(new JdbcPostgresPgoutputStreamFactory(), new PostgresReplicationResourcesPreflight());
  }

  public PostgresLiveRuntimeFactory(
      PostgresPgoutputStreamFactory streamFactory,
      PostgresReplicationResourcesPreflight replicationResourcesPreflight) {
    this(
        streamFactory,
        replicationResourcesPreflight,
        new JdbcPostgresReplicaIdentityInspector(),
        new PostgresReplicaIdentityPolicy());
  }

  public PostgresLiveRuntimeFactory(
      PostgresPgoutputStreamFactory streamFactory,
      PostgresReplicationResourcesPreflight replicationResourcesPreflight,
      JdbcPostgresReplicaIdentityInspector replicaIdentityInspector,
      PostgresReplicaIdentityPolicy replicaIdentityPolicy) {
    this.streamFactory = Objects.requireNonNull(streamFactory, "streamFactory");
    this.replicationResourcesPreflight =
        Objects.requireNonNull(replicationResourcesPreflight, "replicationResourcesPreflight");
    this.replicaIdentityInspector =
        Objects.requireNonNull(replicaIdentityInspector, "replicaIdentityInspector");
    this.replicaIdentityPolicy =
        Objects.requireNonNull(replicaIdentityPolicy, "replicaIdentityPolicy");
  }

  /**
   * Open the live pgoutput runtime using a pre-opened pair of connections. Ownership of both
   * connections transfers to the returned runtime on success; on failure neither connection is
   * closed here — the caller still owns them and must close via {@link
   * io.github.aandreakis.dblog.adapter.api.SourceConnections#close()}.
   */
  public OpenedSourceRuntime<PostgresPgoutputTransaction> open(
      RelationalSourceConfig config,
      Connection sqlConnection,
      Connection replicationConnection,
      List<TableSchema> contractSchemas,
      SourceChunkReader chunkReader,
      WatermarkMetadataWriter watermarkWriter,
      HeartbeatMetadataWriter heartbeatWriter,
      PostgresSourceCheckpointStore checkpointStore,
      Tap tap)
      throws Exception {
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(sqlConnection, "sqlConnection");
    Objects.requireNonNull(replicationConnection, "replicationConnection");
    Objects.requireNonNull(contractSchemas, "contractSchemas");
    Objects.requireNonNull(chunkReader, "chunkReader");
    Objects.requireNonNull(watermarkWriter, "watermarkWriter");
    Objects.requireNonNull(heartbeatWriter, "heartbeatWriter");
    Objects.requireNonNull(checkpointStore, "checkpointStore");
    Objects.requireNonNull(tap, "tap");

    String databaseName =
        config.databaseName() == null
            ? RelationalSourceConfigValidator.databaseNameFromJdbcUrl(
                config.jdbcUrl(), PostgresDialect.JDBC_PREFIX, PostgresDialect.DISPLAY_NAME)
            : config.databaseName();
    String publicationName =
        stringOption(
            config.options(),
            "postgres.publicationName",
            "pub_" + sanitizeIdentifier(config.sourceId()));
    String slotName =
        stringOption(
            config.options(),
            "postgres.slotName",
            "slot_" + sanitizeIdentifier(config.sourceId()));
    Duration statusInterval =
        durationOption(config.options(), "postgres.statusInterval", Duration.ofSeconds(10));
    PostgresResourceOwnership publicationOwnership =
        ownershipOption(
            config.options(), "postgres.publicationOwnership", PostgresResourceOwnership.DBLOG_MANAGED);
    PostgresResourceOwnership slotOwnership =
        ownershipOption(
            config.options(), "postgres.slotOwnership", PostgresResourceOwnership.DBLOG_MANAGED);
    boolean slotFailover = booleanOption(config.options(), "postgres.slotFailover", false);

    // PostgreSQL does not need a separate bootstrap-resume position: if the slot was created
    // successfully, its own restart_lsn is the durable first-run position. Contrast MySQL, where
    // the binlog has no equivalent property and the adapter therefore persists an explicit
    // bootstrap resume.
    PostgresLsn loadedCheckpoint = checkpointStore.load(config.sourceId()).orElse(null);
    PostgresLsn startLsn = loadedCheckpoint;

    // Startup preflight: all captured tables must use REPLICA IDENTITY FULL before we commit to
    // opening the replication slot. Deferring this to the first RELATION message on the stream
    // would leak partial events; this gate intentionally runs up front.
    for (TableSchema schema : contractSchemas) {
      TableId tableId = schema.tableId();
      replicaIdentityPolicy.requireReplicaIdentityFull(
          replicaIdentityInspector
              .readReplicaIdentity(sqlConnection, tableId)
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "captured PostgreSQL table is missing or inaccessible at replica-identity"
                              + " preflight: "
                              + tableId.displayName())));
    }

    watermarkWriter.ensureMetadataTable(sqlConnection);
    heartbeatWriter.ensureHeartbeatTable(sqlConnection);
    PostgresReplicationResourcesResult replicationResources =
        replicationResourcesPreflight.inspectAndEnsure(
            sqlConnection,
            new PostgresReplicationResourcesRequest(
                databaseName,
                new PostgresPublicationConfig(
                    databaseName,
                    publicationName,
                    publicationTables(databaseName, contractSchemas),
                    publicationOwnership),
                new PostgresReplicationSlotConfig(
                    slotName,
                    databaseName,
                    "pgoutput",
                    false,
                    false,
                    slotFailover,
                    slotOwnership)));
    validateStartLsnAvailability(loadedCheckpoint, replicationResources.slot(), config.sourceId(), checkpointStore);

    PostgresPgoutputStream stream =
        streamFactory.open(
            replicationConnection,
            new PostgresPgoutputStreamRequest(
                slotName, publicationName, startLsn, statusInterval));
    try {
      String runId = UUID.randomUUID().toString();
      String sourceStreamId = slotName;
      PostgresTransactionStreamingSession session =
          new PostgresTransactionStreamingSession(
              databaseName,
              runId,
              sourceStreamId,
              config.sourceId(),
              contractSchemas,
              stream,
              checkpointStore);
      return new OpenedSourceRuntime<>(
          new PostgresLiveStreamingRuntime(
              runId,
              sqlConnection,
              replicationConnection,
              config.sourceId(),
              sourceStreamId,
              checkpointStore,
              watermarkWriter,
              heartbeatWriter,
              session,
              tap),
          chunkReader,
          loadedCheckpoint == null ? null : loadedCheckpoint.displayValue());
    } catch (Throwable failure) {
      try {
        stream.close();
      } catch (Exception closeFailure) {
        failure.addSuppressed(closeFailure);
      }
      throw failure;
    }
  }

  private static String stringOption(Map<String, String> options, String key, String defaultValue) {
    String raw = options.get(key);
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    return raw.trim();
  }

  private static Duration durationOption(
      Map<String, String> options, String key, Duration defaultValue) {
    String raw = options.get(key);
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    return Duration.parse(raw.trim());
  }

  private static boolean booleanOption(Map<String, String> options, String key, boolean defaultValue) {
    String raw = options.get(key);
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    return Boolean.parseBoolean(raw.trim());
  }

  private static PostgresResourceOwnership ownershipOption(
      Map<String, String> options, String key, PostgresResourceOwnership defaultValue) {
    String raw = options.get(key);
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    return PostgresResourceOwnership.valueOf(raw.trim().toUpperCase(Locale.ROOT));
  }

  private static List<TableId> publicationTables(String databaseName, List<TableSchema> contractSchemas) {
    LinkedHashSet<TableId> tables = new LinkedHashSet<>();
    for (TableSchema schema : contractSchemas) {
      tables.add(schema.tableId());
    }
    tables.add(WatermarkMetadata.tableIdFor(databaseName));
    tables.add(HeartbeatMetadata.tableIdFor(databaseName));
    return List.copyOf(new ArrayList<>(tables));
  }

  private static void validateStartLsnAvailability(
      PostgresLsn loadedCheckpoint,
      PostgresReplicationSlotState slotState,
      String sourceId,
      PostgresSourceCheckpointStore checkpointStore) {
    if (loadedCheckpoint == null || slotState.restartLsn().isEmpty()) {
      return;
    }
    if (loadedCheckpoint.compareTo(slotState.restartLsn().orElseThrow()) >= 0) {
      return;
    }
    String reason =
        "PostgreSQL logical slot no longer retains required checkpoint LSN "
            + loadedCheckpoint.displayValue()
            + "; full dump required because data loss was detected";
    checkpointStore.saveFullDumpRequiredSignal(sourceId, null, reason);
    throw new IllegalStateException(reason);
  }

  private static String sanitizeIdentifier(String value) {
    String sanitized =
        requireNonBlank(value, "value")
            .trim()
            .toLowerCase(Locale.ROOT)
            .replaceAll("[^a-z0-9_]", "_");
    if (sanitized.isBlank()) {
      return "runtime";
    }
    if (sanitized.length() > POSTGRES_IDENTIFIER_MAX_LENGTH) {
      return sanitized.substring(0, POSTGRES_IDENTIFIER_MAX_LENGTH);
    }
    return sanitized;
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
