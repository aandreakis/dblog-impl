package io.github.aandreakis.dblog.adapter.postgres.internal;

import io.github.aandreakis.dblog.DbLogRuntimeException;
import io.github.aandreakis.dblog.adapter.api.AdapterConnectionSupport;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.SourcePreflight;
import io.github.aandreakis.dblog.adapter.postgres.PostgresDialect;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * PostgreSQL implementation of {@link SourcePreflight}. Idempotently ensures the configured
 * publication and logical replication slot exist (or are created, per the configured
 * ownership mode) by delegating to {@link PostgresReplicationResourcesPreflight} through a
 * short-lived SQL connection.
 *
 * <p>The result of {@code inspectAndEnsure} is discarded here — validating readiness without
 * returning adapter-specific state keeps the {@link SourcePreflight} contract adapter-neutral.
 * {@link PostgresLiveRuntimeFactory} still runs the same preflight when it opens the runtime
 * (needing the result for checkpoint-LSN availability validation); both invocations are
 * idempotent.
 */
public final class PostgresSourcePreflight implements SourcePreflight {
  private final PostgresReplicationResourcesPreflight replicationResourcesPreflight;
  private final Function<RelationalSourceConfig, Connection> connectionFactory;
  private final JdbcPostgresReplicaIdentityInspector replicaIdentityInspector;
  private final PostgresReplicaIdentityPolicy replicaIdentityPolicy;
  private final JdbcPostgresWatermarkTableHelper watermarkTableHelper;
  private final JdbcPostgresHeartbeatTableHelper heartbeatTableHelper;

  public PostgresSourcePreflight() {
    this(new PostgresReplicationResourcesPreflight(), PostgresSourcePreflight::openConnection);
  }

  public PostgresSourcePreflight(
      PostgresReplicationResourcesPreflight replicationResourcesPreflight,
      Function<RelationalSourceConfig, Connection> connectionFactory) {
    this.replicationResourcesPreflight =
        Objects.requireNonNull(replicationResourcesPreflight, "replicationResourcesPreflight");
    this.connectionFactory = Objects.requireNonNull(connectionFactory, "connectionFactory");
    this.replicaIdentityInspector = new JdbcPostgresReplicaIdentityInspector();
    this.replicaIdentityPolicy = new PostgresReplicaIdentityPolicy();
    this.watermarkTableHelper = new JdbcPostgresWatermarkTableHelper();
    this.heartbeatTableHelper = new JdbcPostgresHeartbeatTableHelper();
  }

  @Override
  public void ensure(RelationalSourceConfig config, List<TableSchema> contractSchemas) {
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(contractSchemas, "contractSchemas");
    try (Connection connection = connectionFactory.apply(config)) {
      for (TableSchema schema : contractSchemas) {
        var tableId = schema.tableId();
        replicaIdentityPolicy.requireReplicaIdentityFull(
            replicaIdentityInspector
                .readReplicaIdentity(connection, tableId)
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "captured PostgreSQL table is missing or inaccessible at preflight: "
                                + tableId.displayName())));
      }
      watermarkTableHelper.ensureMetadataTable(connection);
      heartbeatTableHelper.ensureHeartbeatTable(connection);
      replicationResourcesPreflight.inspectAndEnsure(
          connection,
          PostgresReplicationResourcesRequestFactory.build(config, contractSchemas));
    } catch (SQLException failure) {
      throw new DbLogRuntimeException(failure);
    }
  }

  private static Connection openConnection(RelationalSourceConfig config) {
    try {
      return AdapterConnectionSupport.openConfiguredSqlConnection(
          config, PostgresDialect.DISPLAY_NAME);
    } catch (SQLException failure) {
      throw new DbLogRuntimeException(failure);
    }
  }
}
