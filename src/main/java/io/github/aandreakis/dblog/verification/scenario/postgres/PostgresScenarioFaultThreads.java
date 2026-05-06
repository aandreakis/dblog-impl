package io.github.aandreakis.dblog.verification.scenario.postgres;

import io.github.aandreakis.dblog.verification.scenario.ScenarioExecutionException;
import io.github.aandreakis.dblog.verification.scenario.ScenarioStore;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Background fault-thread helpers for PostgreSQL scenario runs. */
public final class PostgresScenarioFaultThreads {
  private PostgresScenarioFaultThreads() {}

  public static Thread startOptionalReplicationTermination(
      PostgresScenarioConfig config, ScenarioStore scenarioStore) {
    if (config.terminateReplicationBackendAfter().isZero()) {
      return null;
    }
    return Thread.ofVirtual()
        .name("dblog-postgres-replication-killer")
        .start(
            () -> {
              sleepQuietly(config.terminateReplicationBackendAfter());
              try (Connection connection =
                      DriverManager.getConnection(
                          config.jdbcUrl(), config.username(), config.password());
                  Statement statement = connection.createStatement()) {
                List<Long> replicationBackendPids = new ArrayList<>();
                try (ResultSet resultSet =
                    statement.executeQuery("SELECT pid FROM pg_stat_replication ORDER BY pid")) {
                  while (resultSet.next()) {
                    replicationBackendPids.add(resultSet.getLong(1));
                  }
                }
                int killed = 0;
                for (long pid : replicationBackendPids) {
                  statement.execute("SELECT pg_terminate_backend(" + pid + ")");
                  killed++;
                }
                scenarioStore.recordTelemetry(
                    config.scenarioId(),
                    "failure-injection",
                    "terminate-replication-backend",
                    "killedReplicationBackends=" + killed);
              } catch (Exception ex) {
                scenarioStore.recordTelemetry(
                    config.scenarioId(),
                    "failure-injection",
                    "terminate-replication-backend-failed",
                    ex.toString());
              }
            });
  }

  public static Thread startOptionalSqlConnectionClose(
      PostgresScenarioConfig config, ScenarioStore scenarioStore, Connection sqlConnection) {
    if (config.closeRuntimeSqlConnectionAfter().isZero()) {
      return null;
    }
    return Thread.ofVirtual()
        .name("dblog-postgres-sql-connection-closer")
        .start(
            () -> {
              sleepQuietly(config.closeRuntimeSqlConnectionAfter());
              try {
                sqlConnection.close();
                scenarioStore.recordTelemetry(
                    config.scenarioId(),
                    "failure-injection",
                    "close-runtime-sql-connection",
                    "sqlConnection closed intentionally");
              } catch (Exception ex) {
                scenarioStore.recordTelemetry(
                    config.scenarioId(),
                    "failure-injection",
                    "close-runtime-sql-connection-failed",
                    ex.toString());
              }
            });
  }

  public static Thread startOptionalCapturedSchemaAlteration(
      PostgresScenarioConfig config,
      ScenarioStore scenarioStore,
      PostgresScenarioSchema scenarioSchema) {
    if (config.alterCapturedSchemaAfter().isZero()) {
      return null;
    }
    return Thread.ofVirtual()
        .name("dblog-postgres-schema-alterer")
        .start(
            () -> {
              sleepQuietly(config.alterCapturedSchemaAfter());
              try (Connection connection =
                      DriverManager.getConnection(
                          config.jdbcUrl(), config.username(), config.password());
                  Statement statement = connection.createStatement()) {
                statement.execute(
                    "ALTER TABLE \""
                        + scenarioSchema.schemaName()
                        + "\".\"widgets\" ADD COLUMN note TEXT");
                statement.execute(
                    "UPDATE \""
                        + scenarioSchema.schemaName()
                        + "\".\"widgets\" SET name = 'schema-drift-live' WHERE id = 2");
                scenarioStore.recordTelemetry(
                    config.scenarioId(),
                    "failure-injection",
                    "alter-captured-schema",
                    "added supported column note to "
                        + scenarioSchema.widgets().tableId().displayName());
              } catch (Exception ex) {
                scenarioStore.recordTelemetry(
                    config.scenarioId(),
                    "failure-injection",
                    "alter-captured-schema-failed",
                    ex.toString());
              }
            });
  }

  public static Thread startOptionalMetadataShapeAlteration(
      PostgresScenarioConfig config, ScenarioStore scenarioStore) {
    if (config.alterMetadataShapeAfter().isZero()) {
      return null;
    }
    return Thread.ofVirtual()
        .name("dblog-postgres-metadata-alterer")
        .start(
            () -> {
              sleepQuietly(config.alterMetadataShapeAfter());
              try (Connection connection =
                      DriverManager.getConnection(
                          config.jdbcUrl(), config.username(), config.password());
                  Statement statement = connection.createStatement()) {
                statement.execute("ALTER TABLE dblog_meta.watermarks ADD COLUMN extra TEXT");
                scenarioStore.recordTelemetry(
                    config.scenarioId(),
                    "failure-injection",
                    "alter-metadata-shape",
                    "added extra column to dblog_meta.watermarks");
              } catch (Exception ex) {
                scenarioStore.recordTelemetry(
                    config.scenarioId(),
                    "failure-injection",
                    "alter-metadata-shape-failed",
                    ex.toString());
              }
            });
  }

  public static Thread startOptionalMetadataRowDeletion(
      PostgresScenarioConfig config, ScenarioStore scenarioStore) {
    if (config.deleteMetadataRowAfter().isZero()) {
      return null;
    }
    return Thread.ofVirtual()
        .name("dblog-postgres-metadata-row-deleter")
        .start(
            () -> {
              sleepQuietly(config.deleteMetadataRowAfter());
              try (Connection connection =
                      DriverManager.getConnection(
                          config.jdbcUrl(), config.username(), config.password());
                  Statement statement = connection.createStatement()) {
                statement.execute("DELETE FROM dblog_meta.watermarks WHERE id = 1");
                scenarioStore.recordTelemetry(
                    config.scenarioId(),
                    "failure-injection",
                    "delete-metadata-row",
                    "deleted dblog_meta.watermarks singleton row");
              } catch (Exception ex) {
                scenarioStore.recordTelemetry(
                    config.scenarioId(),
                    "failure-injection",
                    "delete-metadata-row-failed",
                    ex.toString());
              }
            });
  }

  public static void sleepQuietly(Duration duration) {
    try {
      Thread.sleep(duration.toMillis());
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new ScenarioExecutionException("Interrupted while draining scenario runtime", ex);
    }
  }

  public static void joinQuietly(Thread thread) {
    if (thread == null) {
      return;
    }
    try {
      thread.join();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new ScenarioExecutionException(
          "Interrupted while waiting for scenario helper thread", ex);
    }
  }
}
