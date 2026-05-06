package io.github.aandreakis.dblog.verification.scenario.mysql.internal;

import io.github.aandreakis.dblog.verification.scenario.ScenarioJdbcSupport;
import io.github.aandreakis.dblog.verification.scenario.ScenarioStore;
import io.github.aandreakis.dblog.verification.scenario.ScenarioTelemetry;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;

/** Background fault-thread helpers shared by the MySQL scenario runners. */
public final class MySqlScenarioFaultThreads {
  private MySqlScenarioFaultThreads() {}

  public static Thread startOptionalSqlConnectionClose(
      String adapterName,
      ScenarioStore scenarioStore,
      String scenarioId,
      Connection sqlConnection,
      Duration delay) {
    if (delay.isZero()) {
      return null;
    }
    return Thread.ofVirtual()
        .name("dblog-" + adapterName + "-scenario-sql-close")
        .start(
            () -> {
              ScenarioJdbcSupport.sleepQuietly(delay);
              try {
                ScenarioTelemetry.injectedFailure(
                    scenarioStore,
                    scenarioId,
                    "failure-injection",
                    "close-runtime-sql-connection",
                    "delay=" + delay);
                sqlConnection.close();
              } catch (Exception ex) {
                ScenarioTelemetry.record(
                    scenarioStore,
                    scenarioId,
                    "failure-injection",
                    "close-runtime-sql-connection-failed",
                    ex.toString());
              }
            });
  }

  public static Thread startOptionalCapturedSchemaAlteration(
      String adapterName,
      ScenarioStore scenarioStore,
      String scenarioId,
      String jdbcUrl,
      String username,
      String password,
      Duration delay,
      MySqlScenarioSchema scenarioSchema) {
    if (delay.isZero()) {
      return null;
    }
    return Thread.ofVirtual()
        .name("dblog-" + adapterName + "-scenario-schema-alter")
        .start(
            () -> {
              ScenarioJdbcSupport.sleepQuietly(delay);
              try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
                  Statement statement = connection.createStatement()) {
                statement.execute(
                    "ALTER TABLE `"
                        + scenarioSchema.databaseName()
                        + "`.`widgets` ADD COLUMN scenario_break_col INT NULL");
                ScenarioTelemetry.injectedFailure(
                    scenarioStore,
                    scenarioId,
                    "failure-injection",
                    "alter-captured-schema",
                    "table=" + scenarioSchema.widgets().tableId().displayName());
              } catch (Exception ex) {
                ScenarioTelemetry.record(
                    scenarioStore,
                    scenarioId,
                    "failure-injection",
                    "alter-captured-schema-failed",
                    ex.toString());
              }
            });
  }

  public static Thread startOptionalMetadataShapeAlteration(
      String adapterName,
      ScenarioStore scenarioStore,
      String scenarioId,
      String jdbcUrl,
      String username,
      String password,
      Duration delay) {
    if (delay.isZero()) {
      return null;
    }
    return Thread.ofVirtual()
        .name("dblog-" + adapterName + "-scenario-metadata-alter")
        .start(
            () -> {
              ScenarioJdbcSupport.sleepQuietly(delay);
              try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
                  Statement statement = connection.createStatement()) {
                statement.execute(
                    "ALTER TABLE dblog_meta.watermarks ADD COLUMN scenario_break_col INT NULL");
                ScenarioTelemetry.injectedFailure(
                    scenarioStore,
                    scenarioId,
                    "failure-injection",
                    "alter-metadata-shape",
                    "table=dblog_meta.watermarks");
              } catch (Exception ex) {
                ScenarioTelemetry.record(
                    scenarioStore,
                    scenarioId,
                    "failure-injection",
                    "alter-metadata-shape-failed",
                    ex.toString());
              }
            });
  }

  public static Thread startOptionalMetadataRowDeletion(
      String adapterName,
      ScenarioStore scenarioStore,
      String scenarioId,
      String jdbcUrl,
      String username,
      String password,
      Duration delay) {
    if (delay.isZero()) {
      return null;
    }
    return Thread.ofVirtual()
        .name("dblog-" + adapterName + "-scenario-metadata-delete")
        .start(
            () -> {
              ScenarioJdbcSupport.sleepQuietly(delay);
              try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
                  Statement statement = connection.createStatement()) {
                statement.execute("DELETE FROM dblog_meta.watermarks WHERE id = 1");
                ScenarioTelemetry.injectedFailure(
                    scenarioStore,
                    scenarioId,
                    "failure-injection",
                    "delete-metadata-row",
                    "table=dblog_meta.watermarks id=1");
              } catch (Exception ex) {
                ScenarioTelemetry.record(
                    scenarioStore,
                    scenarioId,
                    "failure-injection",
                    "delete-metadata-row-failed",
                    ex.toString());
              }
            });
  }

  public static Thread startOptionalNullHeartbeatTimestampUpdate(
      String adapterName,
      ScenarioStore scenarioStore,
      String scenarioId,
      String jdbcUrl,
      String username,
      String password,
      Duration delay) {
    if (delay.isZero()) {
      return null;
    }
    return Thread.ofVirtual()
        .name("dblog-" + adapterName + "-scenario-heartbeat-null")
        .start(
            () -> {
              ScenarioJdbcSupport.sleepQuietly(delay);
              try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
                  PreparedStatement validUpdate =
                      connection.prepareStatement(
                          "UPDATE `dblog_meta`.`heartbeats` SET `last_beat_at` = ? WHERE `id` = 1");
                  PreparedStatement nullUpdate =
                      connection.prepareStatement(
                          "UPDATE `dblog_meta`.`heartbeats` SET `last_beat_at` = ? WHERE `id` = 1")) {
                validUpdate.setTimestamp(1, java.sql.Timestamp.from(Instant.now()));
                validUpdate.executeUpdate();
                ScenarioTelemetry.injectedFailure(
                    scenarioStore,
                    scenarioId,
                    "failure-injection",
                    "write-heartbeat",
                    "table=dblog_meta.heartbeats id=1");
                ScenarioJdbcSupport.sleepQuietly(Duration.ofMillis(50));
                nullUpdate.setNull(1, java.sql.Types.TIMESTAMP);
                nullUpdate.executeUpdate();
                ScenarioTelemetry.injectedFailure(
                    scenarioStore,
                    scenarioId,
                    "failure-injection",
                    "null-heartbeat-timestamp",
                    "table=dblog_meta.heartbeats id=1");
              } catch (Exception ex) {
                ScenarioTelemetry.record(
                    scenarioStore,
                    scenarioId,
                    "failure-injection",
                    "null-heartbeat-timestamp-failed",
                    ex.toString());
              }
            });
  }
}
