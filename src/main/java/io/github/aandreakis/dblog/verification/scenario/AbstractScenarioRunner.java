package io.github.aandreakis.dblog.verification.scenario;

import io.github.aandreakis.dblog.controlplane.service.ControlPlaneEventCapture;
import io.github.aandreakis.dblog.core.checkpoint.CheckpointFlushPolicy;
import io.github.aandreakis.dblog.runtime.observer.DbLogRuntimeObservability;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/** Shared constructor/state holder for production scenario runners. */
public abstract class AbstractScenarioRunner<C extends ScenarioConfigView> implements ScenarioRunner {
  protected static final Duration DEFAULT_HEARTBEAT_INTERVAL = Duration.ofSeconds(5);

  protected final C config;
  protected final UnaryOperator<ScenarioStore> scenarioStoreDecorator;
  protected final CheckpointFlushPolicy checkpointFlushPolicy;
  protected final Duration heartbeatInterval;
  protected final MeterRegistry meterRegistry;
  protected final DbLogRuntimeObservability runtimeObservability;
  protected final ControlPlaneEventCapture eventCapture;

  protected AbstractScenarioRunner(C config) {
    this(
        config,
        UnaryOperator.identity(),
        CheckpointFlushPolicy.defaults(),
        DEFAULT_HEARTBEAT_INTERVAL,
        Metrics.globalRegistry,
        new DbLogRuntimeObservability(),
        new ControlPlaneEventCapture());
  }

  protected AbstractScenarioRunner(C config, UnaryOperator<ScenarioStore> scenarioStoreDecorator) {
    this(
        config,
        scenarioStoreDecorator,
        CheckpointFlushPolicy.defaults(),
        DEFAULT_HEARTBEAT_INTERVAL,
        Metrics.globalRegistry,
        new DbLogRuntimeObservability(),
        new ControlPlaneEventCapture());
  }

  protected AbstractScenarioRunner(
      C config,
      UnaryOperator<ScenarioStore> scenarioStoreDecorator,
      CheckpointFlushPolicy checkpointFlushPolicy,
      Duration heartbeatInterval,
      MeterRegistry meterRegistry,
      DbLogRuntimeObservability runtimeObservability) {
    this(
        config,
        scenarioStoreDecorator,
        checkpointFlushPolicy,
        heartbeatInterval,
        meterRegistry,
        runtimeObservability,
        new ControlPlaneEventCapture());
  }

  protected AbstractScenarioRunner(
      C config,
      UnaryOperator<ScenarioStore> scenarioStoreDecorator,
      CheckpointFlushPolicy checkpointFlushPolicy,
      Duration heartbeatInterval,
      MeterRegistry meterRegistry,
      DbLogRuntimeObservability runtimeObservability,
      ControlPlaneEventCapture eventCapture) {
    this.config = Objects.requireNonNull(config, "config");
    this.scenarioStoreDecorator =
        Objects.requireNonNull(scenarioStoreDecorator, "scenarioStoreDecorator");
    this.checkpointFlushPolicy =
        Objects.requireNonNull(checkpointFlushPolicy, "checkpointFlushPolicy");
    this.heartbeatInterval = Objects.requireNonNull(heartbeatInterval, "heartbeatInterval");
    this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry");
    this.runtimeObservability = Objects.requireNonNull(runtimeObservability, "runtimeObservability");
    this.eventCapture = Objects.requireNonNull(eventCapture, "eventCapture");
  }

  protected final ScenarioStore scenarioStore(
      Path sinkPath, Duration sinkDelay, Integer failSinkAfterAppendCount) {
    return ScenarioRunnerSupport.scenarioStore(
        sinkPath, sinkDelay, failSinkAfterAppendCount, scenarioStoreDecorator);
  }

  protected final void recordScenarioSuccess(
      ScenarioStore scenarioStore, String scenarioId, String detailMessage) {
    ScenarioRunnerSupport.recordScenarioSuccess(scenarioStore, scenarioId, detailMessage);
  }

  protected final void recordScenarioFailure(
      ScenarioStore scenarioStore, String scenarioId, Throwable failure) {
    ScenarioRunnerSupport.recordScenarioFailure(scenarioStore, scenarioId, failure);
  }

  protected final void cacheLatestSourceFlowControlSnapshot(
      Supplier<?> sourceFlowControlSupplier) {
    ScenarioRunnerSupport.cacheLatestSourceFlowControlSnapshot(
        runtimeObservability, sourceFlowControlSupplier);
  }

  protected final ScenarioConnections openScenarioConnections(
      SqlConnectionConfigurer connectionConfigurer)
      throws SQLException {
    Objects.requireNonNull(connectionConfigurer, "connectionConfigurer");
    Connection sqlConnection =
        DriverManager.getConnection(config.jdbcUrl(), config.username(), config.password());
    Connection sideConnection =
        DriverManager.getConnection(config.jdbcUrl(), config.username(), config.password());
    Connection verificationConnection =
        DriverManager.getConnection(config.jdbcUrl(), config.username(), config.password());
    boolean success = false;
    try {
      connectionConfigurer.configure(sqlConnection);
      connectionConfigurer.configure(sideConnection);
      connectionConfigurer.configure(verificationConnection);
      success = true;
      return new ScenarioConnections(sqlConnection, sideConnection, verificationConnection);
    } finally {
      if (!success) {
        closeQuietly(verificationConnection);
        closeQuietly(sideConnection);
        closeQuietly(sqlConnection);
      }
    }
  }

  private static void closeQuietly(Connection connection) {
    if (connection == null) {
      return;
    }
    try {
      connection.close();
    } catch (SQLException ignored) {
      // best effort during failed setup
    }
  }

  protected record ScenarioConnections(
      Connection sqlConnection,
      Connection sideConnection,
      Connection verificationConnection)
      implements AutoCloseable {
    @Override
    public void close() throws SQLException {
      SQLException failure = null;
      failure = closeConnection(verificationConnection, failure);
      failure = closeConnection(sideConnection, failure);
      failure = closeConnection(sqlConnection, failure);
      if (failure != null) {
        throw failure;
      }
    }

    private static SQLException closeConnection(Connection connection, SQLException failure) {
      try {
        connection.close();
      } catch (SQLException ex) {
        if (failure == null) {
          return ex;
        }
        failure.addSuppressed(ex);
      }
      return failure;
    }
  }

  @FunctionalInterface
  protected interface SqlConnectionConfigurer {
    void configure(Connection connection) throws SQLException;
  }
}
