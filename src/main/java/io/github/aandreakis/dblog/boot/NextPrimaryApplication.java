package io.github.aandreakis.dblog.boot;

import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.config.DbLogProperties;
import io.github.aandreakis.dblog.sink.api.ChangeEventSink;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Holder for the runtime / startup-check launch record types used by the Spring Boot entry point
 * ({@link DbLogImplApplication}).
 *
 * <p>This class is intentionally not itself an application entry point. The main class lives in
 * {@link DbLogImplApplication}; the record types below describe the inputs passed between
 * the Spring-wired runner and the runtime/startup support.
 */
public final class NextPrimaryApplication {
  private NextPrimaryApplication() {}

  record ControlPlaneConfig(
      boolean enabled,
      String host,
      int port,
      boolean allowNonLoopback,
      int executorMaxThreads,
      int executorQueueCapacity,
      int maxRequestBodyBytes,
      Path portFile) {
    ControlPlaneConfig {
      host = requireNonBlank(host, "dblog.control-plane.host");
      if (port < 0) {
        throw new IllegalArgumentException("dblog.control-plane.port must be >= 0");
      }
      if (executorMaxThreads <= 0) {
        throw new IllegalArgumentException("dblog.control-plane.executor-max-threads must be > 0");
      }
      if (executorQueueCapacity <= 0) {
        throw new IllegalArgumentException("dblog.control-plane.executor-queue-capacity must be > 0");
      }
      if (maxRequestBodyBytes <= 0) {
        throw new IllegalArgumentException("dblog.control-plane.max-request-body-bytes must be > 0");
      }
    }
  }

  record RuntimeLaunchPlan(
      String adapterKey,
      DbLogProperties properties,
      RelationalSourceConfig sourceConfig,
      Path statePath,
      int chunkSize,
      ControlPlaneConfig controlPlane) {
    RuntimeLaunchPlan {
      adapterKey = requireNonBlank(adapterKey, "adapterKey");
      properties = Objects.requireNonNull(properties, "properties");
      sourceConfig = Objects.requireNonNull(sourceConfig, "sourceConfig");
      statePath = Objects.requireNonNull(statePath, "statePath");
      if (chunkSize <= 0) {
        throw new IllegalArgumentException("chunkSize must be > 0");
      }
      controlPlane = Objects.requireNonNull(controlPlane, "controlPlane");
    }
  }

  record RuntimeLaunchContext(
      SourceAdapterRegistry adapterRegistry,
      String adapterKey,
      RelationalSourceConfig sourceConfig,
      H2RuntimeStateStore stateStore,
      ChangeEventSink sink,
      MeterRegistry meterRegistry,
      int chunkSize,
      ControlPlaneConfig controlPlane) {
    RuntimeLaunchContext {
      adapterRegistry = Objects.requireNonNull(adapterRegistry, "adapterRegistry");
      adapterKey = requireNonBlank(adapterKey, "adapterKey");
      sourceConfig = Objects.requireNonNull(sourceConfig, "sourceConfig");
      stateStore = Objects.requireNonNull(stateStore, "stateStore");
      sink = Objects.requireNonNull(sink, "sink");
      if (chunkSize <= 0) {
        throw new IllegalArgumentException("chunkSize must be > 0");
      }
      controlPlane = Objects.requireNonNull(controlPlane, "controlPlane");
    }
  }

  record StartupCheckLaunchPlan(
      String adapterKey,
      DbLogProperties properties,
      RelationalSourceConfig sourceConfig,
      Path statePath) {
    StartupCheckLaunchPlan {
      adapterKey = requireNonBlank(adapterKey, "adapterKey");
      properties = Objects.requireNonNull(properties, "properties");
      sourceConfig = Objects.requireNonNull(sourceConfig, "sourceConfig");
      statePath = Objects.requireNonNull(statePath, "statePath");
    }
  }

  record StartupCheckLaunchContext(
      String adapterKey,
      RelationalSourceConfig sourceConfig,
      H2RuntimeStateStore stateStore,
      ChangeEventSink sink,
      Path statePath) {
    StartupCheckLaunchContext {
      adapterKey = requireNonBlank(adapterKey, "adapterKey");
      sourceConfig = Objects.requireNonNull(sourceConfig, "sourceConfig");
      stateStore = Objects.requireNonNull(stateStore, "stateStore");
      sink = Objects.requireNonNull(sink, "sink");
      statePath = Objects.requireNonNull(statePath, "statePath");
    }
  }

  record SinkResources(ChangeEventSink sink, MeterRegistry meterRegistry) implements AutoCloseable {
    SinkResources {
      sink = Objects.requireNonNull(sink, "sink");
    }

    @Override
    public void close() throws Exception {
      Exception firstFailure = null;
      try {
        sink.close();
      } catch (Exception ex) {
        firstFailure = ex;
      }
      if (meterRegistry instanceof AutoCloseable closeable) {
        try {
          closeable.close();
        } catch (Exception ex) {
          if (firstFailure == null) {
            firstFailure = ex;
          } else {
            firstFailure.addSuppressed(ex);
          }
        }
      }
      if (firstFailure != null) {
        throw firstFailure;
      }
    }
  }

  static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  /** Returns {@code null} if the value is null or blank after trim; otherwise the trimmed value. */
  static String trimToNull(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }
}
