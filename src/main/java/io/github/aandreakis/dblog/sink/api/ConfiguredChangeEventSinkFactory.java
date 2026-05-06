package io.github.aandreakis.dblog.sink.api;

import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfigValidator;
import io.github.aandreakis.dblog.config.DbLogProperties;
import io.github.aandreakis.dblog.config.DbLogTableMappingProperties;
import io.github.aandreakis.dblog.config.DbLogTargetProperties;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.runtime.host.RuntimeFailureClassifier;
import io.github.aandreakis.dblog.runtime.host.RuntimeFailureDisposition;
import io.github.aandreakis.dblog.runtime.observer.DbLogRuntimeObservability;
import io.github.aandreakis.dblog.sink.jdbc.JdbcApplyChangeEventSink;
import io.github.aandreakis.dblog.sink.jdbc.JdbcApplyTargetDialect;
import io.github.aandreakis.dblog.sink.jdbc.JdbcApplyTargetPreflight;
import io.github.aandreakis.dblog.sink.jdbc.JdbcTypedChangeEventSink;
import io.github.aandreakis.dblog.sink.jdbc.TargetTableResolver;
import io.github.aandreakis.dblog.sink.ndjson.NdjsonChangeEventSink;
import io.github.aandreakis.dblog.tap.NoopTap;
import io.github.aandreakis.dblog.tap.Tap;
import io.github.aandreakis.dblog.tap.TappingChangeEventSink;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Sink-owned composition for configured DBLog output sinks. */
public final class ConfiguredChangeEventSinkFactory {
  private ConfiguredChangeEventSinkFactory() {}

  public static ChangeEventSink configuredChangeEventSink(
      DbLogProperties properties,
      MeterRegistry meterRegistry,
      DbLogRuntimeObservability observability) {
    return configuredChangeEventSink(properties, List.of(), meterRegistry, observability);
  }

  /**
   * Wraps the configured sink chain so each delegate emits {@code tap.onSinkEvent(event,
   * sinkName)} after a successful append. The returned sink is a drop-in replacement for the one
   * the factory produced. Pass {@link NoopTap#INSTANCE} (or null) to return the sink unchanged.
   */
  public static ChangeEventSink withTap(ChangeEventSink sink, Tap tap) {
    Objects.requireNonNull(sink, "sink");
    if (tap == null || tap == NoopTap.INSTANCE) {
      return sink;
    }
    return wrapWithTap(sink, tap);
  }

  private static ChangeEventSink wrapWithTap(ChangeEventSink sink, Tap tap) {
    if (sink instanceof CompositeConfiguredChangeEventSink composite) {
      List<ChangeEventSink> wrapped = new ArrayList<>(composite.delegates.size());
      for (ChangeEventSink delegate : composite.delegates) {
        wrapped.add(wrapSingle(delegate, tap));
      }
      return new CompositeConfiguredChangeEventSink(wrapped);
    }
    return wrapSingle(sink, tap);
  }

  private static ChangeEventSink wrapSingle(ChangeEventSink delegate, Tap tap) {
    return new TappingChangeEventSink(delegate, canonicalSinkName(delegate), tap);
  }

  private static String canonicalSinkName(ChangeEventSink sink) {
    String className = sink.getClass().getSimpleName();
    String lower = className.toLowerCase(Locale.ROOT);
    if (lower.contains("ndjson")) {
      return "ndjson";
    }
    if (lower.contains("typed") && lower.contains("h2")) {
      return "typed_h2";
    }
    if (lower.contains("jdbc") || lower.contains("target")) {
      return "jdbc";
    }
    if (lower.contains("noop")) {
      return "noop";
    }
    return lower;
  }

  public static ChangeEventSink configuredChangeEventSink(
      DbLogProperties properties,
      RelationalSourceConfig sourceConfig,
      MeterRegistry meterRegistry,
      DbLogRuntimeObservability observability) {
    DbLogProperties requiredProperties = Objects.requireNonNull(properties, "properties");
    RelationalSourceConfig requiredSourceConfig = Objects.requireNonNull(sourceConfig, "sourceConfig");
    DbLogRuntimeObservability requiredObservability =
        Objects.requireNonNull(observability, "observability");

    DbLogProperties.Sink sink = requiredProperties.getSink();
    List<ChangeEventSink> delegates = new ArrayList<>();
    if (sink.getNdjson().isStdout()) {
      delegates.add(NdjsonChangeEventSink.stdout());
    }
    if (sink.getNdjson().getPath() != null) {
      delegates.add(NdjsonChangeEventSink.forFile(sink.getNdjson().getPath()));
    }
    if (sink.getTypedH2().getPath() != null) {
      delegates.add(JdbcTypedChangeEventSink.forH2(sink.getTypedH2().getPath()));
    }
    if (sink.getNoop().isEnabled()) {
      delegates.add(NoOpChangeEventSink.instance());
    }

    DbLogTargetProperties target = requiredProperties.getTarget();
    if (target.isEnabled()) {
      if (target.getDialect() == null) {
        throw new IllegalArgumentException(
            "dblog.target.dialect must be configured when dblog.target.enabled=true");
      }
      TargetTableResolver targetTableResolver =
          configuredTargetTableResolver(target, requiredSourceConfig);
      delegates.add(
          new RetryingTargetChangeEventSink(
              requiredObservability,
              JdbcApplyChangeEventSink.forTarget(
                  JdbcApplyTargetDialect.from(target.getDialect().name()),
                  requireNonBlank(target.getJdbcUrl(), "dblog.target.jdbc-url"),
                  requireNonBlank(target.getUsername(), "dblog.target.username"),
                  target.getPassword() == null ? "" : target.getPassword(),
                  target.getMaximumPoolSize(),
                  target.getConnectionTimeout(),
                  targetTableResolver),
              target.getRetryBackoff(),
              target.getDialect().name().toLowerCase(Locale.ROOT)));
    }
    return compositeOf(delegates);
  }

  public static ChangeEventSink configuredChangeEventSink(
      DbLogProperties properties,
      List<TableSchema> capturedSchemas,
      MeterRegistry meterRegistry,
      DbLogRuntimeObservability observability) {
    DbLogProperties requiredProperties = Objects.requireNonNull(properties, "properties");
    Objects.requireNonNull(capturedSchemas, "capturedSchemas");
    DbLogRuntimeObservability requiredObservability =
        Objects.requireNonNull(observability, "observability");

    DbLogProperties.Sink sink = requiredProperties.getSink();
    List<ChangeEventSink> delegates = new ArrayList<>();
    if (sink.getNdjson().isStdout()) {
      delegates.add(NdjsonChangeEventSink.stdout());
    }
    if (sink.getNdjson().getPath() != null) {
      delegates.add(NdjsonChangeEventSink.forFile(sink.getNdjson().getPath()));
    }
    if (sink.getTypedH2().getPath() != null) {
      delegates.add(JdbcTypedChangeEventSink.forH2(sink.getTypedH2().getPath()));
    }
    if (sink.getNoop().isEnabled()) {
      delegates.add(NoOpChangeEventSink.instance());
    }

    DbLogTargetProperties target = requiredProperties.getTarget();
    if (target.isEnabled()) {
      if (target.getDialect() == null) {
        throw new IllegalArgumentException(
            "dblog.target.dialect must be configured when dblog.target.enabled=true");
      }
      TargetTableResolver targetTableResolver =
          configuredTargetTableResolver(target, List.copyOf(capturedSchemas));
      delegates.add(
          new RetryingTargetChangeEventSink(
              requiredObservability,
              JdbcApplyChangeEventSink.forTarget(
                  JdbcApplyTargetDialect.from(target.getDialect().name()),
                  requireNonBlank(target.getJdbcUrl(), "dblog.target.jdbc-url"),
                  requireNonBlank(target.getUsername(), "dblog.target.username"),
                  target.getPassword() == null ? "" : target.getPassword(),
                  target.getMaximumPoolSize(),
                  target.getConnectionTimeout(),
                  targetTableResolver),
              target.getRetryBackoff(),
              target.getDialect().name().toLowerCase(Locale.ROOT)));
    }
    return compositeOf(delegates);
  }

  public static void validateConfiguredTargetSink(
      DbLogProperties properties, List<TableSchema> capturedSchemas) {
    DbLogTargetProperties target = Objects.requireNonNull(properties, "properties").getTarget();
    if (!target.isEnabled()) {
      return;
    }
    if (target.getDialect() == null) {
      throw new IllegalArgumentException(
          "dblog.target.dialect must be configured when dblog.target.enabled=true");
    }
    JdbcApplyTargetPreflight.validate(
        JdbcApplyTargetDialect.from(target.getDialect().name()),
        requireNonBlank(target.getJdbcUrl(), "dblog.target.jdbc-url"),
        requireNonBlank(target.getUsername(), "dblog.target.username"),
        target.getPassword() == null ? "" : target.getPassword(),
        target.getConnectionTimeout(),
        List.copyOf(capturedSchemas),
        configuredTargetTableResolver(target, List.copyOf(capturedSchemas)));
  }

  static TargetTableResolver configuredTargetTableResolver(
      DbLogTargetProperties target, List<TableSchema> capturedSchemas) {
    Objects.requireNonNull(target, "target");
    List<DbLogTableMappingProperties> mappings = target.getTableMappings();
    if (mappings.isEmpty()) {
      return TargetTableResolver.identity();
    }
    if (capturedSchemas.isEmpty()) {
      throw new IllegalStateException(
          "dblog.target.table-mappings require captured schemas to resolve source tables");
    }
    Map<String, TableSchema> schemasByQualifiedName = new LinkedHashMap<>();
    for (TableSchema capturedSchema : capturedSchemas) {
      schemasByQualifiedName.put(
          sourceKey(capturedSchema.tableId().schemaName(), capturedSchema.tableId().tableName()),
          capturedSchema);
    }
    LinkedHashMap<TableId, TableId> overrides = new LinkedHashMap<>();
    for (DbLogTableMappingProperties mapping : mappings) {
      String sourceSchema = mapping.getSourceSchema().trim();
      String sourceTable = mapping.getSourceTable().trim();
      TableSchema capturedSchema =
          schemasByQualifiedName.get(sourceKey(sourceSchema, sourceTable));
      if (capturedSchema == null) {
        throw new IllegalStateException(
            "Target table mapping references a source table that is not captured: "
                + sourceSchema
                + "."
                + sourceTable);
      }
      TableId sourceTableId = capturedSchema.tableId();
      String targetSchema =
          mapping.getTargetSchema() == null || mapping.getTargetSchema().isBlank()
              ? sourceTableId.schemaName()
              : mapping.getTargetSchema().trim();
      String targetTable =
          mapping.getTargetTable() == null || mapping.getTargetTable().isBlank()
              ? sourceTableId.tableName()
              : mapping.getTargetTable().trim();
      overrides.put(
          sourceTableId,
          new TableId(sourceTableId.databaseName(), targetSchema, targetTable));
    }
    return TargetTableResolver.of(overrides);
  }

  static TargetTableResolver configuredTargetTableResolver(
      DbLogTargetProperties target, RelationalSourceConfig sourceConfig) {
    Objects.requireNonNull(target, "target");
    RelationalSourceConfig requiredSourceConfig = Objects.requireNonNull(sourceConfig, "sourceConfig");
    List<DbLogTableMappingProperties> mappings = target.getTableMappings();
    if (mappings.isEmpty()) {
      return TargetTableResolver.identity();
    }

    String sourceDatabase = sourceDatabaseName(requiredSourceConfig);
    Map<String, String> capturedTablesByQualifiedName = new LinkedHashMap<>();
    for (String capturedTable : requiredSourceConfig.capturedTables()) {
      String[] parts = parseCapturedTable(capturedTable);
      capturedTablesByQualifiedName.put(sourceKey(parts[0], parts[1]), capturedTable);
    }

    LinkedHashMap<TableId, TableId> overrides = new LinkedHashMap<>();
    for (DbLogTableMappingProperties mapping : mappings) {
      String sourceSchema = requireNonBlank(mapping.getSourceSchema(), "dblog.target.table-mappings.source-schema");
      String sourceTable = requireNonBlank(mapping.getSourceTable(), "dblog.target.table-mappings.source-table");
      if (!capturedTablesByQualifiedName.containsKey(sourceKey(sourceSchema, sourceTable))) {
        throw new IllegalStateException(
            "Target table mapping references a source table that is not captured: "
                + sourceSchema
                + "."
                + sourceTable);
      }
      TableId sourceTableId =
          sourceTableIdForMapping(requiredSourceConfig, sourceDatabase, sourceSchema, sourceTable);
      String targetSchema =
          mapping.getTargetSchema() == null || mapping.getTargetSchema().isBlank()
              ? sourceTableId.schemaName()
              : mapping.getTargetSchema().trim();
      String targetTable =
          mapping.getTargetTable() == null || mapping.getTargetTable().isBlank()
              ? sourceTableId.tableName()
              : mapping.getTargetTable().trim();
      overrides.put(
          sourceTableId,
          new TableId(sourceTableId.databaseName(), targetSchema, targetTable));
    }
    return TargetTableResolver.of(overrides);
  }

  private static ChangeEventSink compositeOf(List<ChangeEventSink> delegates) {
    List<ChangeEventSink> filtered = new ArrayList<>();
    for (ChangeEventSink delegate : delegates) {
      if (delegate != null) {
        filtered.add(delegate);
      }
    }
    if (filtered.isEmpty()) {
      throw new IllegalStateException(
          "No DBLog output sink is configured. Configure at least one real sink, or set "
              + "dblog.sink.noop.enabled=true to discard events explicitly.");
    }
    if (filtered.size() == 1) {
      return filtered.get(0);
    }
    return new CompositeConfiguredChangeEventSink(filtered);
  }

  static ChangeEventSink retryingTargetChangeEventSink(
      DbLogRuntimeObservability observability,
      ChangeEventSink delegate,
      Duration retryBackoff,
      String targetDialect) {
    return new RetryingTargetChangeEventSink(
        observability, delegate, retryBackoff, targetDialect);
  }

  private static String requireNonBlank(String value, String key) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("Required target property is missing: " + key);
    }
    return value;
  }

  private static String sourceKey(String schemaName, String tableName) {
    return schemaName.trim().toLowerCase(Locale.ROOT)
        + "."
        + tableName.trim().toLowerCase(Locale.ROOT);
  }

  private static String sourceDatabaseName(RelationalSourceConfig sourceConfig) {
    if (sourceConfig.databaseName() != null) {
      return sourceConfig.databaseName();
    }
    String jdbcUrl = requireNonBlank(sourceConfig.jdbcUrl(), "sourceConfig.jdbcUrl");
    if (jdbcUrl.startsWith("jdbc:mysql://")) {
      return RelationalSourceConfigValidator.databaseNameFromJdbcUrl(
          jdbcUrl, "jdbc:mysql://", "MySQL");
    }
    if (jdbcUrl.startsWith("jdbc:postgresql://")) {
      return RelationalSourceConfigValidator.databaseNameFromJdbcUrl(
          jdbcUrl, "jdbc:postgresql://", "PostgreSQL");
    }
    throw new IllegalArgumentException(
        "Could not infer source database name from JDBC URL for target table mappings: " + jdbcUrl);
  }

  private static TableId sourceTableIdForMapping(
      RelationalSourceConfig sourceConfig,
      String sourceDatabase,
      String sourceSchema,
      String sourceTable) {
    String jdbcUrl = requireNonBlank(sourceConfig.jdbcUrl(), "sourceConfig.jdbcUrl");
    if (jdbcUrl.startsWith("jdbc:mysql://")) {
      return new TableId(sourceConfig.sourceId(), sourceSchema, sourceTable);
    }
    return new TableId(sourceDatabase, sourceSchema, sourceTable);
  }

  private static String[] parseCapturedTable(String capturedTable) {
    String normalized = requireNonBlank(capturedTable, "capturedTable");
    String[] parts = normalized.split("\\.", 2);
    if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
      throw new IllegalArgumentException(
          "Captured tables must use adapter-native two-part names such as schema.table");
    }
    return new String[] {parts[0].trim(), parts[1].trim()};
  }

  private static Duration requirePositive(Duration value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isZero() || value.isNegative()) {
      throw new IllegalArgumentException(name + " must be > 0");
    }
    return value;
  }

  private static final class CompositeConfiguredChangeEventSink
      implements ContextualChangeEventSink, SinkSchemaValidator {
    private final List<ChangeEventSink> delegates;

    private CompositeConfiguredChangeEventSink(List<ChangeEventSink> delegates) {
      this.delegates = List.copyOf(delegates);
    }

    @Override
    public void appendEvents(List<ChangeEvent> events) {
      for (ChangeEventSink delegate : delegates) {
        delegate.appendEvents(events);
      }
    }

    @Override
    public void appendEvents(ChangeEventBatchContext context, List<ChangeEvent> events) {
      Objects.requireNonNull(context, "context");
      for (ChangeEventSink delegate : delegates) {
        if (delegate instanceof ContextualChangeEventSink contextual) {
          contextual.appendEvents(context, events);
        } else {
          delegate.appendEvents(events);
        }
      }
    }

    @Override
    public void requestStop() {
      for (ChangeEventSink delegate : delegates) {
        delegate.requestStop();
      }
    }

    // Forward the bootstrap-time schema announcement to any delegate that needs it (typed-h2
    // sink, jdbc apply sink). Delegates that don't implement SinkSchemaValidator are skipped.
    @Override
    public void validateCapturedSchemas(List<TableSchema> capturedSchemas) {
      for (ChangeEventSink delegate : delegates) {
        if (delegate instanceof SinkSchemaValidator validator) {
          validator.validateCapturedSchemas(capturedSchemas);
        }
      }
    }

    @Override
    public void close() throws Exception {
      Exception firstFailure = null;
      for (ChangeEventSink delegate : delegates) {
        try {
          delegate.close();
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

  private static final class RetryingTargetChangeEventSink
      implements ContextualChangeEventSink, SinkSchemaValidator {
    private static final Logger log = LoggerFactory.getLogger(RetryingTargetChangeEventSink.class);

    private final ChangeEventSink delegate;
    private final Duration retryBackoff;
    private final String targetDialect;
    private final DbLogRuntimeObservability observability;
    private final AtomicBoolean stopRequested = new AtomicBoolean(false);
    private final AtomicReference<Thread> activeAppenderThread = new AtomicReference<>();

    private RetryingTargetChangeEventSink(
        DbLogRuntimeObservability observability,
        ChangeEventSink delegate,
        Duration retryBackoff,
        String targetDialect) {
      this.observability = Objects.requireNonNull(observability, "observability");
      this.delegate = Objects.requireNonNull(delegate, "delegate");
      this.retryBackoff = requirePositive(retryBackoff, "retryBackoff");
      this.targetDialect = requireNonBlank(targetDialect, "targetDialect");
    }

    @Override
    public void appendEvents(List<ChangeEvent> events) {
      appendEvents(new ChangeEventBatchContext("unspecified", null), events);
    }

    // Forward bootstrap-time schema announcement to the wrapped sink so the typed-h2 sink (and
    // any other schema-aware sink we may wrap in the future) can build its mirror tables.
    @Override
    public void validateCapturedSchemas(List<TableSchema> capturedSchemas) {
      if (delegate instanceof SinkSchemaValidator validator) {
        validator.validateCapturedSchemas(capturedSchemas);
      }
    }

    @Override
    public void appendEvents(ChangeEventBatchContext context, List<ChangeEvent> events) {
      Objects.requireNonNull(context, "context");
      Objects.requireNonNull(events, "events");
      throwIfStopRequested(null);
      Thread currentThread = Thread.currentThread();
      activeAppenderThread.set(currentThread);
      try {
        while (true) {
          throwIfStopRequested(null);
          try {
            if (delegate instanceof ContextualChangeEventSink contextual) {
              contextual.appendEvents(context, events);
            } else {
              delegate.appendEvents(events);
            }
            observability.sinkUp();
            observability.sinkApplySucceeded(lastSourcePosition(events), Instant.now().toString());
            return;
          } catch (RuntimeException | Error failure) {
            throwIfStopRequested(failure);
            RuntimeFailureDisposition disposition = RuntimeFailureClassifier.classifySink(failure);
            if (disposition == RuntimeFailureDisposition.FAIL_HARD_CONTRACT_BREACH) {
              observability.sinkFailed(failure);
              throw failure;
            }
            observability.sinkRetrying(failure);
            log.warn(
                "Sink apply failed for {}; retrying every {} until the sink becomes reachable again",
                targetDialect,
                retryBackoff,
                failure);
            sleepBeforeRetry(failure);
          }
        }
      } finally {
        activeAppenderThread.compareAndSet(currentThread, null);
      }
    }

    @Override
    public void requestStop() {
      stopRequested.set(true);
      Thread thread = activeAppenderThread.get();
      if (thread != null) {
        thread.interrupt();
      }
      delegate.requestStop();
      observability.sinkInactive();
    }

    @Override
    public void close() throws Exception {
      requestStop();
      delegate.close();
    }

    private void sleepBeforeRetry(Throwable originalFailure) {
      try {
        Thread.sleep(retryBackoff.toMillis());
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw stoppedFailure(
            originalFailure,
            new IllegalStateException(
                "Sink retry loop was interrupted while waiting to retry", interrupted));
      }
    }

    private void throwIfStopRequested(Throwable originalFailure) {
      if (stopRequested.get()) {
        throw stoppedFailure(originalFailure, null);
      }
    }

    private IllegalStateException stoppedFailure(
        Throwable originalFailure, RuntimeException interruptionFailure) {
      String message =
          "Sink retry loop was stopped before the target became reachable again";
      IllegalStateException failure = new IllegalStateException(message);
      if (originalFailure != null) {
        failure.addSuppressed(originalFailure);
      }
      if (interruptionFailure != null) {
        failure.addSuppressed(interruptionFailure);
      }
      return failure;
    }

    private static String lastSourcePosition(List<ChangeEvent> events) {
      for (int index = events.size() - 1; index >= 0; index--) {
        ChangeEvent event = events.get(index);
        if (event.sourcePosition() != null) {
          return event.sourcePosition().displayValue();
        }
      }
      return null;
    }
  }
}
