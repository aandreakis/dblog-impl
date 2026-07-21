package io.github.aandreakis.dblog.sink.jdbc;

import io.github.aandreakis.dblog.DbLogRuntimeException;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.sink.api.ChangeEventSink;
import io.github.aandreakis.dblog.sink.api.SinkSchemaValidator;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.datasource.AbstractDataSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * JDBC-backed target-apply sink for explicit PostgreSQL/MySQL targets.
 *
 * <p>This applies one event batch inside one target transaction and uses exact table/schema names
 * from the neutral DBLog event model.
 */
public final class JdbcApplyChangeEventSink implements ChangeEventSink, SinkSchemaValidator {
  private static final Logger log = LoggerFactory.getLogger(JdbcApplyChangeEventSink.class);

  private static final JdbcApplyTargetSchemaInspector TARGET_SCHEMA_INSPECTOR =
      new JdbcApplyTargetSchemaInspector();

  /**
   * Guards the once-per-process WARN emitted the first time a batch collapses duplicate
   * primary-key rows. This is expected behaviour during the watermark algorithm's chunk
   * window (the same PK can appear as a binlog INSERT and as a SELECT-origin refresh row
   * within one batch) but operators should see it once so they understand the semantic.
   */
  private static final AtomicBoolean BATCH_PK_DEDUP_WARNED = new AtomicBoolean(false);

  private final JdbcApplyTargetDialect dialect;
  private final String driverClassName;
  private final String jdbcUrl;
  private final SqlConnectionSource connectionSource;
  private final DataSource dataSource;
  private final JdbcTemplate jdbcTemplate;
  private final TransactionTemplate transactionTemplate;
  private final TargetSchemaLookup targetSchemaLookup;
  private final TargetTableResolver targetTableResolver;
  private final Map<TableId, JdbcApplyTargetSchemaInspector.TargetTableMetadata> targetSchemaCache =
      new ConcurrentHashMap<>();
  private final Map<CompiledStatementPlanKey, CompiledStatementPlan> compiledPlanCache =
      new ConcurrentHashMap<>();
  private final Map<StatementKey, PreparedStatementCreatorFactory> statementFactoryCache =
      new ConcurrentHashMap<>();
  public static JdbcApplyChangeEventSink forTarget(
      JdbcApplyTargetDialect dialect,
      String jdbcUrl,
      String username,
      String password,
      int maximumPoolSize) {
    return forTarget(
        dialect,
        jdbcUrl,
        username,
        password,
        maximumPoolSize,
        Duration.ofSeconds(2),
        TargetTableResolver.identity());
  }

  public static JdbcApplyChangeEventSink forTarget(
      JdbcApplyTargetDialect dialect,
      String jdbcUrl,
      String username,
      String password,
      int maximumPoolSize,
      Duration connectionTimeout) {
    return forTarget(
        dialect,
        jdbcUrl,
        username,
        password,
        maximumPoolSize,
        connectionTimeout,
        TargetTableResolver.identity());
  }

  public static JdbcApplyChangeEventSink forTarget(
      JdbcApplyTargetDialect dialect,
      String jdbcUrl,
      String username,
      String password,
      int maximumPoolSize,
      Duration connectionTimeout,
      TargetTableResolver targetTableResolver) {
    Objects.requireNonNull(dialect, "dialect");
    String requiredJdbcUrl = requireNonBlank(jdbcUrl, "jdbcUrl");
    if (maximumPoolSize <= 0) {
      throw new IllegalArgumentException("maximumPoolSize must be > 0");
    }
    requirePositive(connectionTimeout, "connectionTimeout");
    return new JdbcApplyChangeEventSink(
        dialect,
        dialect.driverClassName(),
        requiredJdbcUrl,
        new HikariTargetConnectionPool(
            dialect.driverClassName(),
            requiredJdbcUrl,
            username,
            password,
            dialect.name().toLowerCase(java.util.Locale.ROOT) + "-target-apply",
            maximumPoolSize,
            connectionTimeout),
        targetTableResolver,
        TARGET_SCHEMA_INSPECTOR::inspect);
  }

  JdbcApplyChangeEventSink(
      JdbcApplyTargetDialect dialect,
      String driverClassName,
      String jdbcUrl,
      SqlConnectionSource connectionSource) {
    this(
        dialect,
        driverClassName,
        jdbcUrl,
        connectionSource,
        TargetTableResolver.identity(),
        TARGET_SCHEMA_INSPECTOR::inspect);
  }

  JdbcApplyChangeEventSink(
      JdbcApplyTargetDialect dialect,
      String driverClassName,
      String jdbcUrl,
      SqlConnectionSource connectionSource,
      TargetTableResolver targetTableResolver,
      TargetSchemaLookup targetSchemaLookup) {
    this.dialect = Objects.requireNonNull(dialect, "dialect");
    this.driverClassName = requireNonBlank(driverClassName, "driverClassName");
    this.jdbcUrl = requireNonBlank(jdbcUrl, "jdbcUrl");
    this.connectionSource = Objects.requireNonNull(connectionSource, "connectionSource");
    this.dataSource =
        connectionSource instanceof DataSource candidate
            ? candidate
            : new ConnectionSourceDataSource(connectionSource);
    this.jdbcTemplate = new JdbcTemplate(this.dataSource);
    DataSourceTransactionManager transactionManager =
        new DataSourceTransactionManager(this.dataSource);
    transactionManager.setRollbackOnCommitFailure(true);
    this.transactionTemplate = new TransactionTemplate(transactionManager);
    this.targetTableResolver = Objects.requireNonNull(targetTableResolver, "targetTableResolver");
    this.targetSchemaLookup = Objects.requireNonNull(targetSchemaLookup, "targetSchemaLookup");
    loadDriver(this.driverClassName);
  }

  @Override
  public void appendEvents(List<ChangeEvent> events) {
    Objects.requireNonNull(events, "events");
    List<ChangeEvent> applicableEvents = events.stream().filter(event -> !isControlEvent(event)).toList();
    if (applicableEvents.isEmpty()) {
      return;
    }
    transactionTemplate.executeWithoutResult(
        ignored ->
            jdbcTemplate.execute(
                (ConnectionCallback<Void>)
                    connection -> {
                      appendEventsInTransaction(connection, applicableEvents);
                      return null;
                    }));
  }

  @Override
  public void close() {
    try {
      connectionSource.close();
    } catch (Exception ex) {
      throw new IllegalStateException("Failed to close JDBC apply sink for " + jdbcUrl, ex);
    }
  }

  @Override
  public void validateCapturedSchemas(List<TableSchema> capturedSchemas) {
    Objects.requireNonNull(capturedSchemas, "capturedSchemas");
    try {
      jdbcTemplate.execute(
          (ConnectionCallback<Void>)
              connection -> {
                JdbcApplyTargetPreflight.validateTargetTables(
                    connection,
                    dialect,
                    capturedSchemas,
                    targetTableResolver,
                    (ignoredConnection, ignoredDialect, tableId) ->
                        targetSchemaLookup.inspect(connection, dialect, tableId));
                return null;
              });
    } catch (RuntimeException failure) {
      throw new DbLogRuntimeException(failure);
    }
  }

  private void appendEventsInTransaction(Connection connection, List<ChangeEvent> events) {
    StatementKey currentKey = null;
    List<PlannedStatement> currentPlans = new ArrayList<>();
    for (ChangeEvent event : events) {
      PlannedStatement plan = plan(connection, event);
      StatementKey planKey = plan.compiledPlan().key();
      if (currentKey != null && !currentKey.equals(planKey)) {
        executeBatch(connection, currentKey, currentPlans);
        currentPlans = new ArrayList<>();
      }
      currentKey = planKey;
      currentPlans.add(plan);
    }
    if (currentKey != null && !currentPlans.isEmpty()) {
      executeBatch(connection, currentKey, currentPlans);
    }
  }

  private void executeBatch(Connection connection, StatementKey key, List<PlannedStatement> plans) {
    List<PlannedStatement> batchPlans = dedupeByPrimaryKeyKeepingLast(key, plans);
    try {
      PreparedStatementCreatorFactory factory =
          statementFactoryCache.computeIfAbsent(
              key, ignored -> new PreparedStatementCreatorFactory(key.sql()));
      jdbcTemplate.execute(
          factory.newPreparedStatementCreator(List.of()),
          (PreparedStatementCallback<Void>)
              statement -> {
                for (PlannedStatement plan : batchPlans) {
                  bindValues(statement, plan);
                  statement.addBatch();
                }
                statement.executeBatch();
                return null;
              });
    } catch (DataAccessException ex) {
      throw TargetApplyFailures.operationFailed(
          dialect, jdbcUrl, key.tableId(), extractSqlException(ex));
    }
  }

  /**
   * Collapses plans that share a primary key within the same batch, keeping the last
   * event for each PK. This is required because a single sink batch can legitimately
   * contain multiple events for the same PK (e.g. a binlog INSERT and a SELECT-origin
   * refresh row from the active chunk), and Postgres 15+ rejects an
   * {@code INSERT ... ON CONFLICT DO UPDATE} where the same conflict target row would
   * be affected twice. The at-least-once delivery contract with last-write-wins
   * semantics makes collapsing safe: only the final event per PK is observable
   * downstream anyway.
   */
  private static List<PlannedStatement> dedupeByPrimaryKeyKeepingLast(
      StatementKey key, List<PlannedStatement> plans) {
    if (plans.size() <= 1) {
      return plans;
    }
    LinkedHashMap<ImmutableRowImage, PlannedStatement> byPrimaryKey =
        new LinkedHashMap<>(plans.size());
    for (PlannedStatement plan : plans) {
      byPrimaryKey.put(plan.event().primaryKey(), plan);
    }
    if (byPrimaryKey.size() == plans.size()) {
      return plans;
    }
    if (BATCH_PK_DEDUP_WARNED.compareAndSet(false, true)) {
      log.warn(
          "JDBC apply sink collapsed {} duplicate primary-key events down to {} rows in one"
              + " batch for table {}. This is expected during the watermark window when a"
              + " binlog event and a dump SELECT refresh row both target the same PK: the"
              + " sink keeps the last event for each PK (last-write-wins per spec §14).",
          plans.size() - byPrimaryKey.size(),
          byPrimaryKey.size(),
          key.tableId().displayName());
    }
    return new ArrayList<>(byPrimaryKey.values());
  }

  private PlannedStatement plan(Connection connection, ChangeEvent event) {
    Objects.requireNonNull(event, "event");
    Objects.requireNonNull(connection, "connection");
    return switch (event.operationType()) {
      case INSERT, UPDATE -> upsertPlan(connection, event);
      case DELETE -> deletePlan(connection, event);
      case WATERMARK, HEARTBEAT ->
          throw new IllegalStateException(
              "Control events should be filtered before reaching the JDBC apply sink");
    };
  }

  private PlannedStatement upsertPlan(Connection connection, ChangeEvent event) {
    TableId targetTableId = targetTableResolver.resolve(event.tableId());
    JdbcApplyTargetSchemaInspector.TargetTableMetadata targetSchema =
        targetSchema(connection, event.tableId(), targetTableId);
    ImmutableRowImage primaryKeyImage = event.primaryKey();
    ImmutableRowImage afterRowImage = requireAfterRowImage(event);
    List<String> primaryKeyColumns = orderedPrimaryKeyColumns(primaryKeyImage);
    List<String> nonPrimaryColumns = nonPrimaryUpsertColumns(event, primaryKeyImage, afterRowImage);
    CompiledStatementPlan compiledPlan =
        compiledUpsertPlan(targetTableId, targetSchema, primaryKeyColumns, nonPrimaryColumns);
    return new PlannedStatement(
        event, compiledPlan, plannedValues(event, compiledPlan, primaryKeyImage, afterRowImage));
  }

  private PlannedStatement deletePlan(Connection connection, ChangeEvent event) {
    TableId targetTableId = targetTableResolver.resolve(event.tableId());
    JdbcApplyTargetSchemaInspector.TargetTableMetadata targetSchema =
        targetSchema(connection, event.tableId(), targetTableId);
    ImmutableRowImage primaryKeyImage = event.primaryKey();
    List<String> primaryKeyColumns = orderedPrimaryKeyColumns(primaryKeyImage);
    CompiledStatementPlan compiledPlan =
        compiledDeletePlan(targetTableId, targetSchema, primaryKeyColumns);
    return new PlannedStatement(
        event, compiledPlan, plannedValues(event, compiledPlan, primaryKeyImage, null));
  }

  private CompiledStatementPlan compiledUpsertPlan(
      TableId targetTableId,
      JdbcApplyTargetSchemaInspector.TargetTableMetadata targetSchema,
      List<String> primaryKeyColumns,
      List<String> nonPrimaryColumns) {
    CompiledStatementPlanKey key =
        new CompiledStatementPlanKey(
            targetTableId, StatementKind.UPSERT, primaryKeyColumns, nonPrimaryColumns);
    return compiledPlanCache.computeIfAbsent(key, ignored -> compileUpsertPlan(targetSchema, key));
  }

  private CompiledStatementPlan compileUpsertPlan(
      JdbcApplyTargetSchemaInspector.TargetTableMetadata targetSchema,
      CompiledStatementPlanKey key) {
    ArrayList<CompiledBinding> bindings =
        new ArrayList<>(key.primaryKeyColumns().size() + key.nonPrimaryColumns().size());
    for (String primaryKeyColumn : key.primaryKeyColumns()) {
      bindings.add(
          new CompiledBinding(
              BindingSource.PRIMARY_KEY, primaryKeyColumn, targetSchema.requireColumn(primaryKeyColumn)));
    }
    for (String nonPrimaryColumn : key.nonPrimaryColumns()) {
      bindings.add(
          new CompiledBinding(
              BindingSource.AFTER_ROW, nonPrimaryColumn, targetSchema.requireColumn(nonPrimaryColumn)));
    }
    List<String> allColumns = bindings.stream().map(CompiledBinding::sourceColumnName).toList();
    List<String> valueExpressions =
        bindings.stream().map(binding -> valueExpression(binding.targetColumn())).toList();
    StatementKey statementKey =
        new StatementKey(
            key.tableId(),
            key.kind(),
            allColumns,
            dialect.upsertSql(
                key.tableId(), allColumns, key.primaryKeyColumns(), valueExpressions));
    return new CompiledStatementPlan(statementKey, bindings);
  }

  private CompiledStatementPlan compiledDeletePlan(
      TableId targetTableId,
      JdbcApplyTargetSchemaInspector.TargetTableMetadata targetSchema,
      List<String> primaryKeyColumns) {
    CompiledStatementPlanKey key =
        new CompiledStatementPlanKey(
            targetTableId, StatementKind.DELETE, primaryKeyColumns, primaryKeyColumns);
    return compiledPlanCache.computeIfAbsent(
        key,
        ignored -> {
          List<CompiledBinding> bindings =
              key.primaryKeyColumns().stream()
                  .map(
                      column ->
                          new CompiledBinding(
                              BindingSource.PRIMARY_KEY,
                              column,
                              targetSchema.requireColumn(column)))
                  .toList();
          StatementKey statementKey =
              new StatementKey(
                  key.tableId(),
                  key.kind(),
                  key.primaryKeyColumns(),
                  dialect.deleteSql(key.tableId(), key.primaryKeyColumns()));
          return new CompiledStatementPlan(statementKey, bindings);
        });
  }

  private static List<String> nonPrimaryUpsertColumns(
      ChangeEvent event, ImmutableRowImage primaryKeyImage, ImmutableRowImage afterRowImage) {
    ArrayList<String> nonPrimaryColumns = new ArrayList<>(afterRowImage.columnNames().size());
    for (int index = 0; index < afterRowImage.columnNames().size(); index++) {
      String columnName = afterRowImage.columnNames().get(index);
      int primaryKeyIndex = primaryKeyImage.indexOf(columnName);
      if (primaryKeyIndex >= 0) {
        if (!Objects.deepEquals(
            primaryKeyImage.valueAt(primaryKeyIndex), afterRowImage.valueAt(index))) {
          throw new IllegalStateException(
              "Event afterRow primary key does not match event.primaryKey for "
                  + event.tableId().displayName());
        }
        continue;
      }
      nonPrimaryColumns.add(columnName);
    }
    return List.copyOf(nonPrimaryColumns);
  }

  private static List<String> orderedPrimaryKeyColumns(ImmutableRowImage primaryKeyImage) {
    Objects.requireNonNull(primaryKeyImage, "primaryKeyImage");
    if (primaryKeyImage.isEmpty()) {
      throw new IllegalStateException("JDBC apply sink requires at least one primary-key column");
    }
    return primaryKeyImage.columnNames();
  }

  private void bindValues(PreparedStatement statement, PlannedStatement plan) throws SQLException {
    ChangeEvent event = plan.event();
    CompiledStatementPlan compiledPlan = plan.compiledPlan();
    for (int index = 0; index < plan.values().size(); index++) {
      CompiledBinding binding = compiledPlan.bindings().get(index);
      bindValue(
          statement,
          index + 1,
          plan.values().get(index),
          event.tableId(),
          compiledPlan.key().tableId(),
          binding.sourceColumnName(),
          binding.targetColumn());
    }
  }

  private static List<Object> plannedValues(
      ChangeEvent event,
      CompiledStatementPlan compiledPlan,
      ImmutableRowImage primaryKeyImage,
      ImmutableRowImage afterRowImage) {
    ArrayList<Object> values = new ArrayList<>(compiledPlan.bindings().size());
    for (CompiledBinding binding : compiledPlan.bindings()) {
      values.add(
          switch (binding.source()) {
            case PRIMARY_KEY ->
                requiredRowValue(event.tableId(), primaryKeyImage, binding.sourceColumnName(), "primaryKey");
            case AFTER_ROW ->
                requiredRowValue(
                    event.tableId(),
                    Objects.requireNonNull(afterRowImage, "afterRowImage"),
                    binding.sourceColumnName(),
                    "afterRow");
          });
    }
    return values;
  }

  private static Object requiredRowValue(
      TableId tableId, ImmutableRowImage rowImage, String columnName, String sourceLabel) {
    int index = rowImage.indexOf(columnName);
    if (index < 0) {
      throw new IllegalStateException(
          sourceLabel + " is missing required column " + columnName + " for " + tableId.displayName());
    }
    return rowImage.valueAt(index);
  }

  private static ImmutableRowImage requireAfterRowImage(ChangeEvent event) {
    ImmutableRowImage afterRowImage = event.afterRow();
    if (afterRowImage == null) {
      throw new IllegalStateException(
          "JDBC apply sink requires afterRow for " + event.operationType() + " events");
    }
    return afterRowImage;
  }

  private JdbcApplyTargetSchemaInspector.TargetTableMetadata targetSchema(
      Connection connection, TableId sourceTableId, TableId tableId) {
    return targetSchemaCache.computeIfAbsent(
        tableId, ignored -> loadTargetSchema(connection, sourceTableId, tableId));
  }

  private JdbcApplyTargetSchemaInspector.TargetTableMetadata loadTargetSchema(
      Connection connection, TableId sourceTableId, TableId tableId) {
    try {
      return targetSchemaLookup
          .inspect(connection, dialect, tableId)
          .orElseThrow(() -> TargetApplyFailures.missingTargetTable(sourceTableId, tableId));
    } catch (SQLException ex) {
      throw TargetApplyFailures.metadataLookupFailed(sourceTableId, tableId, ex);
    }
  }

  private String valueExpression(JdbcApplyTargetSchemaInspector.TargetColumnMetadata column) {
    if (dialect != JdbcApplyTargetDialect.POSTGRES) {
      return "?";
    }
    if (!requiresExplicitCast(column)) {
      return "?";
    }
    return "CAST(? AS " + quotedTypeReference(column) + ")";
  }

  private boolean requiresExplicitCast(JdbcApplyTargetSchemaInspector.TargetColumnMetadata column) {
    if ("e".equals(column.typeKind())) {
      return true;
    }
    return column.neutralType() == NeutralColumnType.JSON
        || column.neutralType() == NeutralColumnType.XML
        || column.neutralType() == NeutralColumnType.UUID;
  }

  private String quotedTypeReference(JdbcApplyTargetSchemaInspector.TargetColumnMetadata column) {
    String requiredTypeName = requireNonBlank(column.typeName(), "typeName");
    String typeSchema = column.typeSchema();
    if (typeSchema == null || typeSchema.isBlank() || "pg_catalog".equalsIgnoreCase(typeSchema)) {
      return dialect.quoteIdentifier(requiredTypeName);
    }
    return dialect.quoteIdentifier(typeSchema) + "." + dialect.quoteIdentifier(requiredTypeName);
  }

  private void bindValue(
      PreparedStatement statement,
      int parameterIndex,
      Object value,
      TableId sourceTableId,
      TableId targetTableId,
      String sourceColumnName,
      JdbcApplyTargetSchemaInspector.TargetColumnMetadata targetColumn)
      throws SQLException {
    Object coercedValue;
    try {
      coercedValue = TargetValueCoercions.coerce(value, targetColumn);
    } catch (RuntimeException failure) {
      throw TargetApplyFailures.valueCoercionFailed(
          sourceTableId, targetTableId, sourceColumnName, targetColumn, value, failure);
    }
    if (coercedValue == null) {
      statement.setObject(parameterIndex, null);
      return;
    }
    if (coercedValue instanceof String stringValue) {
      statement.setString(parameterIndex, stringValue);
      return;
    }
    if (coercedValue instanceof Boolean booleanValue) {
      statement.setBoolean(parameterIndex, booleanValue);
      return;
    }
    if (coercedValue instanceof Byte byteValue) {
      statement.setByte(parameterIndex, byteValue);
      return;
    }
    if (coercedValue instanceof Short shortValue) {
      statement.setShort(parameterIndex, shortValue);
      return;
    }
    if (coercedValue instanceof Integer integerValue) {
      statement.setInt(parameterIndex, integerValue);
      return;
    }
    if (coercedValue instanceof Long longValue) {
      statement.setLong(parameterIndex, longValue);
      return;
    }
    if (coercedValue instanceof Float floatValue) {
      statement.setFloat(parameterIndex, floatValue);
      return;
    }
    if (coercedValue instanceof Double doubleValue) {
      statement.setDouble(parameterIndex, doubleValue);
      return;
    }
    if (coercedValue instanceof BigInteger bigInteger) {
      if (bigInteger.bitLength() <= 63) {
        statement.setLong(parameterIndex, bigInteger.longValueExact());
      } else {
        statement.setBigDecimal(parameterIndex, new BigDecimal(bigInteger));
      }
      return;
    }
    if (coercedValue instanceof BigDecimal bigDecimal) {
      if (targetColumn.neutralType() == NeutralColumnType.FLOAT) {
        statement.setDouble(parameterIndex, bigDecimal.doubleValue());
        return;
      }
      statement.setBigDecimal(parameterIndex, bigDecimal);
      return;
    }
    if (coercedValue instanceof byte[] bytes) {
      statement.setBytes(parameterIndex, bytes);
      return;
    }
    if (coercedValue instanceof Instant instant) {
      if (dialect == JdbcApplyTargetDialect.POSTGRES) {
        statement.setObject(parameterIndex, OffsetDateTime.ofInstant(instant, ZoneOffset.UTC));
      } else {
        statement.setTimestamp(parameterIndex, Timestamp.from(instant));
      }
      return;
    }
    if (coercedValue instanceof LocalDate localDate) {
      statement.setDate(parameterIndex, Date.valueOf(localDate));
      return;
    }
    if (coercedValue instanceof LocalTime localTime) {
      statement.setObject(parameterIndex, localTime);
      return;
    }
    if (coercedValue instanceof LocalDateTime localDateTime) {
      statement.setTimestamp(parameterIndex, Timestamp.valueOf(localDateTime));
      return;
    }
    if (coercedValue instanceof UUID uuid) {
      if (dialect == JdbcApplyTargetDialect.POSTGRES) {
        statement.setObject(parameterIndex, uuid);
      } else {
        statement.setString(parameterIndex, uuid.toString());
      }
      return;
    }
    statement.setObject(parameterIndex, coercedValue);
  }

  private static boolean isControlEvent(ChangeEvent event) {
    return event.operationType() == OperationType.WATERMARK
        || event.operationType() == OperationType.HEARTBEAT;
  }

  private void loadDriver(String driverClassName) {
    try {
      Class.forName(driverClassName);
    } catch (ClassNotFoundException ex) {
      throw new IllegalStateException(
          "Required JDBC driver is not on the classpath: " + driverClassName, ex);
    }
  }

  private static SQLException extractSqlException(DataAccessException exception) {
    Throwable current = exception;
    while (current != null) {
      if (current instanceof SQLException sqlException) {
        return sqlException;
      }
      current = current.getCause();
    }
    return new SQLException(
        exception.getMessage() == null ? "Spring JDBC operation failed" : exception.getMessage(),
        exception);
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static Duration requirePositive(Duration value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isZero() || value.isNegative()) {
      throw new IllegalArgumentException(name + " must be > 0");
    }
    return value;
  }

  private static <T> List<T> immutableCopyAllowingNulls(List<T> values) {
    return Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(values, "values")));
  }

  private enum StatementKind {
    UPSERT,
    DELETE
  }

  private record StatementKey(TableId tableId, StatementKind kind, List<String> columns, String sql) {
    StatementKey {
      tableId = Objects.requireNonNull(tableId, "tableId");
      kind = Objects.requireNonNull(kind, "kind");
      columns = List.copyOf(Objects.requireNonNull(columns, "columns"));
      sql = requireNonBlank(sql, "sql");
      if (columns.isEmpty()) {
        throw new IllegalArgumentException("columns must not be empty");
      }
      if (new LinkedHashSet<>(columns).size() != columns.size()) {
        throw new IllegalArgumentException("columns must not contain duplicates: " + columns);
      }
    }
  }

  private record PlannedStatement(ChangeEvent event, CompiledStatementPlan compiledPlan, List<Object> values) {
    PlannedStatement {
      event = Objects.requireNonNull(event, "event");
      compiledPlan = Objects.requireNonNull(compiledPlan, "compiledPlan");
      values = immutableCopyAllowingNulls(values);
      if (values.size() != compiledPlan.bindings().size()) {
        throw new IllegalArgumentException(
            "planned statement values must align with compiled bindings: "
                + values.size()
                + " != "
                + compiledPlan.bindings().size());
      }
    }
  }

  private record CompiledStatementPlanKey(
      TableId tableId, StatementKind kind, List<String> primaryKeyColumns, List<String> nonPrimaryColumns) {
    CompiledStatementPlanKey {
      tableId = Objects.requireNonNull(tableId, "tableId");
      kind = Objects.requireNonNull(kind, "kind");
      primaryKeyColumns = List.copyOf(Objects.requireNonNull(primaryKeyColumns, "primaryKeyColumns"));
      nonPrimaryColumns = List.copyOf(Objects.requireNonNull(nonPrimaryColumns, "nonPrimaryColumns"));
      if (primaryKeyColumns.isEmpty()) {
        throw new IllegalArgumentException("primaryKeyColumns must not be empty");
      }
    }
  }

  private record CompiledStatementPlan(StatementKey key, List<CompiledBinding> bindings) {
    CompiledStatementPlan {
      key = Objects.requireNonNull(key, "key");
      bindings = List.copyOf(Objects.requireNonNull(bindings, "bindings"));
      if (bindings.isEmpty()) {
        throw new IllegalArgumentException("bindings must not be empty");
      }
    }
  }

  private record CompiledBinding(
      BindingSource source,
      String sourceColumnName,
      JdbcApplyTargetSchemaInspector.TargetColumnMetadata targetColumn) {
    CompiledBinding {
      source = Objects.requireNonNull(source, "source");
      sourceColumnName = requireNonBlank(sourceColumnName, "sourceColumnName");
      targetColumn = Objects.requireNonNull(targetColumn, "targetColumn");
    }
  }

  private enum BindingSource {
    PRIMARY_KEY,
    AFTER_ROW
  }

  @FunctionalInterface
  interface SqlConnectionSource extends AutoCloseable {
    Connection open() throws SQLException;

    @Override
    default void close() throws Exception {
      // default no-op
    }
  }

  @FunctionalInterface
  interface TargetSchemaLookup {
    Optional<JdbcApplyTargetSchemaInspector.TargetTableMetadata> inspect(
        Connection connection, JdbcApplyTargetDialect dialect, TableId tableId) throws SQLException;
  }

  @FunctionalInterface
  private interface SqlWork<T> {
    T execute(Connection connection) throws SQLException;
  }

  private static final class ConnectionSourceDataSource extends AbstractDataSource {
    private final SqlConnectionSource delegate;

    private ConnectionSourceDataSource(SqlConnectionSource delegate) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public Connection getConnection() throws SQLException {
      return delegate.open();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      return delegate.open();
    }
  }

}
