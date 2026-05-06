package io.github.aandreakis.dblog.sink.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class JdbcApplyTargetPreflightTests {
  @Test
  void returnsImmediatelyWhenNoCapturedSchemasAreProvided() {
    JdbcApplyTargetPreflight.validate(
        JdbcApplyTargetDialect.POSTGRES,
        " ",
        "ignored",
        "ignored",
        Duration.ofSeconds(2),
        List.of());
  }

  @Test
  void rejectsBlankJdbcUrlBeforeOpeningTargetResources() {
    assertThatThrownBy(
            () ->
                JdbcApplyTargetPreflight.validate(
                    JdbcApplyTargetDialect.POSTGRES,
                    " ",
                    "postgres",
                    "postgres",
                    Duration.ofSeconds(2),
                    List.of(sampleOrdersSchema())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("jdbcUrl must not be blank");
  }

  @Test
  void rejectsNonPositiveConnectionTimeoutBeforeOpeningTargetResources() {
    assertThatThrownBy(
            () ->
                JdbcApplyTargetPreflight.validate(
                    JdbcApplyTargetDialect.POSTGRES,
                    "jdbc:postgresql://db/app",
                    "postgres",
                    "postgres",
                    Duration.ZERO,
                    List.of(sampleOrdersSchema())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("connectionTimeout must be > 0");
  }

  @Test
  void passesConfiguredConnectionTimeoutIntoPreflightConnectionSource() {
    Connection connection = mock(Connection.class);
    TableSchema schema = sampleOrdersSchema();
    AtomicReference<Duration> observedTimeout = new AtomicReference<>();

    JdbcApplyTargetPreflight.validate(
        JdbcApplyTargetDialect.POSTGRES,
        "jdbc:postgresql://db/app",
        "postgres",
        "postgres",
        Duration.ofSeconds(4),
        List.of(schema),
        TargetTableResolver.identity(),
        (ignoredConnection, ignoredDialect, ignoredTableId) ->
            Optional.of(
                targetTableMetadata(
                    schema.tableId(),
                    List.of(
                        targetColumn("id", "bigint", NeutralColumnType.INTEGER, true, 1),
                        targetColumn(
                            "customer_name",
                            "varchar(255)",
                            NeutralColumnType.STRING,
                            false,
                            0)))),
        (dialect, jdbcUrl, username, password, connectionTimeout) -> {
          observedTimeout.set(connectionTimeout);
          return () -> connection;
        });

    assertThat(observedTimeout).hasValue(Duration.ofSeconds(4));
  }

  @Test
  void validatesAgainstResolvedTargetTableIdentity() throws Exception {
    Connection connection = mock(Connection.class);
    TableSchema schema = sampleOrdersSchema();
    TableId mappedTarget = new TableId("target", "archive", "orders_copy");
    AtomicReference<TableId> inspectedTable = new AtomicReference<>();

    JdbcApplyTargetPreflight.validateTargetTables(
        connection,
        JdbcApplyTargetDialect.POSTGRES,
        List.of(schema),
        TargetTableResolver.of(java.util.Map.of(schema.tableId(), mappedTarget)),
        (ignoredConnection, ignoredDialect, tableId) -> {
          inspectedTable.set(tableId);
          return Optional.of(
              targetTableMetadata(
                  mappedTarget,
                  List.of(
                      targetColumn("id", "bigint", NeutralColumnType.INTEGER, true, 1),
                      targetColumn("customer_name", "varchar(255)", NeutralColumnType.STRING, false, 0))));
        });

    assertThat(inspectedTable).hasValue(mappedTarget);
  }

  @Test
  void failsClosedWhenTargetTableIsMissing() {
    Connection connection = mock(Connection.class);
    TableSchema schema = sampleOrdersSchema();

    assertThatThrownBy(
            () ->
                JdbcApplyTargetPreflight.validateTargetTables(
                    connection,
                    JdbcApplyTargetDialect.POSTGRES,
                    List.of(schema),
                    TargetTableResolver.identity(),
                    (ignoredConnection, ignoredDialect, ignoredTableId) -> Optional.empty()))
        .isInstanceOf(TargetApplyContractException.class)
        .satisfies(
            failure ->
                assertThat(((TargetApplyContractException) failure).failure().type())
                    .isEqualTo(TargetApplyFailureType.TARGET_TABLE_MISSING));
  }

  @Test
  void failsClosedWhenPrimaryKeyColumnsDoNotMatch() {
    Connection connection = mock(Connection.class);
    TableSchema schema = sampleOrdersSchema();
    TableId targetTableId = schema.tableId();

    assertThatThrownBy(
            () ->
                JdbcApplyTargetPreflight.validateTargetTables(
                    connection,
                    JdbcApplyTargetDialect.POSTGRES,
                    List.of(schema),
                    TargetTableResolver.identity(),
                    (ignoredConnection, ignoredDialect, ignoredTableId) ->
                        Optional.of(
                            targetTableMetadata(
                                targetTableId,
                                List.of(
                                    targetColumn("other_id", "bigint", NeutralColumnType.INTEGER, true, 1),
                                    targetColumn("customer_name", "varchar(255)", NeutralColumnType.STRING, false, 0))))))
        .isInstanceOf(TargetApplyContractException.class)
        .satisfies(
            failure ->
                assertThat(((TargetApplyContractException) failure).failure().type())
                    .isEqualTo(TargetApplyFailureType.TARGET_PRIMARY_KEY_MISMATCH));
  }

  private static TableSchema sampleOrdersSchema() {
    return TableSchema.create(
        new TableId("source", "app", "orders"),
        List.of(
            new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, 1, false),
            new ColumnDefinition("customer_name", "varchar(255)", NeutralColumnType.STRING, false, 0, true)),
        Instant.parse("2026-04-07T00:00:00Z"));
  }

  private static JdbcApplyTargetSchemaInspector.TargetTableMetadata targetTableMetadata(
      TableId tableId, List<JdbcApplyTargetSchemaInspector.TargetColumnMetadata> columns) {
    return new JdbcApplyTargetSchemaInspector.TargetTableMetadata(tableId, columns, true);
  }

  private static JdbcApplyTargetSchemaInspector.TargetColumnMetadata targetColumn(
      String name,
      String sourceType,
      NeutralColumnType neutralType,
      boolean primaryKey,
      int primaryKeyOrdinal) {
    return new JdbcApplyTargetSchemaInspector.TargetColumnMetadata(
        name, sourceType, neutralType, primaryKey, primaryKeyOrdinal, !primaryKey, null, sourceType, null);
  }
}
