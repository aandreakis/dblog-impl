package io.github.aandreakis.dblog.adapter.postgres.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.aandreakis.dblog.core.model.TableId;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class JdbcPostgresSourceSchemaInspectorTests {
  @Test
  void preservesCompositePrimaryKeyOrderFromPrimaryKeyOrdinal() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true, true, true, false);
    when(resultSet.getString("column_name")).thenReturn("b_id", "a_id", "name");
    when(resultSet.getString("source_type")).thenReturn("bigint", "bigint", "text");
    when(resultSet.getString("type_name")).thenReturn("int8", "int8", "text");
    when(resultSet.getString("type_kind")).thenReturn("b", "b", "b");
    when(resultSet.getInt("primary_key_ordinal")).thenReturn(2, 1, 0);
    when(resultSet.getBoolean("nullable")).thenReturn(false, false, true);

    JdbcPostgresSourceSchemaInspector inspector = new JdbcPostgresSourceSchemaInspector();

    var schema =
        inspector.readTableSchema(
            connectionReturning(resultSet), new TableId("appdb", "public", "widgets"));

    assertThat(schema).isNotNull();
    assertThat(schema.primaryKeyColumns()).containsExactly("a_id", "b_id");
    assertThat(schema.primaryKeyDefinitions())
        .extracting(io.github.aandreakis.dblog.core.schema.ColumnDefinition::primaryKeyOrdinal)
        .containsExactly(1, 2);
  }

  @Test
  void mapsEnumColumnsToEnumStringNeutralType() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true, true, false);
    when(resultSet.getString("column_name")).thenReturn("id", "status");
    when(resultSet.getString("source_type")).thenReturn("bigint", "widget_status");
    when(resultSet.getString("type_name")).thenReturn("int8", "widget_status");
    when(resultSet.getString("type_kind")).thenReturn("b", "e");
    when(resultSet.getInt("primary_key_ordinal")).thenReturn(1, 0);
    when(resultSet.getBoolean("nullable")).thenReturn(false, true);

    JdbcPostgresSourceSchemaInspector inspector = new JdbcPostgresSourceSchemaInspector();

    var schema =
        inspector.readTableSchema(
            connectionReturning(resultSet), new TableId("appdb", "public", "widgets"));

    assertThat(schema).isNotNull();
    assertThat(schema.columns().get(1).neutralType())
        .isEqualTo(io.github.aandreakis.dblog.core.schema.NeutralColumnType.ENUM_STRING);
  }

  @Test
  void stampsTheObservedRefreshTimeWhenReadingTableSchema() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true, true, false);
    when(resultSet.getString("column_name")).thenReturn("id", "name");
    when(resultSet.getString("source_type")).thenReturn("bigint", "text");
    when(resultSet.getString("type_name")).thenReturn("int8", "text");
    when(resultSet.getString("type_kind")).thenReturn("b", "b");
    when(resultSet.getInt("primary_key_ordinal")).thenReturn(1, 0);
    when(resultSet.getBoolean("nullable")).thenReturn(false, true);
    JdbcPostgresSourceSchemaInspector inspector = new JdbcPostgresSourceSchemaInspector();

    var schema =
        inspector.readTableSchema(
            connectionReturning(resultSet), new TableId("appdb", "public", "widgets"));

    assertThat(schema).isNotNull();
    assertThat(schema.refreshedAt()).isNotEqualTo(Instant.EPOCH);
  }

  @Test
  void rejectsTimetzPrimaryKeysBeforeLiveCaptureCanStart() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true, true, false);
    when(resultSet.getString("column_name")).thenReturn("observed_at", "name");
    when(resultSet.getString("source_type")).thenReturn("time with time zone", "text");
    when(resultSet.getString("type_name")).thenReturn("timetz", "text");
    when(resultSet.getString("type_kind")).thenReturn("b", "b");
    when(resultSet.getInt("primary_key_ordinal")).thenReturn(1, 0);
    when(resultSet.getBoolean("nullable")).thenReturn(false, true);

    JdbcPostgresSourceSchemaInspector inspector = new JdbcPostgresSourceSchemaInspector();

    assertThatThrownBy(
            () ->
                inspector.readTableSchema(
                    connectionReturning(resultSet),
                    new TableId("appdb", "public", "timed_widgets")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("TIMETZ primary key")
        .hasMessageContaining("public.timed_widgets");
  }

  private static Connection connectionReturning(ResultSet resultSet) throws Exception {
    PreparedStatement statement = mock(PreparedStatement.class);
    when(statement.executeQuery()).thenReturn(resultSet);

    Connection connection = mock(Connection.class);
    when(connection.prepareStatement(anyString())).thenReturn(statement);
    return connection;
  }
}
