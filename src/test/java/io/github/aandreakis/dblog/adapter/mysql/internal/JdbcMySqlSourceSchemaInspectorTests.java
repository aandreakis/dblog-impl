package io.github.aandreakis.dblog.adapter.mysql.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.aandreakis.dblog.core.model.TableId;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class JdbcMySqlSourceSchemaInspectorTests {
  @Test
  void canonicalizesIntegerDisplayWidthsWhenReadingTableSchema() throws Exception {
    ResultSet resultSet =
        sequentialResultSet(
            new Object[][] {
              {"id", "bigint", "bigint(20)", "NO", 1},
              {"name", "varchar", "varchar(255)", "YES", 0}
            });
    JdbcMySqlSourceSchemaInspector inspector = new JdbcMySqlSourceSchemaInspector();

    var schema =
        inspector.readTableSchema(
            connectionReturning(resultSet), new TableId("mysql-source", "appdb", "widgets"));

    assertThat(schema).isNotNull();
    assertThat(schema.primaryKeyDefinition().sourceType()).isEqualTo("bigint");
    assertThat(schema.columns().get(1).sourceType()).isEqualTo("varchar(255)");
  }

  @Test
  void stampsTheObservedRefreshTimeWhenReadingTableSchema() throws Exception {
    ResultSet resultSet =
        sequentialResultSet(
            new Object[][] {
              {"id", "bigint", "bigint(20)", "NO", 1},
              {"name", "varchar", "varchar(255)", "YES", 0}
            });
    JdbcMySqlSourceSchemaInspector inspector = new JdbcMySqlSourceSchemaInspector();

    var schema =
        inspector.readTableSchema(
            connectionReturning(resultSet), new TableId("mysql-source", "appdb", "widgets"));

    assertThat(schema).isNotNull();
    assertThat(schema.refreshedAt()).isNotEqualTo(Instant.EPOCH);
  }

  @Test
  void preservesMeaningfulIntegerModifiersAndCompositePrimaryKeyOrder() throws Exception {
    ResultSet resultSet =
        sequentialResultSet(
            new Object[][] {
              {"b_id", "bigint", "bigint(20) unsigned", "NO", 2},
              {"a_id", "bigint", "bigint(20)", "NO", 1},
              {"name", "varchar", "varchar(255)", "YES", 0}
            });
    JdbcMySqlSourceSchemaInspector inspector = new JdbcMySqlSourceSchemaInspector();

    var schema =
        inspector.readTableSchema(
            connectionReturning(resultSet), new TableId("mysql-source", "appdb", "widgets"));

    assertThat(schema).isNotNull();
    assertThat(schema.columns().getFirst().sourceType()).isEqualTo("bigint unsigned");
    assertThat(schema.primaryKeyColumns()).containsExactly("a_id", "b_id");
  }

  private static Connection connectionReturning(ResultSet resultSet) throws Exception {
    PreparedStatement statement = mock(PreparedStatement.class);
    when(statement.executeQuery()).thenReturn(resultSet);

    Connection connection = mock(Connection.class);
    when(connection.prepareStatement(anyString())).thenReturn(statement);
    return connection;
  }

  private static ResultSet sequentialResultSet(Object[][] rows) throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    Boolean[] nextValues = new Boolean[rows.length + 1];
    for (int index = 0; index < rows.length; index++) {
      nextValues[index] = true;
    }
    nextValues[rows.length] = false;
    when(resultSet.next())
        .thenReturn(nextValues[0], java.util.Arrays.copyOfRange(nextValues, 1, nextValues.length));

    String[] column1 = new String[rows.length];
    String[] column2 = new String[rows.length];
    String[] column3 = new String[rows.length];
    String[] column4 = new String[rows.length];
    Integer[] column5 = new Integer[rows.length];
    for (int index = 0; index < rows.length; index++) {
      column1[index] = (String) rows[index][0];
      column2[index] = (String) rows[index][1];
      column3[index] = (String) rows[index][2];
      column4[index] = (String) rows[index][3];
      column5[index] = (Integer) rows[index][4];
    }

    when(resultSet.getString(1))
        .thenReturn(column1[0], java.util.Arrays.copyOfRange(column1, 1, column1.length));
    when(resultSet.getString(2))
        .thenReturn(column2[0], java.util.Arrays.copyOfRange(column2, 1, column2.length));
    when(resultSet.getString(3))
        .thenReturn(column3[0], java.util.Arrays.copyOfRange(column3, 1, column3.length));
    when(resultSet.getString(4))
        .thenReturn(column4[0], java.util.Arrays.copyOfRange(column4, 1, column4.length));
    when(resultSet.getInt(5))
        .thenReturn(column5[0], java.util.Arrays.copyOfRange(column5, 1, column5.length));
    return resultSet;
  }
}
