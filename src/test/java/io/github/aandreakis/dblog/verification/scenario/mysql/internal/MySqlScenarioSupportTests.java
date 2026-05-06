package io.github.aandreakis.dblog.verification.scenario.mysql.internal;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.aandreakis.dblog.verification.scenario.ScenarioStore;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class MySqlScenarioSupportTests {

  @Test
  void closesAlreadyOpenedStatementsWhenPreparedMutationConstructionFails() throws Exception {
    Connection connection = mock(Connection.class);
    PreparedStatement firstStatement = mock(PreparedStatement.class);
    SQLException expected = new SQLException("boom");
    when(connection.prepareStatement(anyString())).thenReturn(firstStatement).thenThrow(expected);

    assertThatThrownBy(
            () ->
                MySqlScenarioSupport.runDeterministicMutationSequence(
                    connection,
                    mock(ScenarioStore.class),
                    "scenario-1",
                    MySqlScenarioSchema.forScenario("source-1", "db1"),
                    1,
                    Duration.ofMillis(1),
                    1))
        .isSameAs(expected);

    verify(connection, times(2)).prepareStatement(anyString());
    verify(firstStatement).close();
  }

  @Test
  void quotesDatabaseNameWhenPreparingWidgetMutationStatements() throws Exception {
    Connection connection = mock(Connection.class);
    PreparedStatement statement = mock(PreparedStatement.class);
    when(connection.prepareStatement(anyString())).thenReturn(statement);

    MySqlScenarioSupport.insertWidget(connection, "db`1", 1L, "widget", true, new byte[] {1});

    verify(connection)
        .prepareStatement(
            "INSERT INTO `db``1`.`widgets` (id, name, enabled, payload) VALUES (?, ?, ?, ?)");
  }

  @Test
  void quotesDatabaseNameWhenExecutingResetDdl() throws Exception {
    Connection connection = mock(Connection.class);
    Statement statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);

    MySqlScenarioSupport.resetScenarioSource(connection, "db`1");

    verify(statement).execute("DROP TABLE IF EXISTS `db``1`.`widgets`");
    verify(statement)
        .execute(
            "CREATE TABLE `db``1`.`widgets` (id BIGINT PRIMARY KEY, name VARCHAR(255), enabled BOOLEAN, payload VARBINARY(255))");
  }
}
