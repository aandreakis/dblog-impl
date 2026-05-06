package io.github.aandreakis.dblog.verification.scenario.postgres;

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
import java.time.Duration;
import org.junit.jupiter.api.Test;

class PostgresScenarioSupportTests {

  @Test
  void closesAlreadyOpenedStatementsWhenPreparedMutationConstructionFails() throws Exception {
    Connection connection = mock(Connection.class);
    PreparedStatement firstStatement = mock(PreparedStatement.class);
    SQLException expected = new SQLException("boom");
    when(connection.prepareStatement(anyString())).thenReturn(firstStatement).thenThrow(expected);

    assertThatThrownBy(
            () ->
                PostgresScenarioSupport.runDeterministicMutationSequence(
                    connection,
                    mock(ScenarioStore.class),
                    "scenario-1",
                    new PostgresScenarioSchema("scn_test", null, null, "pub_test", "slot_test"),
                    1,
                    Duration.ofMillis(1),
                    1))
        .isSameAs(expected);

    verify(connection, times(2)).prepareStatement(anyString());
    verify(firstStatement).close();
  }
}
