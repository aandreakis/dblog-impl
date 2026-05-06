package io.github.aandreakis.dblog.runtime.sql;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class RuntimeSqlSupportTests {
  @Test
  void configureRuntimeSqlConnectionSetsAutoCommitIsolationAndNetworkTimeout() throws Exception {
    Connection connection = mock(Connection.class);

    RuntimeSqlSupport.configureRuntimeSqlConnection(connection, Duration.ofSeconds(7));

    verify(connection).setAutoCommit(true);
    verify(connection).setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    verify(connection).setNetworkTimeout(any(), anyInt());
  }

  @Test
  void requireRuntimeSqlConnectionAcceptsOpenAutoCommitReadCommittedConnection() throws Exception {
    Connection connection = mock(Connection.class);
    when(connection.isClosed()).thenReturn(false);
    when(connection.getAutoCommit()).thenReturn(true);
    when(connection.getTransactionIsolation()).thenReturn(Connection.TRANSACTION_READ_COMMITTED);

    assertThatCode(() -> RuntimeSqlSupport.requireRuntimeSqlConnection(connection, "postgres"))
        .doesNotThrowAnyException();
  }

  @Test
  void requireRuntimeSqlConnectionRejectsClosedConnection() throws Exception {
    Connection connection = mock(Connection.class);
    when(connection.isClosed()).thenReturn(true);

    assertThatThrownBy(() -> RuntimeSqlSupport.requireRuntimeSqlConnection(connection, "postgres"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("must be open");
  }

  @Test
  void requireRuntimeSqlConnectionRejectsManualCommitConnection() throws Exception {
    Connection connection = mock(Connection.class);
    when(connection.isClosed()).thenReturn(false);
    when(connection.getAutoCommit()).thenReturn(false);

    assertThatThrownBy(() -> RuntimeSqlSupport.requireRuntimeSqlConnection(connection, "postgres"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("auto-commit");
  }

  @Test
  void requireRuntimeSqlConnectionRejectsWrongIsolation() throws Exception {
    Connection connection = mock(Connection.class);
    when(connection.isClosed()).thenReturn(false);
    when(connection.getAutoCommit()).thenReturn(true);
    when(connection.getTransactionIsolation()).thenReturn(Connection.TRANSACTION_REPEATABLE_READ);

    assertThatThrownBy(() -> RuntimeSqlSupport.requireRuntimeSqlConnection(connection, "mysql"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("READ_COMMITTED")
        .hasMessageContaining("REPEATABLE_READ");
  }
}
