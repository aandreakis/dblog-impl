package io.github.aandreakis.dblog.state.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class JdbcStateStoreSupport {
  private final ConnectionFactory connectionFactory;
  private final AutoCloseable closeAction;

  public JdbcStateStoreSupport(String driverClassName, String jdbcUrl) {
    this(
        requireNonBlank(driverClassName, "driverClassName"),
        () -> DriverManager.getConnection(requireNonBlank(jdbcUrl, "jdbcUrl")),
        null);
  }

  public JdbcStateStoreSupport(
      String driverClassName, ConnectionFactory connectionFactory, AutoCloseable closeAction) {
    this.connectionFactory = Objects.requireNonNull(connectionFactory, "connectionFactory");
    this.closeAction = closeAction;
    loadDriver(requireNonBlank(driverClassName, "driverClassName"));
  }

  public <T> T withTransaction(SqlFunction<Connection, T> work) {
    Objects.requireNonNull(work, "work");
    try (Connection connection = connectionFactory.open()) {
      connection.setAutoCommit(false);
      try {
        T result = work.apply(connection);
        connection.commit();
        return result;
      } catch (Throwable failure) {
        rollbackQuietly(connection, failure);
        throw wrapFailure(failure);
      }
    } catch (SQLException failure) {
      throw new IllegalStateException("state-store JDBC operation failed", failure);
    }
  }

  public void close() {
    if (closeAction == null) {
      return;
    }
    try {
      closeAction.close();
    } catch (RuntimeException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new IllegalStateException("state-store JDBC shutdown failed", ex);
    }
  }

  int update(Connection connection, String sql, Object... parameters) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      bind(statement, parameters);
      return statement.executeUpdate();
    }
  }

  void insert(Connection connection, String sql, Object... parameters) throws SQLException {
    update(connection, sql, parameters);
  }

  <T> Optional<T> queryOptional(
      Connection connection, String sql, SqlRowMapper<T> rowMapper, Object... parameters)
      throws SQLException {
    List<T> results = queryList(connection, sql, rowMapper, parameters);
    if (results.isEmpty()) {
      return Optional.empty();
    }
    return Optional.ofNullable(results.getFirst());
  }

  <T> List<T> queryList(
      Connection connection, String sql, SqlRowMapper<T> rowMapper, Object... parameters)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      bind(statement, parameters);
      try (ResultSet resultSet = statement.executeQuery()) {
        List<T> results = new ArrayList<>();
        while (resultSet.next()) {
          results.add(rowMapper.map(resultSet));
        }
        return List.copyOf(results);
      }
    }
  }

  private static void bind(PreparedStatement statement, Object... parameters) throws SQLException {
    for (int index = 0; index < parameters.length; index++) {
      statement.setObject(index + 1, parameters[index]);
    }
  }

  private static RuntimeException wrapFailure(Throwable failure) {
    if (failure instanceof RuntimeException runtimeException) {
      return runtimeException;
    }
    if (failure instanceof Error error) {
      throw error;
    }
    return new IllegalStateException("state-store JDBC operation failed", failure);
  }

  private static void rollbackQuietly(Connection connection, Throwable originalFailure) {
    try {
      connection.rollback();
    } catch (SQLException rollbackFailure) {
      originalFailure.addSuppressed(rollbackFailure);
    }
  }

  private static void loadDriver(String driverClassName) {
    try {
      Class.forName(driverClassName);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(
          "required JDBC driver is not on the classpath: " + driverClassName, e);
    }
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  @FunctionalInterface
  public interface SqlFunction<T, R> {
    R apply(T value) throws Exception;
  }

  @FunctionalInterface
  public interface ConnectionFactory {
    Connection open() throws SQLException;
  }

  @FunctionalInterface
  interface SqlRowMapper<T> {
    T map(ResultSet resultSet) throws SQLException;
  }
}
