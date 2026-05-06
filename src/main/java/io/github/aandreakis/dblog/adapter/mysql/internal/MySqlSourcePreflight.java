package io.github.aandreakis.dblog.adapter.mysql.internal;

import io.github.aandreakis.dblog.DbLogRuntimeException;
import io.github.aandreakis.dblog.adapter.api.AdapterConnectionSupport;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.SourcePreflight;
import io.github.aandreakis.dblog.adapter.mysql.MySqlDialect;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * MySQL implementation of {@link SourcePreflight}. Validates the narrow MySQL server
 * capabilities required for streaming (binary logging enabled, {@code binlog_format=ROW},
 * {@code binlog_row_image=FULL}) by reading {@code @@GLOBAL} state through a short-lived
 * JDBC connection.
 *
 * <p>MySQL preflight does not need the contract schemas — capability requirements are
 * database-wide. The parameter is accepted to honor the {@link SourcePreflight} contract.
 */
public final class MySqlSourcePreflight implements SourcePreflight {
  private final JdbcMySqlSourceSchemaInspector schemaInspector;
  private final Function<RelationalSourceConfig, Connection> connectionFactory;

  public MySqlSourcePreflight() {
    this(new JdbcMySqlSourceSchemaInspector(), MySqlSourcePreflight::openConnection);
  }

  public MySqlSourcePreflight(
      JdbcMySqlSourceSchemaInspector schemaInspector,
      Function<RelationalSourceConfig, Connection> connectionFactory) {
    this.schemaInspector = Objects.requireNonNull(schemaInspector, "schemaInspector");
    this.connectionFactory = Objects.requireNonNull(connectionFactory, "connectionFactory");
  }

  @Override
  public void ensure(RelationalSourceConfig config, List<TableSchema> contractSchemas) {
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(contractSchemas, "contractSchemas");
    try (Connection connection = connectionFactory.apply(config)) {
      schemaInspector.readServerCapabilities(connection).requireStreamingPrerequisites();
    } catch (SQLException failure) {
      throw new DbLogRuntimeException(failure);
    }
  }

  private static Connection openConnection(RelationalSourceConfig config) {
    try {
      return AdapterConnectionSupport.openConfiguredSqlConnection(
          config, MySqlDialect.DISPLAY_NAME);
    } catch (SQLException failure) {
      throw new DbLogRuntimeException(failure);
    }
  }
}
