package io.github.aandreakis.dblog.adapter.mysql.internal;

import java.sql.SQLException;

/** Opens the MySQL binlog stream wrapper. */
public interface MySqlBinlogStreamFactory {
  MySqlBinlogStream open(MySqlBinlogStreamRequest request) throws SQLException;
}
