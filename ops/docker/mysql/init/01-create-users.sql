-- Demo fixture — disposable credentials for local/compose use only.
-- Do NOT reuse these values outside this bundled fixture stack.

CREATE DATABASE IF NOT EXISTS app;
CREATE DATABASE IF NOT EXISTS dblog_meta;

CREATE USER IF NOT EXISTS 'dblog'@'%' IDENTIFIED WITH mysql_native_password BY 'dblog';
GRANT ALL PRIVILEGES ON app.* TO 'dblog'@'%';
GRANT ALL PRIVILEGES ON dblog_meta.* TO 'dblog'@'%';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'dblog'@'%';
-- Allows DBLog to SET SESSION sql_log_bin=0 around its own metadata-table bootstrap DDL so
-- those CREATE DATABASE/TABLE statements do not enter the binlog (and therefore do not
-- propagate to downstream MySQL replicas). Without this privilege, the bootstrap DDL still
-- works — DBLog's streaming session skips its own metadata DDL via SQL-pattern fallback —
-- but replicas attached to this source would observe the leaked schema. A real production
-- grant should mirror this.
GRANT SESSION_VARIABLES_ADMIN ON *.* TO 'dblog'@'%';
FLUSH PRIVILEGES;

CREATE TABLE IF NOT EXISTS app.sample_orders (
  id BIGINT PRIMARY KEY,
  customer_name VARCHAR(255) NOT NULL,
  status VARCHAR(64) NOT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
