-- Demo fixture — disposable credentials for local/compose use only.
-- Do NOT reuse these values outside this bundled fixture stack.

CREATE ROLE dblog WITH LOGIN PASSWORD 'dblog' REPLICATION;
GRANT ALL PRIVILEGES ON DATABASE app TO dblog;

\connect app

CREATE SCHEMA IF NOT EXISTS demo;
CREATE SCHEMA IF NOT EXISTS app;
CREATE SCHEMA IF NOT EXISTS dblog_meta AUTHORIZATION dblog;
GRANT USAGE, CREATE ON SCHEMA demo TO dblog;
GRANT USAGE, CREATE ON SCHEMA app TO dblog;
GRANT USAGE, CREATE ON SCHEMA dblog_meta TO dblog;

CREATE TABLE IF NOT EXISTS demo.sample_orders (
  id BIGINT PRIMARY KEY,
  customer_name TEXT NOT NULL,
  status TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS app.sample_orders (
  id BIGINT PRIMARY KEY,
  customer_name TEXT NOT NULL,
  status TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE demo.sample_orders OWNER TO dblog;
ALTER TABLE app.sample_orders OWNER TO dblog;
ALTER TABLE demo.sample_orders REPLICA IDENTITY FULL;
ALTER TABLE app.sample_orders REPLICA IDENTITY FULL;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA demo TO dblog;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA app TO dblog;
ALTER DEFAULT PRIVILEGES IN SCHEMA demo GRANT ALL PRIVILEGES ON TABLES TO dblog;
ALTER DEFAULT PRIVILEGES IN SCHEMA app GRANT ALL PRIVILEGES ON TABLES TO dblog;
ALTER DEFAULT PRIVILEGES IN SCHEMA dblog_meta GRANT ALL PRIVILEGES ON TABLES TO dblog;
-- DBLog creates its own explicit-table publication at runtime when
-- dblog.source.postgres.publication-ownership is DBLOG_MANAGED (the default);
-- see docs/adapters/postgres.md §8. No init-time publication is needed.
