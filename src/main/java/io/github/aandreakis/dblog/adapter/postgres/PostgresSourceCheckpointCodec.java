package io.github.aandreakis.dblog.adapter.postgres;

import io.github.aandreakis.dblog.adapter.api.SourceCheckpointCodec;

public final class PostgresSourceCheckpointCodec
    implements SourceCheckpointCodec<PostgresLsn> {
  @Override
  public String encode(PostgresLsn position) {
    return position.displayValue();
  }

  @Override
  public PostgresLsn decode(String encoded) {
    return PostgresLsn.parse(encoded);
  }
}
