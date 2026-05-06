package io.github.aandreakis.dblog.adapter.mysql;

import io.github.aandreakis.dblog.adapter.api.SourceCheckpointCodec;

public final class MySqlSourceCheckpointCodec
    implements SourceCheckpointCodec<MySqlSourcePosition> {
  @Override
  public String encode(MySqlSourcePosition position) {
    return position.displayValue();
  }

  @Override
  public MySqlSourcePosition decode(String encoded) {
    return MySqlSourcePosition.parse(encoded);
  }
}
