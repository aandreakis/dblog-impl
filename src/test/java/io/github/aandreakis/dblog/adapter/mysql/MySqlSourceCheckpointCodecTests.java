package io.github.aandreakis.dblog.adapter.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class MySqlSourceCheckpointCodecTests {
  @Test
  void roundTripsTypedSourcePositions() {
    MySqlSourceCheckpointCodec codec = new MySqlSourceCheckpointCodec();
    MySqlSourcePosition position = new MySqlSourcePosition("mysql-bin.000010", 1234L, "uuid:1-2");

    String encoded = codec.encode(position);
    MySqlSourcePosition decoded = codec.decode(encoded);

    assertThat(decoded).isEqualTo(position);
  }
}
