package io.github.aandreakis.dblog.adapter.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PostgresSourceCheckpointCodecTests {
  @Test
  void roundTripsTypedPostgresLsns() {
    PostgresSourceCheckpointCodec codec = new PostgresSourceCheckpointCodec();
    PostgresLsn position = new PostgresLsn(0L, 0x2AL);

    assertThat(codec.decode(codec.encode(position))).isEqualTo(position);
  }
}
