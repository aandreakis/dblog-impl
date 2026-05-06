package io.github.aandreakis.dblog.adapter.mysql.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class MySqlValueDecoderTests {

  @Test
  void returnsNullForNullInput() {
    assertThat(MySqlValueDecoder.INSTANCE.decodeJson(null)).isNull();
  }

  @Test
  void passesThroughStringValues() {
    String json = "{\"k\":1}";
    assertThat(MySqlValueDecoder.INSTANCE.decodeJson(json)).isEqualTo(json);
  }

  @Test
  void binaryPayloadThatIsNotMySqlBinaryFormatFailsClosed() {
    byte[] notJsonBinary = new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
    assertThatThrownBy(() -> MySqlValueDecoder.INSTANCE.decodeJson(notJsonBinary))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Failed to decode MySQL JSON bytes");
  }

  @Test
  void byteBufferIsDecodedLikeByteArray() {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0xFF});
    assertThatThrownBy(() -> MySqlValueDecoder.INSTANCE.decodeJson(buffer))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Failed to decode MySQL JSON bytes");
  }
}
