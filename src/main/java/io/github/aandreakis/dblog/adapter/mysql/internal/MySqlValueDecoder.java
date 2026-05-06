package io.github.aandreakis.dblog.adapter.mysql.internal;

import com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary;
import io.github.aandreakis.dblog.adapter.api.ValueDecoder;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * MySQL {@link ValueDecoder}. Owns the dependency on {@code mysql-binlog-connector-java}'s
 * {@link JsonBinary}, which decodes MySQL's internal binary JSON wire format captured in row
 * events into a JSON string the neutral normalizer can pass through.
 */
public final class MySqlValueDecoder implements ValueDecoder {
  public static final MySqlValueDecoder INSTANCE = new MySqlValueDecoder();

  private MySqlValueDecoder() {}

  @Override
  public Object decodeJson(Object raw) {
    if (raw == null) {
      return null;
    }
    if (raw instanceof byte[] bytes) {
      return parseJsonBytes(bytes);
    }
    if (raw instanceof ByteBuffer byteBuffer) {
      ByteBuffer duplicate = byteBuffer.duplicate();
      byte[] bytes = new byte[duplicate.remaining()];
      duplicate.get(bytes);
      return parseJsonBytes(bytes);
    }
    return raw;
  }

  private static String parseJsonBytes(byte[] bytes) {
    try {
      return JsonBinary.parseAsString(bytes);
    } catch (IOException ex) {
      throw new IllegalStateException("Failed to decode MySQL JSON bytes", ex);
    }
  }
}
