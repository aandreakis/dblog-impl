package io.github.aandreakis.dblog.core.schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

/** Encodes primary-key tuples for persistence and transport at DBLog's storage edges. */
public final class PrimaryKeyTupleCodec {
  private static final String PREFIX = "pkt:v1:";

  private PrimaryKeyTupleCodec() {}

  public static String encode(PrimaryKeyTuple tuple) {
    Objects.requireNonNull(tuple, "tuple");
    try {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      DataOutputStream output = new DataOutputStream(buffer);
      output.writeInt(tuple.columnNames().size());
      for (int index = 0; index < tuple.columnNames().size(); index++) {
        output.writeUTF(tuple.columnNames().get(index));
        output.writeUTF(tuple.values().get(index).neutralType().name());
        output.writeUTF(tuple.values().get(index).literal());
      }
      output.flush();
      return PREFIX + Base64.getUrlEncoder().withoutPadding().encodeToString(buffer.toByteArray());
    } catch (IOException e) {
      throw new IllegalStateException("failed to encode primary-key tuple", e);
    }
  }

  public static PrimaryKeyTuple decode(String encoded) {
    Objects.requireNonNull(encoded, "encoded");
    if (!encoded.startsWith(PREFIX)) {
      throw new IllegalArgumentException(
          "unsupported primary-key tuple encoding; expected " + PREFIX + " prefix");
    }
    try {
      byte[] bytes =
          Base64.getUrlDecoder().decode(encoded.substring(PREFIX.length()));
      DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes));
      int count = input.readInt();
      if (count <= 0) {
        throw new IllegalArgumentException("encoded primary-key tuple must contain at least one column");
      }
      List<String> columnNames = new ArrayList<>(count);
      List<PrimaryKeyValue> values = new ArrayList<>(count);
      for (int index = 0; index < count; index++) {
        columnNames.add(input.readUTF());
        NeutralColumnType neutralType = NeutralColumnType.valueOf(input.readUTF());
        values.add(PrimaryKeyValue.fromLiteral(neutralType, input.readUTF()));
      }
      return PrimaryKeyTuple.fromValues(columnNames, values);
    } catch (IOException e) {
      throw new IllegalArgumentException("failed to decode primary-key tuple", e);
    }
  }

  public static String encodeNullable(PrimaryKeyTuple tuple) {
    return tuple == null ? null : encode(tuple);
  }

  public static PrimaryKeyTuple decodeNullable(String encoded) {
    return encoded == null ? null : decode(encoded);
  }
}
