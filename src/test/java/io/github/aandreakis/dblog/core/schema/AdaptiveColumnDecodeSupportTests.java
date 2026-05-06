package io.github.aandreakis.dblog.core.schema;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class AdaptiveColumnDecodeSupportTests {
  @Test
  void keepsIntegerFamilyForVeryLargeIntegralValues() {
    ColumnDefinition column =
        new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false);

    AdaptiveColumnDecodeSupport.DecodedColumn decoded =
        AdaptiveColumnDecodeSupport.decode(column, new java.math.BigInteger("9223372036854775808"));

    assertThat(decoded.column().neutralType()).isEqualTo(NeutralColumnType.INTEGER);
    assertThat(decoded.value()).isEqualTo(new java.math.BigInteger("9223372036854775808"));
  }

  @Test
  void widensIntegerToStringWhenObservedValueStopsLookingNumeric() {
    ColumnDefinition column =
        new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false);

    AdaptiveColumnDecodeSupport.DecodedColumn decoded =
        AdaptiveColumnDecodeSupport.decode(column, "not-a-number");

    assertThat(decoded.column().neutralType()).isEqualTo(NeutralColumnType.STRING);
    assertThat(decoded.value()).isEqualTo("not-a-number");
  }

  @Test
  void widensTimestampToStringWhenObservedValueCannotBeParsedAsTemporal() {
    ColumnDefinition column =
        new ColumnDefinition("updated_at", "timestamp", NeutralColumnType.TIMESTAMP, false, true);

    AdaptiveColumnDecodeSupport.DecodedColumn decoded =
        AdaptiveColumnDecodeSupport.decode(column, "not-a-timestamp");

    assertThat(decoded.column().neutralType()).isEqualTo(NeutralColumnType.STRING);
    assertThat(decoded.value()).isEqualTo("not-a-timestamp");
  }

  @Test
  void widensBooleanToIntegerWhenObservedValueNeedsIntegerFamily() {
    ColumnDefinition column =
        new ColumnDefinition("flag", "tinyint(1)", NeutralColumnType.BOOLEAN, false, true);

    AdaptiveColumnDecodeSupport.DecodedColumn decoded =
        AdaptiveColumnDecodeSupport.decode(column, 7);

    assertThat(decoded.column().neutralType()).isEqualTo(NeutralColumnType.INTEGER);
    assertThat(decoded.value()).isEqualTo(7L);
  }
}
