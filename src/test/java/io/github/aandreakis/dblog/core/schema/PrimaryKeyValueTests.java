package io.github.aandreakis.dblog.core.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PrimaryKeyValueTests {
  private static final ColumnDefinition BINARY_PK =
      new ColumnDefinition("id", "varbinary(16)", NeutralColumnType.BINARY, true, false);
  private static final ColumnDefinition TIMESTAMP_PK =
      new ColumnDefinition("created_at", "timestamp", NeutralColumnType.TIMESTAMP, true, false);
  private static final ColumnDefinition UUID_PK =
      new ColumnDefinition("uuid_value", "uuid", NeutralColumnType.UUID, true, false);
  private static final ColumnDefinition BOOLEAN_PK =
      new ColumnDefinition("enabled", "boolean", NeutralColumnType.BOOLEAN, true, false);
  private static final ColumnDefinition INTEGER_PK =
      new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false);
  private static final ColumnDefinition DECIMAL_PK =
      new ColumnDefinition("amount", "decimal(18,4)", NeutralColumnType.DECIMAL, true, false);
  private static final ColumnDefinition DATE_PK =
      new ColumnDefinition("created_on", "date", NeutralColumnType.DATE, true, false);
  private static final ColumnDefinition TIME_PK =
      new ColumnDefinition("created_at", "time", NeutralColumnType.TIME, true, false);

  @Test
  void supportsDeterministicBinaryUuidAndDecimalCanonicalization() {
    PrimaryKeyValue binaryLow =
        PrimaryKeyValue.fromColumn(BINARY_PK, ByteBuffer.wrap(new byte[] {0x00, (byte) 0xFF}));
    PrimaryKeyValue binaryHigh = PrimaryKeyValue.fromLiteral(BINARY_PK, "0100");
    PrimaryKeyValue uuidLow =
        PrimaryKeyValue.fromColumn(UUID_PK, UUID.fromString("00000000-0000-0000-0000-000000000001"));
    PrimaryKeyValue uuidHigh =
        PrimaryKeyValue.fromLiteral(UUID_PK, "00000000-0000-0000-0000-000000000002");
    PrimaryKeyValue integerValue = PrimaryKeyValue.fromColumn(INTEGER_PK, new BigDecimal("42.0"));
    PrimaryKeyValue decimalValue = PrimaryKeyValue.fromLiteral(DECIMAL_PK, "10.5000");

    assertThat(binaryLow.literal()).isEqualTo("00ff");
    assertThat(binaryLow.compareTo(binaryHigh)).isLessThan(0);
    assertThat(uuidLow.compareTo(uuidHigh)).isLessThan(0);
    assertThat(integerValue.literal()).isEqualTo("42");
    assertThat(decimalValue.literal()).isEqualTo("10.5");
  }

  @Test
  void normalizesDateTimeAndRejectsInconsistentTimestampRepresentations() {
    PrimaryKeyValue dateValue =
        PrimaryKeyValue.fromColumn(DATE_PK, Date.valueOf(LocalDate.of(2026, 4, 3)));
    PrimaryKeyValue timeValue =
        PrimaryKeyValue.fromColumn(TIME_PK, Time.valueOf(LocalTime.of(1, 2, 3)));
    PrimaryKeyValue instantValue =
        PrimaryKeyValue.fromColumn(TIMESTAMP_PK, OffsetDateTime.parse("2026-04-03T01:02:03+02:00"));
    PrimaryKeyValue localValue =
        PrimaryKeyValue.fromColumn(TIMESTAMP_PK, LocalDateTime.parse("2026-03-25T00:00:00"));

    assertThat(dateValue.literal()).isEqualTo("2026-04-03");
    assertThat(timeValue.literal()).isEqualTo("01:02:03");
    assertThat(instantValue.literal()).isEqualTo(Instant.parse("2026-04-02T23:02:03Z").toString());
    assertThatThrownBy(() -> instantValue.compareTo(localValue))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("inconsistent representations");
  }

  @Test
  void rejectsInvalidRepresentations() {
    assertThatThrownBy(() -> PrimaryKeyValue.fromLiteral(BOOLEAN_PK, "yes"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("invalid boolean primary key value");
    assertThatThrownBy(() -> PrimaryKeyValue.fromColumn(INTEGER_PK, new BigDecimal("42.5")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be integral");
    assertThatThrownBy(() -> PrimaryKeyValue.fromColumn(TIMESTAMP_PK, new Object()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("unsupported timestamp primary key value");
  }
}
