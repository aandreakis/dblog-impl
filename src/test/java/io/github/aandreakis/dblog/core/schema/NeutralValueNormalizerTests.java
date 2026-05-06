package io.github.aandreakis.dblog.core.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Per-type semantic coverage for {@link NeutralValueNormalizer}. Each neutral column type gets
 * a dedicated parameterized matrix plus any family-specific fail-closed assertions.
 *
 * <p>Covers the canonicalization rules described in docs/IMPLEMENTATION.md §5.2.
 */
class NeutralValueNormalizerTests {

  // ---------------------------------------------------------------------------------------
  // Round-trip summary (preserved from the original broad-assertion tests).
  // ---------------------------------------------------------------------------------------

  @Test
  void normalizesBooleanIntegerDecimalTemporalUuidAndBinaryFamilies() {
    assertThat(normalized(NeutralColumnType.BOOLEAN, "true")).isEqualTo(true);
    assertThat(normalized(NeutralColumnType.INTEGER, "42")).isEqualTo(42L);
    assertThat(normalized(NeutralColumnType.INTEGER, new BigInteger("9223372036854775808")))
        .isEqualTo(new BigInteger("9223372036854775808"));
    assertThat(normalized(NeutralColumnType.DECIMAL, "42.50")).isEqualTo(new BigDecimal("42.50"));
    assertThat(normalized(NeutralColumnType.DATE, "2026-04-12"))
        .isEqualTo(LocalDate.parse("2026-04-12"));
    assertThat(normalized(NeutralColumnType.TIME, "10:15:30"))
        .isEqualTo(LocalTime.parse("10:15:30"));
    assertThat(normalized(NeutralColumnType.TIMESTAMP, "2026-04-12T10:15:30Z"))
        .isEqualTo(Instant.parse("2026-04-12T10:15:30Z"));
    assertThat(normalized(NeutralColumnType.TIMESTAMP, "2026-04-12 10:15:30"))
        .isEqualTo(LocalDateTime.parse("2026-04-12T10:15:30"));

    UUID uuid = UUID.randomUUID();
    assertThat(normalized(NeutralColumnType.UUID, uuid.toString())).isEqualTo(uuid);

    byte[] binary = new byte[] {(byte) 0x10, (byte) 0xFF};
    assertThat((byte[]) normalized(NeutralColumnType.BINARY, binary)).containsExactly(binary);
    assertThat((byte[]) normalized(NeutralColumnType.BINARY, ByteBuffer.wrap(binary)))
        .containsExactly(binary);
  }

  @Test
  void preservesHighBinaryBytesWhenSurfacedAsStringAndNormalizesOffsetDateTimeToInstant() {
    String binaryString =
        new String(new byte[] {(byte) 0x10, (byte) 0xFF}, StandardCharsets.ISO_8859_1);
    assertThat((byte[]) normalized(NeutralColumnType.BINARY, binaryString))
        .containsExactly((byte) 0x10, (byte) 0xFF);

    OffsetDateTime timestamp =
        OffsetDateTime.of(2026, 4, 12, 10, 15, 30, 0, ZoneOffset.ofHours(2));
    assertThat(normalized(NeutralColumnType.TIMESTAMP, timestamp)).isEqualTo(timestamp.toInstant());
  }

  // ---------------------------------------------------------------------------------------
  // Null passthrough: every type returns null for null input.
  // ---------------------------------------------------------------------------------------

  @ParameterizedTest
  @org.junit.jupiter.params.provider.EnumSource(NeutralColumnType.class)
  void nullInputAlwaysPassesThroughForEveryType(NeutralColumnType type) {
    assertThat(normalized(type, null)).isNull();
  }

  // ---------------------------------------------------------------------------------------
  // BOOLEAN
  // ---------------------------------------------------------------------------------------

  @ParameterizedTest(name = "[{index}] {0} => {1}")
  @MethodSource("booleanCases")
  void booleanNormalizationAcceptsAllDocumentedRepresentations(Object input, Boolean expected) {
    assertThat(normalized(NeutralColumnType.BOOLEAN, input)).isEqualTo(expected);
  }

  private static Stream<Arguments> booleanCases() {
    return Stream.of(
        Arguments.of(Boolean.TRUE, true),
        Arguments.of(Boolean.FALSE, false),
        Arguments.of(0, false),
        Arguments.of(1, true),
        Arguments.of(5, true),
        Arguments.of(-1, true),
        Arguments.of(0L, false),
        Arguments.of("true", true),
        Arguments.of("TRUE", true),
        Arguments.of("false", false),
        Arguments.of("FALSE", false),
        Arguments.of("t", true),
        Arguments.of("f", false),
        Arguments.of("1", true),
        Arguments.of("0", false),
        Arguments.of("yes", true),
        Arguments.of("no", false),
        Arguments.of("on", true),
        Arguments.of("off", false),
        Arguments.of("  true  ", true));
  }

  @ParameterizedTest
  @ValueSource(strings = {"xyz", "maybe", "2", "truee", ""})
  void booleanNormalizationFailsClosedOnUnsupportedStringRepresentations(String bad) {
    assertThatThrownBy(() -> normalized(NeutralColumnType.BOOLEAN, bad))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Unsupported boolean value representation");
  }

  // ---------------------------------------------------------------------------------------
  // INTEGER — widens to BigInteger past 63 bits, keeps long otherwise.
  // ---------------------------------------------------------------------------------------

  @ParameterizedTest(name = "[{index}] {0} => {1}")
  @MethodSource("integerCases")
  void integerNormalizationPreservesValueAndWidensPast63Bits(Object input, Object expected) {
    assertThat(normalized(NeutralColumnType.INTEGER, input)).isEqualTo(expected);
  }

  private static Stream<Arguments> integerCases() {
    return Stream.of(
        Arguments.of((byte) 7, 7L),
        Arguments.of((short) 7, 7L),
        Arguments.of(7, 7L),
        Arguments.of(7L, 7L),
        Arguments.of(BigInteger.valueOf(42), 42L),
        Arguments.of(new BigDecimal("42"), 42L),
        Arguments.of("42", 42L),
        Arguments.of(Long.MAX_VALUE, Long.MAX_VALUE),
        Arguments.of(Long.MIN_VALUE, Long.MIN_VALUE),
        // Long.MAX_VALUE + 1 requires BigInteger representation.
        Arguments.of(new BigInteger("9223372036854775808"), new BigInteger("9223372036854775808")),
        Arguments.of("9223372036854775808", new BigInteger("9223372036854775808")),
        // Long.MIN_VALUE - 1 also widens.
        Arguments.of(new BigInteger("-9223372036854775809"), new BigInteger("-9223372036854775809")));
  }

  @Test
  void integerNormalizationFailsClosedOnNonIntegralDecimalInput() {
    assertThatThrownBy(() -> normalized(NeutralColumnType.INTEGER, new BigDecimal("1.5")))
        .isInstanceOf(ArithmeticException.class);
  }

  // ---------------------------------------------------------------------------------------
  // DECIMAL / FLOAT — preserved as BigDecimal; trailing-zero preservation tested.
  // ---------------------------------------------------------------------------------------

  @ParameterizedTest(name = "[{index}] {0} => {1}")
  @MethodSource("decimalCases")
  void decimalNormalizationCoercesToBigDecimal(Object input, BigDecimal expected) {
    assertThat(normalized(NeutralColumnType.DECIMAL, input)).isEqualTo(expected);
    assertThat(normalized(NeutralColumnType.FLOAT, input)).isEqualTo(expected);
  }

  private static Stream<Arguments> decimalCases() {
    return Stream.of(
        Arguments.of(new BigDecimal("42.50"), new BigDecimal("42.50")),
        Arguments.of(new BigDecimal("0"), new BigDecimal("0")),
        Arguments.of(BigInteger.valueOf(42), new BigDecimal("42")),
        Arguments.of(42, new BigDecimal("42.0")),
        Arguments.of(42L, new BigDecimal("42.0")),
        Arguments.of("0", new BigDecimal("0")),
        Arguments.of("42.50", new BigDecimal("42.50")),
        Arguments.of("-3.14", new BigDecimal("-3.14")));
  }

  @Test
  void decimalPreservesScaleOnBigDecimalInput() {
    BigDecimal result =
        (BigDecimal) normalized(NeutralColumnType.DECIMAL, new BigDecimal("42.5000"));
    assertThat(result.scale()).isEqualTo(4);
  }

  // ---------------------------------------------------------------------------------------
  // STRING / ENUM_STRING — byte[] and ByteBuffer decoded as UTF-8.
  // ---------------------------------------------------------------------------------------

  @ParameterizedTest
  @MethodSource("stringCases")
  void stringNormalizationReturnsUtf8StringForEveryRepresentation(Object input, String expected) {
    assertThat(normalized(NeutralColumnType.STRING, input)).isEqualTo(expected);
    assertThat(normalized(NeutralColumnType.ENUM_STRING, input)).isEqualTo(expected);
    assertThat(normalized(NeutralColumnType.XML, input)).isEqualTo(expected);
  }

  private static Stream<Arguments> stringCases() {
    return Stream.of(
        Arguments.of("hello", "hello"),
        Arguments.of("", ""),
        Arguments.of(42, "42"),
        Arguments.of(42L, "42"),
        Arguments.of(true, "true"),
        Arguments.of("héllo".getBytes(StandardCharsets.UTF_8), "héllo"),
        Arguments.of(ByteBuffer.wrap("world".getBytes(StandardCharsets.UTF_8)), "world"));
  }

  // ---------------------------------------------------------------------------------------
  // BINARY — byte[] cloned, ByteBuffer copied, strings with \x prefix hex-parsed.
  // ---------------------------------------------------------------------------------------

  @Test
  void binaryByteArrayInputIsCloned() {
    byte[] input = new byte[] {0x01, 0x02, 0x03};
    byte[] output = (byte[]) normalized(NeutralColumnType.BINARY, input);
    assertThat(output).containsExactly(0x01, 0x02, 0x03);
    input[0] = 0x7F;
    assertThat(output[0]).isEqualTo((byte) 0x01); // defensively copied
  }

  @Test
  void binaryByteBufferPositionPreserved() {
    ByteBuffer buffer = ByteBuffer.allocate(4);
    buffer.put((byte) 0x0A).put((byte) 0x0B).put((byte) 0x0C).put((byte) 0x0D);
    buffer.position(1);
    byte[] output = (byte[]) normalized(NeutralColumnType.BINARY, buffer);
    assertThat(output).containsExactly(0x0B, 0x0C, 0x0D);
    assertThat(buffer.position()).isEqualTo(1); // duplicate did not mutate the input buffer
  }

  @Test
  void binaryHexEscapePrefixIsDecoded() {
    byte[] output = (byte[]) normalized(NeutralColumnType.BINARY, "\\x10ff");
    assertThat(output).containsExactly((byte) 0x10, (byte) 0xFF);
    byte[] uppercase = (byte[]) normalized(NeutralColumnType.BINARY, "\\X10FF");
    assertThat(uppercase).containsExactly((byte) 0x10, (byte) 0xFF);
  }

  @Test
  void binaryBareStringUsesIso88591ToPreserveHighBytes() {
    String binaryString = new String(new byte[] {(byte) 0xC0, (byte) 0xFF}, StandardCharsets.ISO_8859_1);
    byte[] output = (byte[]) normalized(NeutralColumnType.BINARY, binaryString);
    assertThat(output).containsExactly((byte) 0xC0, (byte) 0xFF);
  }

  @Test
  void binaryFailsClosedOnUnsupportedRepresentation() {
    assertThatThrownBy(() -> normalized(NeutralColumnType.BINARY, 42))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Unsupported binary value representation");
  }

  @Test
  void booleanAcceptsBitSetAsReturnedByMysqlBinlogDecoderForBit1Columns() {
    // mysql-binlog-connector-java decodes MySQL BIT(1) columns to java.util.BitSet.
    // Without this branch the normalizer falls through to the string handler, sees
    // "{}" / "{0}" (BitSet.toString), and crashes with "Unsupported boolean value
    // representation" — taking down the entire binlog session the first time any
    // captured table's BIT(1) column changes.
    java.util.BitSet empty = new java.util.BitSet();
    assertThat(normalized(NeutralColumnType.BOOLEAN, empty)).isEqualTo(Boolean.FALSE);

    java.util.BitSet oneSet = new java.util.BitSet();
    oneSet.set(0);
    assertThat(normalized(NeutralColumnType.BOOLEAN, oneSet)).isEqualTo(Boolean.TRUE);
  }

  @Test
  void booleanAcceptsSingleByteArrayWhereLowBitHoldsTheValue() {
    assertThat(normalized(NeutralColumnType.BOOLEAN, new byte[] {0x00})).isEqualTo(Boolean.FALSE);
    assertThat(normalized(NeutralColumnType.BOOLEAN, new byte[] {0x01})).isEqualTo(Boolean.TRUE);
    assertThat(normalized(NeutralColumnType.BOOLEAN, new byte[] {(byte) 0xFE})).isEqualTo(Boolean.FALSE);
    assertThat(normalized(NeutralColumnType.BOOLEAN, new byte[] {})).isEqualTo(Boolean.FALSE);
  }

  @Test
  void binaryAcceptsBitSetAsReturnedByMysqlBinlogDecoderForBitNColumns() {
    // MySQL BIT(n>1) maps to neutral BINARY in MySqlDialect; the binlog decoder
    // still hands it over as java.util.BitSet. Before this fix normalizeBinaryValue
    // failed closed with "Unsupported binary value representation: java.util.BitSet"
    // on the first UPDATE of any BIT(n) column.
    java.util.BitSet bits = new java.util.BitSet();
    bits.set(0); // 0b10101010 low-first (so bit 0 = 1)
    bits.set(2);
    bits.set(4);
    bits.set(6);
    byte[] output = (byte[]) normalized(NeutralColumnType.BINARY, bits);
    assertThat(output).isNotEmpty();
    // BitSet.toByteArray is little-endian; byte[0] low bits == bitset positions 0..7.
    assertThat(output[0] & 0xFF).isEqualTo(0x55); // bits 0,2,4,6 set = 0b01010101 = 0x55
  }

  // ---------------------------------------------------------------------------------------
  // DATE / TIME / TIMESTAMP — prefer canonical java.time types.
  // ---------------------------------------------------------------------------------------

  @ParameterizedTest
  @MethodSource("dateCases")
  void dateNormalizationCoercesToLocalDate(Object input, LocalDate expected) {
    assertThat(normalized(NeutralColumnType.DATE, input)).isEqualTo(expected);
  }

  private static Stream<Arguments> dateCases() {
    LocalDate reference = LocalDate.of(2026, 4, 12);
    return Stream.of(
        Arguments.of(reference, reference),
        Arguments.of(java.sql.Date.valueOf(reference), reference),
        Arguments.of("2026-04-12", reference));
  }

  @ParameterizedTest
  @MethodSource("timeCases")
  void timeNormalizationCoercesToLocalTime(Object input, LocalTime expected) {
    assertThat(normalized(NeutralColumnType.TIME, input)).isEqualTo(expected);
  }

  private static Stream<Arguments> timeCases() {
    LocalTime reference = LocalTime.of(10, 15, 30);
    return Stream.of(
        Arguments.of(reference, reference),
        Arguments.of(java.sql.Time.valueOf(reference), reference),
        Arguments.of("10:15:30", reference),
        // Time extracted from ISO datetime strings with either 'T' or space separators.
        Arguments.of("2026-04-12T10:15:30", reference),
        Arguments.of("2026-04-12 10:15:30", reference));
  }

  @ParameterizedTest
  @MethodSource("timestampCases")
  void timestampNormalizationPrefersInstantFallbackLocalDateTime(Object input, Object expected) {
    assertThat(normalized(NeutralColumnType.TIMESTAMP, input)).isEqualTo(expected);
  }

  private static Stream<Arguments> timestampCases() {
    Instant instant = Instant.parse("2026-04-12T10:15:30Z");
    LocalDateTime local = LocalDateTime.parse("2026-04-12T10:15:30");
    return Stream.of(
        // Instant passes through.
        Arguments.of(instant, instant),
        // OffsetDateTime normalizes to instant.
        Arguments.of(OffsetDateTime.parse("2026-04-12T10:15:30Z"), instant),
        Arguments.of(OffsetDateTime.parse("2026-04-12T12:15:30+02:00"), instant),
        // java.sql.Timestamp (naive local) normalizes via toInstant().
        Arguments.of(java.sql.Timestamp.valueOf(local), java.sql.Timestamp.valueOf(local).toInstant()),
        // LocalDateTime passes through (no timezone info).
        Arguments.of(local, local),
        // ISO strings with Z → Instant.
        Arguments.of("2026-04-12T10:15:30Z", instant),
        // ISO strings with offset → Instant.
        Arguments.of("2026-04-12T12:15:30+02:00", instant),
        // Naive local datetime strings with T → LocalDateTime.
        Arguments.of("2026-04-12T10:15:30", local),
        // Naive local datetime strings with space → LocalDateTime.
        Arguments.of("2026-04-12 10:15:30", local));
  }

  @Test
  void timestampFallsBackToRawStringForUnparseableText() {
    // Unparseable text returns the trimmed string rather than throwing. Downstream code can
    // then escalate via adaptive decode or drop the column explicitly. This preserves the
    // concept-level SPEC rule that the runtime does not silently coerce mixed timestamp shapes.
    assertThat(normalized(NeutralColumnType.TIMESTAMP, "not a timestamp")).isEqualTo("not a timestamp");
  }

  @Test
  void timestampPreservesEmptyStringSentinel() {
    assertThat(normalized(NeutralColumnType.TIMESTAMP, "")).isEqualTo("");
    assertThat(normalized(NeutralColumnType.TIMESTAMP, "   ")).isEqualTo("");
  }

  // ---------------------------------------------------------------------------------------
  // UUID
  // ---------------------------------------------------------------------------------------

  @Test
  void uuidNormalizationAcceptsBothUuidAndString() {
    UUID uuid = UUID.fromString("12345678-1234-1234-1234-123456789012");
    assertThat(normalized(NeutralColumnType.UUID, uuid)).isEqualTo(uuid);
    assertThat(normalized(NeutralColumnType.UUID, uuid.toString())).isEqualTo(uuid);
  }

  @Test
  void uuidNormalizationFailsClosedOnMalformedString() {
    assertThatThrownBy(() -> normalized(NeutralColumnType.UUID, "not-a-uuid"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // ---------------------------------------------------------------------------------------
  // UNSUPPORTED — pass-through, no coercion.
  // ---------------------------------------------------------------------------------------

  @Test
  void unsupportedNormalizationReturnsValueUnchanged() {
    Object opaque = new Object();
    assertThat(normalized(NeutralColumnType.UNSUPPORTED, opaque)).isSameAs(opaque);
  }

  // ---------------------------------------------------------------------------------------
  // JSON — UTF-8 string passthrough for non-binary inputs; invalid binary JSON fails closed.
  // ---------------------------------------------------------------------------------------

  @Test
  void jsonStringValueIsPassedThrough() {
    assertThat(normalized(NeutralColumnType.JSON, "{\"k\":1}")).isEqualTo("{\"k\":1}");
  }

  @Test
  void jsonByteArrayIsDecodedAsUtf8Text() {
    byte[] payload = "{\"tag\":\"mid\"}".getBytes(StandardCharsets.UTF_8);
    assertThat(normalized(NeutralColumnType.JSON, payload)).isEqualTo("{\"tag\":\"mid\"}");
  }

  @Test
  void jsonByteBufferIsDecodedAsUtf8Text() {
    java.nio.ByteBuffer payload =
        java.nio.ByteBuffer.wrap("{\"count\":1}".getBytes(StandardCharsets.UTF_8));
    assertThat(normalized(NeutralColumnType.JSON, payload)).isEqualTo("{\"count\":1}");
  }

  // ---------------------------------------------------------------------------------------
  // Helper
  // ---------------------------------------------------------------------------------------

  private static Object normalized(NeutralColumnType type, Object value) {
    return NeutralValueNormalizer.normalize(
        new ColumnDefinition("col", "source_type", type, false, true), value);
  }
}
