package io.github.aandreakis.dblog.sink.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TargetValueCoercionsTests {
  @Test
  void compatibilityIncludesRepresentativeCurrentTargetApplyPairs() {
    assertThat(TargetValueCoercions.isCompatible(NeutralColumnType.INTEGER, NeutralColumnType.STRING))
        .isTrue();
    assertThat(
            TargetValueCoercions.isCompatible(
                NeutralColumnType.TIMESTAMP, NeutralColumnType.STRING))
        .isTrue();
    assertThat(TargetValueCoercions.isCompatible(NeutralColumnType.UUID, NeutralColumnType.STRING))
        .isTrue();
    assertThat(TargetValueCoercions.isCompatible(NeutralColumnType.JSON, NeutralColumnType.JSON))
        .isTrue();
  }

  @Test
  void coercesInstantAndUuidIntoStableStringTargets() {
    JdbcApplyTargetSchemaInspector.TargetColumnMetadata stringTarget =
        new JdbcApplyTargetSchemaInspector.TargetColumnMetadata(
            "value_text",
            "text",
            NeutralColumnType.STRING,
            false,
            0,
            true,
            null,
            "text",
            "BASE");

    assertThat(TargetValueCoercions.coerce(Instant.parse("2026-03-29T12:34:56Z"), stringTarget))
        .isEqualTo("2026-03-29T12:34:56Z");
    assertThat(
            TargetValueCoercions.coerce(
                UUID.fromString("11111111-1111-1111-1111-111111111111"), stringTarget))
        .isEqualTo("11111111-1111-1111-1111-111111111111");
  }

  @Test
  void coercesJsonBytesIntoCanonicalJsonText() {
    JdbcApplyTargetSchemaInspector.TargetColumnMetadata jsonTarget =
        new JdbcApplyTargetSchemaInspector.TargetColumnMetadata(
            "json_value",
            "jsonb",
            NeutralColumnType.JSON,
            false,
            0,
            true,
            "public",
            "jsonb",
            "BASE");

    assertThat(
            TargetValueCoercions.coerce(
                "{\"tag\":\"mid\",\"value\":123}".getBytes(StandardCharsets.UTF_8), jsonTarget))
            .isEqualTo("{\"tag\":\"mid\",\"value\":123}");
  }

  @Test
  void keepsAlreadyNormalizedTypedValuesAsTypedValues() {
    JdbcApplyTargetSchemaInspector.TargetColumnMetadata dateTarget =
        new JdbcApplyTargetSchemaInspector.TargetColumnMetadata(
            "event_date",
            "date",
            NeutralColumnType.DATE,
            false,
            0,
            true,
            null,
            "date",
            "BASE");
    JdbcApplyTargetSchemaInspector.TargetColumnMetadata timeTarget =
        new JdbcApplyTargetSchemaInspector.TargetColumnMetadata(
            "event_time",
            "time",
            NeutralColumnType.TIME,
            false,
            0,
            true,
            null,
            "time",
            "BASE");
    JdbcApplyTargetSchemaInspector.TargetColumnMetadata timestampTarget =
        new JdbcApplyTargetSchemaInspector.TargetColumnMetadata(
            "updated_at",
            "timestamp",
            NeutralColumnType.TIMESTAMP,
            false,
            0,
            true,
            null,
            "timestamp",
            "BASE");
    JdbcApplyTargetSchemaInspector.TargetColumnMetadata uuidTarget =
        new JdbcApplyTargetSchemaInspector.TargetColumnMetadata(
            "entity_uuid",
            "uuid",
            NeutralColumnType.UUID,
            false,
            0,
            true,
            null,
            "uuid",
            "BASE");

    LocalDate localDate = LocalDate.parse("2026-03-29");
    LocalTime localTime = LocalTime.parse("12:34:56");
    LocalDateTime localDateTime = LocalDateTime.parse("2026-03-29T12:34:56");
    UUID uuid = UUID.fromString("11111111-1111-1111-1111-111111111111");

    assertThat(TargetValueCoercions.coerce(localDate, dateTarget)).isSameAs(localDate);
    assertThat(TargetValueCoercions.coerce(localTime, timeTarget)).isSameAs(localTime);
    assertThat(TargetValueCoercions.coerce(localDateTime, timestampTarget)).isSameAs(localDateTime);
    assertThat(TargetValueCoercions.coerce(uuid, uuidTarget)).isSameAs(uuid);
  }
}
