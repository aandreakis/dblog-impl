package io.github.aandreakis.dblog.sink.ndjson;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.testsupport.ChangeEventTestFixtures;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class NdjsonChangeEventEncoderTests {
  @Test
  void encodesReadableJsonWithNestedTableAndRowContent() {
    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>();
    primaryKey.put("id", 7L);
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put("id", 7L);
    afterRow.put("name", "widget");
    afterRow.put("payload", Map.of("x", 1));

    ChangeEvent event =
        ChangeEventTestFixtures.fromRowMaps(
            new TableId("appdb", "public", "widgets"),
            OperationType.UPDATE,
            CaptureOrigin.SELECT,
            primaryKey,
            null,
            afterRow,
            new OpaqueSourcePosition("snapshot:7"),
            null,
            "dump-7");

    String json = new NdjsonChangeEventEncoder().encode(event);

    assertThat(json).contains("\"databaseName\":\"appdb\"");
    assertThat(json).contains("\"tableName\":\"widgets\"");
    assertThat(json).contains("\"primaryKey\":{\"id\":7}");
    assertThat(json).contains("\"afterRow\":{\"id\":7,\"name\":\"widget\",\"payload\":{\"x\":1}}");
    assertThat(json).contains("\"dumpId\":\"dump-7\"");
  }
}
