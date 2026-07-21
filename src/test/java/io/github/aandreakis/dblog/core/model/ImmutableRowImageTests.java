package io.github.aandreakis.dblog.core.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ImmutableRowImageTests {
  @Test
  void comparesAndHashesBinaryValuesByContent() {
    ImmutableRowImage left =
        ImmutableRowImage.of(orderedValues(new byte[] {0x01, 0x02}, "left"));
    ImmutableRowImage right =
        ImmutableRowImage.of(orderedValues(new byte[] {0x01, 0x02}, "left"));
    ImmutableRowImage different =
        ImmutableRowImage.of(orderedValues(new byte[] {0x01, 0x03}, "left"));

    assertThat(left).isEqualTo(right).hasSameHashCodeAs(right).isNotEqualTo(different);
    assertThat(Map.of(left, "value")).containsEntry(right, "value");
  }

  private static Map<String, Object> orderedValues(byte[] id, String name) {
    LinkedHashMap<String, Object> values = new LinkedHashMap<>();
    values.put("id", id);
    values.put("name", name);
    return values;
  }
}
