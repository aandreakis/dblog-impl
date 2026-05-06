package io.github.aandreakis.dblog.core.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.testsupport.ChangeEventTestFixtures;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ChangeEventTests {
  @Test
  void canonicalConstructorAcceptsImmutableRowImages() {
    ImmutableRowImage primaryKey = ImmutableRowImage.of(Map.of("id", 1L));
    ImmutableRowImage beforeRow = ImmutableRowImage.of(orderedRow("name", "before"));
    ImmutableRowImage afterRow = ImmutableRowImage.of(orderedRow("name", "after"));
    ChangeEvent event =
        new ChangeEvent(
            new TableId("sourceA", "appdb", "widgets"),
            OperationType.UPDATE,
            CaptureOrigin.LOG,
            primaryKey,
            beforeRow,
            afterRow,
            new OpaqueSourcePosition("checkpoint-1"),
            "tx-1",
            null);

    assertThat(event.primaryKey()).isSameAs(primaryKey);
    assertThat(event.beforeRow()).isSameAs(beforeRow);
    assertThat(event.afterRow()).isSameAs(afterRow);
  }

  @Test
  void trustedFactoryWrapsMapsAsImmutableRowImages() {
    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>();
    primaryKey.put("id", 1L);
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put("name", "before");

    ChangeEvent event =
        ChangeEventTestFixtures.fromRowMaps(
            new TableId("sourceA", "appdb", "widgets"),
            OperationType.UPDATE,
            CaptureOrigin.LOG,
            primaryKey,
            null,
            afterRow,
            new OpaqueSourcePosition("checkpoint-1"),
            "tx-1",
            null);

    // The factory takes a defensive copy; mutations to the source map do not escape.
    afterRow.put("name", "after");
    assertThat(event.afterRow().get("name")).isEqualTo("before");
    assertThat(event.beforeRow()).isNull();
    assertThat(event.primaryKey().get("id")).isEqualTo(1L);
  }

  @Test
  void rowImagesExposeOrderedIndexAccess() {
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put("customer_name", "alice");
    afterRow.put("status", "NEW");
    ChangeEvent event =
        ChangeEventTestFixtures.fromRowMaps(
            new TableId("sourceA", "appdb", "orders"),
            OperationType.UPDATE,
            CaptureOrigin.LOG,
            Map.of("id", 1L),
            null,
            afterRow,
            new OpaqueSourcePosition("checkpoint-2"),
            "tx-2",
            null);

    assertThat(event.afterRow().columnNames()).containsExactly("customer_name", "status");
    assertThat(event.afterRow().indexOf("customer_name")).isEqualTo(0);
    assertThat(event.afterRow().indexOf("status")).isEqualTo(1);
    assertThat(event.afterRow().valueAt(0)).isEqualTo("alice");
    assertThat(event.afterRow().valueAt(1)).isEqualTo("NEW");
  }

  @Test
  void rowImageAsMapIsUnmodifiable() {
    ChangeEvent event =
        ChangeEventTestFixtures.fromRowMaps(
            new TableId("sourceA", "appdb", "widgets"),
            OperationType.UPDATE,
            CaptureOrigin.LOG,
            Map.of("id", 1L),
            null,
            orderedRow("name", "after"),
            new OpaqueSourcePosition("checkpoint-1"),
            "tx-1",
            null);

    assertThatThrownBy(() -> event.afterRow().asMap().put("extra", "value"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void twoEventsWithEqualContentAreEqualRecordSemantics() {
    ChangeEvent a =
        ChangeEventTestFixtures.fromRowMaps(
            new TableId("sourceA", "appdb", "widgets"),
            OperationType.UPDATE,
            CaptureOrigin.LOG,
            Map.of("id", 1L),
            Map.of("name", "before"),
            Map.of("name", "after"),
            new OpaqueSourcePosition("checkpoint-1"),
            "tx-1",
            null);
    ChangeEvent b =
        ChangeEventTestFixtures.fromRowMaps(
            new TableId("sourceA", "appdb", "widgets"),
            OperationType.UPDATE,
            CaptureOrigin.LOG,
            Map.of("id", 1L),
            Map.of("name", "before"),
            Map.of("name", "after"),
            new OpaqueSourcePosition("checkpoint-1"),
            "tx-1",
            null);
    assertThat(a).isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
  }

  @Test
  void primaryKeyMustNotBeNull() {
    assertThatThrownBy(
            () ->
                new ChangeEvent(
                    new TableId("sourceA", "appdb", "widgets"),
                    OperationType.UPDATE,
                    CaptureOrigin.LOG,
                    null,
                    null,
                    ImmutableRowImage.of(Map.of("name", "after")),
                    new OpaqueSourcePosition("checkpoint-1"),
                    "tx-1",
                    null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void primaryKeyMustNotBeEmpty() {
    assertThatThrownBy(
            () ->
                new ChangeEvent(
                    new TableId("sourceA", "appdb", "widgets"),
                    OperationType.UPDATE,
                    CaptureOrigin.LOG,
                    ImmutableRowImage.of(Map.of()),
                    null,
                    ImmutableRowImage.of(Map.of("name", "after")),
                    new OpaqueSourcePosition("checkpoint-1"),
                    "tx-1",
                    null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("primaryKey");
  }

  private static Map<String, Object> orderedRow(String column, Object value) {
    LinkedHashMap<String, Object> row = new LinkedHashMap<>();
    row.put(column, value);
    return row;
  }
}
