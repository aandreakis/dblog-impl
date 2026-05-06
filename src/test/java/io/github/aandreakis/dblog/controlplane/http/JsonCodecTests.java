package io.github.aandreakis.dblog.controlplane.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonCodecTests {

  @Test
  void acceptsNestingAtTheExactDepthLimit() {
    // 64 is the parser's hard cap (see JsonCodec.Parser.MAX_NESTING_DEPTH). A payload nested
    // exactly at the limit must still parse — crossing is what fails closed, not arriving.
    String atLimit = nestedObject(64);

    Object parsed = JsonCodec.parse(atLimit);

    assertThat(deepestValue(parsed)).isNull();
    assertThat(nestedDepth(parsed)).isEqualTo(64);
  }

  @Test
  void rejectsNestingJustPastTheDepthLimitWithBadRequestShape() {
    // One level deeper than the cap must fail with IllegalArgumentException — the HTTP
    // layer maps that to 400 bad_request, matching every other malformed-body rejection.
    // Without the depth cap a comparable payload (thousands deep) would StackOverflowError
    // on the handler thread and the client would see a connection drop.
    String overLimit = nestedObject(65);

    assertThatThrownBy(() -> JsonCodec.parse(overLimit))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("nesting depth exceeds limit");
  }

  @Test
  void rejectsVeryDeepNestingThatWouldOtherwiseStackOverflow() {
    // Full repro of the original review-report finding: ~10 000 levels of nested objects
    // in well under 1 MiB. Before the cap this blew the thread stack. The cap turns it
    // into a clean IllegalArgumentException at level 65.
    String veryDeep = nestedObject(10_000);

    assertThatThrownBy(() -> JsonCodec.parse(veryDeep))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("nesting depth exceeds limit");
  }

  @Test
  void acceptsFlatArraysWithManyElementsBecauseOnlyNestingCountsAgainstTheCap() {
    // A 10 000-element flat array uses only one level of nesting. The cap is on structural
    // nesting depth, not element count; legitimate primaryKeyLiterals batches must still
    // parse even when the caller sends thousands of keys.
    StringBuilder flat = new StringBuilder("[");
    for (int i = 0; i < 10_000; i++) {
      if (i > 0) flat.append(',');
      flat.append(i);
    }
    flat.append(']');

    Object parsed = JsonCodec.parse(flat.toString());

    assertThat(parsed).isInstanceOf(List.class);
    assertThat(((List<?>) parsed)).hasSize(10_000);
  }

  @Test
  void countsArrayAndObjectFramesAgainstTheSameDepthBudget() {
    // The cap must cover BOTH constructors symmetrically — an attacker could otherwise
    // alternate [{[{...}]}] to bypass a per-type counter. Here 32 pairs = 64 levels and
    // sits exactly at the limit; 33 pairs = 66 levels is rejected.
    String atLimit = alternatingArrayObject(32);
    String pastLimit = alternatingArrayObject(33);

    Object parsed = JsonCodec.parse(atLimit);
    assertThat(parsed).isInstanceOf(List.class);

    assertThatThrownBy(() -> JsonCodec.parse(pastLimit))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("nesting depth exceeds limit");
  }

  @Test
  void typicalControlPlaneRequestsAreNowhereNearTheCap() {
    // Regression guard for a routine primary-keys repair request: 4 structural levels
    // (object > table-object, array-of-strings). The cap leaves generous headroom for
    // real DBLog payloads; accidentally tightening it would break this shape.
    String payload =
        "{\"scope\":\"PRIMARY_KEYS\","
            + "\"table\":{\"databaseName\":\"src\",\"schemaName\":\"app\",\"tableName\":\"t\"},"
            + "\"primaryKeyLiterals\":[\"1\",\"2\",\"3\"]}";

    @SuppressWarnings("unchecked")
    Map<String, Object> parsed = (Map<String, Object>) JsonCodec.parse(payload);

    assertThat(parsed).containsEntry("scope", "PRIMARY_KEYS");
    assertThat(parsed).containsKey("table");
    assertThat(parsed).containsKey("primaryKeyLiterals");
  }

  private static String nestedObject(int depth) {
    StringBuilder opens = new StringBuilder();
    StringBuilder closes = new StringBuilder();
    for (int i = 0; i < depth; i++) {
      opens.append("{\"a\":");
      closes.append('}');
    }
    return opens.append("null").append(closes).toString();
  }

  private static String alternatingArrayObject(int pairs) {
    StringBuilder opens = new StringBuilder();
    StringBuilder closes = new StringBuilder();
    for (int i = 0; i < pairs; i++) {
      opens.append("[{\"a\":");
      closes.append("}]");
    }
    return opens.append("null").append(closes).toString();
  }

  private static Object deepestValue(Object node) {
    while (node instanceof Map<?, ?> map) {
      node = map.values().iterator().next();
    }
    return node;
  }

  private static int nestedDepth(Object node) {
    int depth = 0;
    while (node instanceof Map<?, ?> map && !map.isEmpty()) {
      depth++;
      node = map.values().iterator().next();
    }
    return depth;
  }
}
