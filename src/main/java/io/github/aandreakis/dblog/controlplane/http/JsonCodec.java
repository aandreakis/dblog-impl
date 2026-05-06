package io.github.aandreakis.dblog.controlplane.http;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

final class JsonCodec {
  private JsonCodec() {}

  static String encode(Object value) {
    StringBuilder builder = new StringBuilder();
    appendValue(builder, value);
    return builder.toString();
  }

  static Object parse(String json) {
    return new Parser(json).parseDocument();
  }

  @SuppressWarnings("unchecked")
  private static void appendValue(StringBuilder builder, Object value) {
    if (value == null) {
      builder.append("null");
      return;
    }
    if (value instanceof String string) {
      appendString(builder, string);
      return;
    }
    if (value instanceof Number || value instanceof Boolean) {
      builder.append(String.valueOf(value));
      return;
    }
    if (value instanceof Map<?, ?> map) {
      builder.append('{');
      boolean first = true;
      for (Map.Entry<String, Object> entry : ((Map<String, Object>) map).entrySet()) {
        if (!first) {
          builder.append(',');
        }
        appendString(builder, entry.getKey());
        builder.append(':');
        appendValue(builder, entry.getValue());
        first = false;
      }
      builder.append('}');
      return;
    }
    if (value instanceof List<?> list) {
      builder.append('[');
      boolean first = true;
      for (Object element : list) {
        if (!first) {
          builder.append(',');
        }
        appendValue(builder, element);
        first = false;
      }
      builder.append(']');
      return;
    }
    appendString(builder, String.valueOf(value));
  }

  private static void appendString(StringBuilder builder, String value) {
    builder.append('"');
    for (int index = 0; index < value.length(); index++) {
      char ch = value.charAt(index);
      switch (ch) {
        case '"' -> builder.append("\\\"");
        case '\\' -> builder.append("\\\\");
        case '\b' -> builder.append("\\b");
        case '\f' -> builder.append("\\f");
        case '\n' -> builder.append("\\n");
        case '\r' -> builder.append("\\r");
        case '\t' -> builder.append("\\t");
        default -> {
          if (ch < 0x20) {
            builder.append(String.format("\\u%04x", (int) ch));
          } else {
            builder.append(ch);
          }
        }
      }
    }
    builder.append('"');
  }

  private static final class Parser {
    /**
     * Hard cap on the nesting depth of arrays and objects the parser will enter.
     *
     * <p>The parser is intentionally recursive for readability; unbounded nesting would let a
     * caller inside the loopback control plane blow the handler thread's stack with an
     * 80 KB payload (well under {@code max-request-body-bytes}), killing the exchange mid-parse
     * with a {@link StackOverflowError} and leaving the client with a socket hang up. Real
     * control-plane requests are 3-4 levels deep; 64 is ~16× that headroom and any genuine
     * DBLog payload beyond it is either a bug or adversarial.
     *
     * <p>Exceeding the cap surfaces as {@link IllegalArgumentException}, which the HTTP layer
     * already maps to {@code 400 bad_request} — matching the failure mode of every other
     * malformed-body rejection rather than a connection drop.
     */
    private static final int MAX_NESTING_DEPTH = 64;

    private final String json;
    private int index;
    private int depth;

    private Parser(String json) {
      this.json = Objects.requireNonNull(json, "json");
    }

    private void enterNesting() {
      depth++;
      if (depth > MAX_NESTING_DEPTH) {
        throw new IllegalArgumentException(
            "JSON nesting depth exceeds limit of " + MAX_NESTING_DEPTH);
      }
    }

    private void exitNesting() {
      depth--;
    }

    private Object parseDocument() {
      Object value = parseValue();
      skipWhitespace();
      if (index != json.length()) {
        throw new IllegalArgumentException("unexpected trailing JSON content");
      }
      return value;
    }

    private Object parseValue() {
      skipWhitespace();
      if (index >= json.length()) {
        throw new IllegalArgumentException("unexpected end of JSON");
      }
      char ch = json.charAt(index);
      return switch (ch) {
        case '{' -> parseObject();
        case '[' -> parseArray();
        case '"' -> parseString();
        case 't' -> parseLiteral("true", Boolean.TRUE);
        case 'f' -> parseLiteral("false", Boolean.FALSE);
        case 'n' -> parseLiteral("null", null);
        default -> parseNumber();
      };
    }

    private Map<String, Object> parseObject() {
      enterNesting();
      try {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        expect('{');
        skipWhitespace();
        if (peek('}')) {
          expect('}');
          return map;
        }
        while (true) {
          String key = parseString();
          skipWhitespace();
          expect(':');
          Object value = parseValue();
          map.put(key, value);
          skipWhitespace();
          if (peek('}')) {
            expect('}');
            return map;
          }
          expect(',');
        }
      } finally {
        exitNesting();
      }
    }

    private List<Object> parseArray() {
      enterNesting();
      try {
        List<Object> list = new ArrayList<>();
        expect('[');
        skipWhitespace();
        if (peek(']')) {
          expect(']');
          return list;
        }
        while (true) {
          list.add(parseValue());
          skipWhitespace();
          if (peek(']')) {
            expect(']');
            return list;
          }
          expect(',');
        }
      } finally {
        exitNesting();
      }
    }

    private String parseString() {
      expect('"');
      StringBuilder builder = new StringBuilder();
      while (index < json.length()) {
        char ch = json.charAt(index++);
        if (ch == '"') {
          return builder.toString();
        }
        if (ch == '\\') {
          if (index >= json.length()) {
            throw new IllegalArgumentException("unterminated escape");
          }
          char escaped = json.charAt(index++);
          switch (escaped) {
            case '"', '\\', '/' -> builder.append(escaped);
            case 'b' -> builder.append('\b');
            case 'f' -> builder.append('\f');
            case 'n' -> builder.append('\n');
            case 'r' -> builder.append('\r');
            case 't' -> builder.append('\t');
            case 'u' -> {
              if (index + 4 > json.length()) {
                throw new IllegalArgumentException("unterminated unicode escape");
              }
              String hex = json.substring(index, index + 4);
              builder.append((char) Integer.parseInt(hex, 16));
              index += 4;
            }
            default -> throw new IllegalArgumentException("unsupported escape: \\" + escaped);
          }
        } else {
          builder.append(ch);
        }
      }
      throw new IllegalArgumentException("unterminated string");
    }

    private Object parseLiteral(String expected, Object value) {
      if (!json.startsWith(expected, index)) {
        throw new IllegalArgumentException("expected " + expected);
      }
      index += expected.length();
      return value;
    }

    private Number parseNumber() {
      int start = index;
      while (index < json.length()) {
        char ch = json.charAt(index);
        if ((ch >= '0' && ch <= '9') || ch == '-' || ch == '+' || ch == '.' || ch == 'e' || ch == 'E') {
          index++;
        } else {
          break;
        }
      }
      String token = json.substring(start, index);
      if (token.contains(".") || token.contains("e") || token.contains("E")) {
        return Double.parseDouble(token);
      }
      return Long.parseLong(token);
    }

    private void expect(char expected) {
      skipWhitespace();
      if (index >= json.length() || json.charAt(index) != expected) {
        throw new IllegalArgumentException("expected '" + expected + "'");
      }
      index++;
    }

    private boolean peek(char expected) {
      skipWhitespace();
      return index < json.length() && json.charAt(index) == expected;
    }

    private void skipWhitespace() {
      while (index < json.length() && Character.isWhitespace(json.charAt(index))) {
        index++;
      }
    }
  }
}
