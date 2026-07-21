package io.github.aandreakis.dblog.core.schema;

import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class PrimaryKeyTuple implements Comparable<PrimaryKeyTuple> {
  private final List<String> columnNames;
  private final List<PrimaryKeyValue> values;
  private final String literal;
  private final int hashCode;

  private PrimaryKeyTuple(List<String> columnNames, List<PrimaryKeyValue> values, String literal) {
    this.columnNames = List.copyOf(Objects.requireNonNull(columnNames, "columnNames"));
    this.values = List.copyOf(Objects.requireNonNull(values, "values"));
    this.literal = Objects.requireNonNull(literal, "literal");
    if (this.columnNames.isEmpty()) {
      throw new IllegalArgumentException("primary key tuple must contain at least one column");
    }
    if (this.columnNames.size() != this.values.size()) {
      throw new IllegalArgumentException(
          "primary key column names and values must have the same size");
    }
    this.hashCode = Objects.hash(this.columnNames, this.values, this.literal);
  }

  public static PrimaryKeyTuple fromValues(
      List<String> columnNames, List<PrimaryKeyValue> values) {
    List<String> requiredColumnNames = List.copyOf(Objects.requireNonNull(columnNames, "columnNames"));
    List<PrimaryKeyValue> requiredValues = List.copyOf(Objects.requireNonNull(values, "values"));
    return new PrimaryKeyTuple(requiredColumnNames, requiredValues, literalFor(requiredColumnNames, requiredValues));
  }

  public static PrimaryKeyTuple fromRow(List<ColumnDefinition> columns, Map<String, Object> row) {
    Objects.requireNonNull(row, "row");
    List<String> columnNames = new ArrayList<>(columns.size());
    List<PrimaryKeyValue> values = new ArrayList<>(columns.size());
    for (ColumnDefinition column : requireColumns(columns)) {
      Object rawValue = row.get(column.name());
      if (rawValue == null) {
        throw new IllegalArgumentException("row is missing primary key column: " + column.name());
      }
      columnNames.add(column.name());
      values.add(PrimaryKeyValue.fromColumn(column, rawValue));
    }
    return new PrimaryKeyTuple(columnNames, values, literalFor(columnNames, values));
  }

  /**
   * Zero-copy path that reads primary-key values directly from a row image via its
   * layout-cached {@code get(columnName)}, skipping the {@code asMap()} materialization.
   */
  public static PrimaryKeyTuple fromRowImage(
      List<ColumnDefinition> columns,
      ImmutableRowImage row) {
    Objects.requireNonNull(row, "row");
    List<String> columnNames = new ArrayList<>(columns.size());
    List<PrimaryKeyValue> values = new ArrayList<>(columns.size());
    for (ColumnDefinition column : requireColumns(columns)) {
      Object rawValue = row.get(column.name());
      if (rawValue == null) {
        throw new IllegalArgumentException("row is missing primary key column: " + column.name());
      }
      columnNames.add(column.name());
      values.add(PrimaryKeyValue.fromColumn(column, rawValue));
    }
    return new PrimaryKeyTuple(columnNames, values, literalFor(columnNames, values));
  }

  public static PrimaryKeyTuple fromLiteral(List<ColumnDefinition> columns, String literal) {
    Objects.requireNonNull(literal, "literal");
    List<ColumnDefinition> requiredColumns = requireColumns(columns);
    if (requiredColumns.size() == 1 && !looksComposite(literal)) {
      ColumnDefinition column = requiredColumns.get(0);
      PrimaryKeyValue single = PrimaryKeyValue.fromLiteral(column, literal);
      return new PrimaryKeyTuple(List.of(column.name()), List.of(single), single.literal());
    }

    Map<String, String> encodedValues =
        looksNamedComposite(literal)
            ? parseNamedCompositeLiteral(literal)
            : parsePositionalCompositeLiteral(requiredColumns, literal);
    List<String> columnNames = new ArrayList<>(requiredColumns.size());
    List<PrimaryKeyValue> values = new ArrayList<>(requiredColumns.size());
    for (ColumnDefinition column : requiredColumns) {
      String encodedValue = encodedValues.get(column.name());
      if (encodedValue == null) {
        throw new IllegalArgumentException(
            "composite primary key literal is missing column " + column.name());
      }
      columnNames.add(column.name());
      values.add(PrimaryKeyValue.fromLiteral(column, encodedValue));
    }
    if (encodedValues.size() != requiredColumns.size()) {
      throw new IllegalArgumentException(
          "composite primary key literal contains unexpected columns: " + encodedValues.keySet());
    }
    return new PrimaryKeyTuple(columnNames, values, literalFor(columnNames, values));
  }

  public String literal() {
    return literal;
  }

  public List<String> columnNames() {
    return columnNames;
  }

  public List<PrimaryKeyValue> values() {
    return values;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof PrimaryKeyTuple that)) {
      return false;
    }
    return columnNames.equals(that.columnNames)
        && values.equals(that.values)
        && literal.equals(that.literal);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public int compareTo(PrimaryKeyTuple other) {
    Objects.requireNonNull(other, "other");
    if (!columnNames.equals(other.columnNames)) {
      throw new IllegalArgumentException(
          "cannot compare different primary-key column sets: "
              + columnNames
              + " vs "
              + other.columnNames);
    }
    for (int index = 0; index < values.size(); index++) {
      int compared = values.get(index).compareTo(other.values.get(index));
      if (compared != 0) {
        return compared;
      }
    }
    return 0;
  }

  private static List<ColumnDefinition> requireColumns(List<ColumnDefinition> columns) {
    List<ColumnDefinition> requiredColumns =
        List.copyOf(Objects.requireNonNull(columns, "columns"));
    if (requiredColumns.isEmpty()) {
      throw new IllegalArgumentException("primary key tuple must contain at least one column");
    }
    return requiredColumns;
  }

  private static boolean looksComposite(String literal) {
    return looksNamedComposite(literal) || looksPositionalComposite(literal);
  }

  private static boolean looksNamedComposite(String literal) {
    return literal.startsWith("{") && literal.endsWith("}");
  }

  private static boolean looksPositionalComposite(String literal) {
    return literal.startsWith("(") && literal.endsWith(")");
  }

  private static String literalFor(List<String> columnNames, List<PrimaryKeyValue> values) {
    if (values.size() == 1) {
      return values.get(0).literal();
    }
    StringBuilder builder = new StringBuilder();
    builder.append('{');
    for (int index = 0; index < values.size(); index++) {
      if (index > 0) {
        builder.append(',');
      }
      builder.append(escape(columnNames.get(index)));
      builder.append('=');
      builder.append(escape(values.get(index).literal()));
    }
    builder.append('}');
    return builder.toString();
  }

  private static Map<String, String> parseNamedCompositeLiteral(String literal) {
    if (!looksNamedComposite(literal)) {
      throw new IllegalArgumentException(
          "composite primary key literal must use {column=value,...} syntax: " + literal);
    }
    String body = literal.substring(1, literal.length() - 1);
    LinkedHashMap<String, String> values = new LinkedHashMap<>();
    if (body.isEmpty()) {
      return values;
    }
    for (String entry : splitEscaped(body, ',')) {
      int separator = firstUnescaped(entry, '=');
      if (separator <= 0) {
        throw new IllegalArgumentException("invalid composite primary key literal entry: " + entry);
      }
      String columnName = unescape(entry.substring(0, separator));
      String value = unescape(entry.substring(separator + 1));
      if (values.put(columnName, value) != null) {
        throw new IllegalArgumentException(
            "duplicate column in composite primary key literal: " + columnName);
      }
    }
    return Map.copyOf(values);
  }

  private static Map<String, String> parsePositionalCompositeLiteral(
      List<ColumnDefinition> requiredColumns, String literal) {
    if (!looksPositionalComposite(literal)) {
      throw new IllegalArgumentException(
          "composite primary key literal must use either {column=value,...} or (value1,value2,...) syntax: "
              + literal);
    }
    String body = literal.substring(1, literal.length() - 1);
    List<String> rawValues = body.isEmpty() ? List.of() : splitEscaped(body, ',');
    if (rawValues.size() != requiredColumns.size()) {
      throw new IllegalArgumentException(
          "composite primary key literal provides "
              + rawValues.size()
              + " values but expected "
              + requiredColumns.size()
              + ": "
              + literal);
    }
    LinkedHashMap<String, String> values = new LinkedHashMap<>();
    for (int index = 0; index < requiredColumns.size(); index++) {
      values.put(requiredColumns.get(index).name(), unescape(rawValues.get(index)));
    }
    return Map.copyOf(values);
  }

  private static List<String> splitEscaped(String value, char separator) {
    List<String> tokens = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    boolean escaping = false;
    for (int index = 0; index < value.length(); index++) {
      char character = value.charAt(index);
      if (escaping) {
        current.append(character);
        escaping = false;
        continue;
      }
      if (character == '\\') {
        current.append(character);
        escaping = true;
        continue;
      }
      if (character == separator) {
        tokens.add(current.toString());
        current.setLength(0);
        continue;
      }
      current.append(character);
    }
    if (escaping) {
      throw new IllegalArgumentException("dangling escape in composite primary key literal: " + value);
    }
    tokens.add(current.toString());
    return List.copyOf(tokens);
  }

  private static int firstUnescaped(String value, char target) {
    boolean escaping = false;
    for (int index = 0; index < value.length(); index++) {
      char character = value.charAt(index);
      if (escaping) {
        escaping = false;
        continue;
      }
      if (character == '\\') {
        escaping = true;
        continue;
      }
      if (character == target) {
        return index;
      }
    }
    return -1;
  }

  private static String escape(String value) {
    return value
        .replace("\\", "\\\\")
        .replace(",", "\\,")
        .replace("=", "\\=")
        .replace("{", "\\{")
        .replace("}", "\\}");
  }

  private static String unescape(String value) {
    StringBuilder unescaped = new StringBuilder();
    boolean escaping = false;
    for (int index = 0; index < value.length(); index++) {
      char character = value.charAt(index);
      if (escaping) {
        unescaped.append(character);
        escaping = false;
        continue;
      }
      if (character == '\\') {
        escaping = true;
        continue;
      }
      unescaped.append(character);
    }
    if (escaping) {
      throw new IllegalArgumentException("dangling escape in composite primary key literal: " + value);
    }
    return unescaped.toString();
  }
}
