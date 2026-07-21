package io.github.aandreakis.dblog.core.model;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * Immutable, ordered row image: a fixed column list with one value per column. Replaces the
 * previous {@code Map<String, Object>}-carrying row fields on {@link ChangeEvent}.
 *
 * <p>Two construction paths:
 *
 * <ul>
 *   <li>{@link #of(Map)} wraps an existing column-to-value map. Preserves iteration order and
 *       takes a defensive copy so callers can continue to mutate their source map.
 *   <li>{@link #ofLayout(RowLayout, Object[])} is the zero-copy, layout-cached path: adapters
 *       build one {@link RowLayout} per schema shape, then emit per-event images that share that
 *       layout. The per-event allocation is this image plus the {@code Object[]} values.
 * </ul>
 *
 * <p>Equality and hashing are by value over {@code (columnNames, values)} so that instances
 * carrying identical content compare equal — essential for this type to be a field of the
 * {@link ChangeEvent} record without breaking record equality semantics.
 */
public final class ImmutableRowImage {
  private static final ImmutableRowImage EMPTY =
      new ImmutableRowImage(List.of(), Map.of(), new Object[0], Map.of());

  private final List<String> columnNames;
  private final Map<String, Integer> indexByName;
  private final Object[] values;
  private final Map<String, Object> asMap;

  private ImmutableRowImage(
      List<String> columnNames,
      Map<String, Integer> indexByName,
      Object[] values,
      Map<String, Object> asMap) {
    this.columnNames = columnNames;
    this.indexByName = indexByName;
    this.values = values;
    this.asMap = asMap;
  }

  /**
   * Wraps a column-to-value map. Iteration order of the map defines the column order. A
   * defensive copy is made so later mutations to the source map do not escape.
   */
  public static ImmutableRowImage of(Map<String, Object> values) {
    Objects.requireNonNull(values, "values");
    if (values.isEmpty()) {
      return EMPTY;
    }
    LinkedHashMap<String, Object> copy = new LinkedHashMap<>(values);
    List<String> columnNames = List.copyOf(copy.keySet());
    Object[] valueArray = new Object[columnNames.size()];
    LinkedHashMap<String, Integer> indexByName = new LinkedHashMap<>(columnNames.size());
    for (int index = 0; index < columnNames.size(); index++) {
      String name = columnNames.get(index);
      valueArray[index] = copy.get(name);
      indexByName.put(name, index);
    }
    return new ImmutableRowImage(
        columnNames,
        Collections.unmodifiableMap(indexByName),
        valueArray,
        Collections.unmodifiableMap(copy));
  }

  /**
   * Zero-copy path: the caller provides a shared {@link RowLayout} and a values array of the
   * matching length. The array is retained by reference; callers must not mutate it after the
   * call. Used on the adapter hot path where the layout is cached per schema.
   */
  public static ImmutableRowImage ofLayout(RowLayout layout, Object[] values) {
    Objects.requireNonNull(layout, "layout");
    Objects.requireNonNull(values, "values");
    if (layout.size() != values.length) {
      throw new IllegalArgumentException(
          "layout size "
              + layout.size()
              + " does not match values length "
              + values.length);
    }
    if (layout.size() == 0) {
      return EMPTY;
    }
    return new ImmutableRowImage(layout.columnNames(), layout.indexByName(), values, null);
  }

  public List<String> columnNames() {
    return columnNames;
  }

  public int size() {
    return columnNames.size();
  }

  public boolean isEmpty() {
    return columnNames.isEmpty();
  }

  public boolean containsKey(String key) {
    return indexByName.containsKey(key);
  }

  public Object get(String key) {
    Integer index = indexByName.get(key);
    return index == null ? null : values[index];
  }

  public int indexOf(String key) {
    Objects.requireNonNull(key, "key");
    Integer index = indexByName.get(key);
    return index == null ? -1 : index;
  }

  public Object valueAt(int index) {
    if (index < 0 || index >= values.length) {
      throw new IndexOutOfBoundsException(
          "row image index out of bounds: " + index + " size=" + values.length);
    }
    return values[index];
  }

  /** Iterates (columnName, value) pairs in column order. */
  public void forEach(BiConsumer<String, Object> action) {
    Objects.requireNonNull(action, "action");
    for (int index = 0; index < values.length; index++) {
      action.accept(columnNames.get(index), values[index]);
    }
  }

  /**
   * Returns a {@code Map<String, Object>} view preserving column order. The view is lazily
   * materialized for layout-cached instances and cached; subsequent calls return the same map.
   * Useful for code that must interoperate with {@code Map}-shaped APIs (JSON serialization,
   * legacy codecs). Prefer {@link #get}, {@link #valueAt}, or {@link #forEach} in new code.
   */
  public Map<String, Object> asMap() {
    Map<String, Object> existing = asMap;
    if (existing != null) {
      return existing;
    }
    LinkedHashMap<String, Object> rebuilt = new LinkedHashMap<>(values.length);
    for (int index = 0; index < values.length; index++) {
      rebuilt.put(columnNames.get(index), values[index]);
    }
    return Collections.unmodifiableMap(rebuilt);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof ImmutableRowImage that)) {
      return false;
    }
    return columnNames.equals(that.columnNames) && Arrays.deepEquals(values, that.values);
  }

  @Override
  public int hashCode() {
    return 31 * columnNames.hashCode() + Arrays.deepHashCode(values);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(64).append('{');
    for (int index = 0; index < values.length; index++) {
      if (index > 0) {
        builder.append(", ");
      }
      builder.append(columnNames.get(index)).append('=').append(values[index]);
    }
    return builder.append('}').toString();
  }
}
