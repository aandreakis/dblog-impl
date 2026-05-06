package io.github.aandreakis.dblog.core.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Shared descriptor for the column shape of a row image. Adapters cache one {@link RowLayout}
 * per observed row shape (typically per {@link io.github.aandreakis.dblog.core.schema.TableSchema}
 * fingerprint) so that every {@link ImmutableRowImage} built against that shape shares the same
 * column-name list and name-to-index map by reference. Per-event allocation shrinks to just the
 * {@code Object[]} carrying the actual row values.
 */
public record RowLayout(List<String> columnNames, Map<String, Integer> indexByName) {

  public RowLayout {
    columnNames = List.copyOf(Objects.requireNonNull(columnNames, "columnNames"));
    indexByName = Map.copyOf(Objects.requireNonNull(indexByName, "indexByName"));
    if (columnNames.size() != indexByName.size()) {
      throw new IllegalArgumentException(
          "columnNames and indexByName must have the same size: "
              + columnNames.size()
              + " vs "
              + indexByName.size());
    }
  }

  /** Builds a layout from an ordered list of column names. Computes the index map once. */
  public static RowLayout forColumns(List<String> columnNames) {
    Objects.requireNonNull(columnNames, "columnNames");
    LinkedHashMap<String, Integer> byName = new LinkedHashMap<>(columnNames.size());
    for (int index = 0; index < columnNames.size(); index++) {
      String name = columnNames.get(index);
      if (byName.put(name, index) != null) {
        throw new IllegalArgumentException("duplicate column name: " + name);
      }
    }
    return new RowLayout(columnNames, Collections.unmodifiableMap(byName));
  }

  public int size() {
    return columnNames.size();
  }
}
