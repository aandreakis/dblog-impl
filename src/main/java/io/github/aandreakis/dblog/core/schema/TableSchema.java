package io.github.aandreakis.dblog.core.schema;

import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import io.github.aandreakis.dblog.core.model.PrimaryKeyHash;
import io.github.aandreakis.dblog.core.model.RowLayout;
import io.github.aandreakis.dblog.core.model.TableId;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.stream.Collectors;

/**
 * Neutral schema contract for one captured source table.
 *
 * <p>The full {@code columns} list records the observed source shape, but DBLog's emitted-row
 * contract is defined by the selected column surface: {@code columns - ignoredColumns}, with the
 * declared primary-key columns always retained. {@code fingerprint} is computed from that selected
 * surface rather than from every live source column, so unsupported non-key columns can be
 * observed and ignored without silently changing the dump/event payload shape.
 *
 * <p>Dump progress persists this fingerprint across chunk boundaries and restart so coordination
 * can detect drift against a request's assumptions instead of mutating output shape mid-request.
 */
public record TableSchema(
    TableId tableId,
    List<ColumnDefinition> columns,
    List<String> primaryKeyColumns,
    String fingerprint,
    Instant refreshedAt,
    List<String> ignoredColumns) {
  private static final Map<TableSchema, CachedViews> CACHED_VIEWS =
      Collections.synchronizedMap(new WeakHashMap<>());

  public TableSchema {
    Objects.requireNonNull(tableId, "tableId");
    Objects.requireNonNull(columns, "columns");
    Objects.requireNonNull(primaryKeyColumns, "primaryKeyColumns");
    Objects.requireNonNull(refreshedAt, "refreshedAt");

    columns = List.copyOf(columns);
    primaryKeyColumns = List.copyOf(primaryKeyColumns);
    ignoredColumns = ignoredColumns == null ? List.of() : List.copyOf(ignoredColumns);
    fingerprint =
        fingerprint == null ? fingerprintFor(tableId, columns, ignoredColumns) : fingerprint;

    if (columns.isEmpty()) {
      throw new IllegalArgumentException("columns must not be empty");
    }
    if (primaryKeyColumns.isEmpty()) {
      throw new IllegalArgumentException("table must declare at least one primary key column");
    }
    Set<String> columnNames = columns.stream().map(ColumnDefinition::name).collect(Collectors.toSet());
    for (String primaryKeyColumn : primaryKeyColumns) {
      if (!columnNames.contains(primaryKeyColumn)) {
        throw new IllegalArgumentException(
            "schema is missing declared primary key column: " + primaryKeyColumn);
      }
    }
  }

  public static TableSchema create(TableId tableId, List<ColumnDefinition> columns, Instant refreshedAt) {
    List<String> ignored = new ArrayList<>();
    List<PrimaryKeyCandidate> primaryKeys = new ArrayList<>();
    for (int index = 0; index < columns.size(); index++) {
      ColumnDefinition column = columns.get(index);
      if (column.primaryKey()) {
        primaryKeys.add(new PrimaryKeyCandidate(column.primaryKeyOrdinal(), index, column.name()));
      }
      if (!column.supported() && !column.primaryKey()) {
        ignored.add(column.name());
      }
    }
    if (primaryKeys.isEmpty()) {
      throw new IllegalArgumentException("table must declare at least one primary key column");
    }
    primaryKeys.sort((left, right) -> {
      int byOrdinal = Integer.compare(left.ordinal(), right.ordinal());
      return byOrdinal != 0 ? byOrdinal : Integer.compare(left.index(), right.index());
    });
    return new TableSchema(
        tableId,
        columns,
        primaryKeys.stream().map(PrimaryKeyCandidate::name).toList(),
        null,
        refreshedAt,
        ignored);
  }

  public List<ColumnDefinition> selectedColumns() {
    return cachedViews().selectedColumns();
  }

  public List<String> selectedColumnNames() {
    return cachedViews().selectedColumnNames();
  }

  /**
   * Shared {@link RowLayout} for rows that match the selected-column set of this schema.
   * Layout-cached so every zero-copy {@link ImmutableRowImage}
   * built for this schema shares one column-name list and one name-to-index map by reference.
   */
  public RowLayout selectedRowLayout() {
    return cachedViews().selectedRowLayout();
  }

  /** Shared {@link RowLayout} for primary-key-only row projections. */
  public RowLayout primaryKeyRowLayout() {
    return cachedViews().primaryKeyRowLayout();
  }

  public boolean hasSinglePrimaryKey() {
    return primaryKeyColumns.size() == 1;
  }

  public String primaryKeyColumn() {
    if (!hasSinglePrimaryKey()) {
      throw new IllegalStateException(
          "schema requires a single-column primary key but found " + primaryKeyColumns);
    }
    return primaryKeyColumns.get(0);
  }

  public ColumnDefinition primaryKeyDefinition() {
    return requireSinglePrimaryKeyDefinition("schema requires a single-column primary key");
  }

  public List<ColumnDefinition> primaryKeyDefinitions() {
    return cachedViews().primaryKeyDefinitions();
  }

  public String primaryKeyFingerprint() {
    return fingerprintFor(tableId, primaryKeyDefinitions(), List.of());
  }

  public PrimaryKeyTuple primaryKeyTupleFor(Map<String, Object> row) {
    return PrimaryKeyTuple.fromRow(primaryKeyDefinitions(), row);
  }

  public PrimaryKeyTuple primaryKeyTupleFor(ImmutableRowImage row) {
    return PrimaryKeyTuple.fromRowImage(primaryKeyDefinitions(), row);
  }

  /**
   * Builds a lightweight {@link PrimaryKeyHash}
   * by normalising each primary-key column's value and wrapping the resulting {@code Object[]}
   * by reference. Per-call allocation is the values array plus the hash record — no per-column
   * wrappers, no literal string.
   *
   * <p>Returns {@code null} when any primary-key column has a neutral type that cannot be
   * normalised (JSON, XML, UNSUPPORTED). Such tables are rejected at dump-request orchestration
   * time, so reconciliation never runs against them; leaving the hash unset means log-only
   * consumers still see the raw primary-key image without the adapter tripping an eager throw.
   */
  public PrimaryKeyHash primaryKeyHashFor(ImmutableRowImage row) {
    Objects.requireNonNull(row, "row");
    List<ColumnDefinition> primaryKeyDefinitions = primaryKeyDefinitions();
    for (ColumnDefinition definition : primaryKeyDefinitions) {
      if (!PrimaryKeyValue.isSupportedPrimaryKeyType(definition.neutralType())) {
        return null;
      }
    }
    Object[] normalized = new Object[primaryKeyDefinitions.size()];
    for (int index = 0; index < primaryKeyDefinitions.size(); index++) {
      ColumnDefinition definition = primaryKeyDefinitions.get(index);
      Object rawValue = row.get(definition.name());
      if (rawValue == null) {
        throw new IllegalArgumentException("row is missing primary key column: " + definition.name());
      }
      normalized[index] = PrimaryKeyValue.normalizedValueFor(definition, rawValue);
    }
    return PrimaryKeyHash.wrapSharedArray(normalized);
  }

  /**
   * Zero-copy variant when the caller already has normalised PK values at hand (typical for
   * adapter log decoders that produce neutral values directly).
   */
  public PrimaryKeyHash primaryKeyHashFromNormalizedValues(Object[] normalizedValues) {
    Objects.requireNonNull(normalizedValues, "normalizedValues");
    if (normalizedValues.length != primaryKeyColumns.size()) {
      throw new IllegalArgumentException(
          "normalizedValues length " + normalizedValues.length
              + " does not match primary key width " + primaryKeyColumns.size());
    }
    return PrimaryKeyHash.wrapSharedArray(normalizedValues);
  }

  public List<PrimaryKeyTuple> primaryKeyTuplesFromLiterals(List<String> literals) {
    List<String> requiredLiterals = List.copyOf(Objects.requireNonNull(literals, "literals"));
    List<PrimaryKeyTuple> tuples = new ArrayList<>(requiredLiterals.size());
    for (String literal : requiredLiterals) {
      tuples.add(primaryKeyTupleFromLiteral(literal));
    }
    return List.copyOf(tuples);
  }

  public List<String> primaryKeyLiteralsForTuples(List<PrimaryKeyTuple> tuples) {
    List<PrimaryKeyTuple> requiredTuples = List.copyOf(Objects.requireNonNull(tuples, "tuples"));
    List<String> literals = new ArrayList<>(requiredTuples.size());
    for (PrimaryKeyTuple tuple : requiredTuples) {
      literals.add(primaryKeyLiteralFor(tuple));
    }
    return List.copyOf(literals);
  }

  public PrimaryKeyValue primaryKeyValueFor(Map<String, Object> row) {
    ColumnDefinition primaryKeyDefinition =
        requireSinglePrimaryKeyDefinition(
            "row primary-key literal utilities require a single-column primary key");
    Object value = row.get(primaryKeyDefinition.name());
    if (value == null) {
      throw new IllegalArgumentException(
          "row is missing primary key column: " + primaryKeyDefinition.name());
    }
    return PrimaryKeyValue.fromColumn(primaryKeyDefinition, value);
  }

  public Map<String, Object> primaryKeyRow(Map<String, Object> row) {
    Objects.requireNonNull(row, "row");
    List<ColumnDefinition> primaryKeyDefinitions = primaryKeyDefinitions();
    if (primaryKeyDefinitions.size() == 1) {
      ColumnDefinition primaryKeyDefinition = primaryKeyDefinitions.getFirst();
      if (!row.containsKey(primaryKeyDefinition.name())) {
        throw new IllegalArgumentException(
            "row is missing primary key column: " + primaryKeyDefinition.name());
      }
      return Map.of(primaryKeyDefinition.name(), row.get(primaryKeyDefinition.name()));
    }
    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>(primaryKeyDefinitions.size());
    for (ColumnDefinition primaryKeyDefinition : primaryKeyDefinitions) {
      String primaryKeyColumn = primaryKeyDefinition.name();
      if (!row.containsKey(primaryKeyColumn)) {
        throw new IllegalArgumentException("row is missing primary key column: " + primaryKeyColumn);
      }
      primaryKey.put(primaryKeyColumn, row.get(primaryKeyColumn));
    }
    return Collections.unmodifiableMap(primaryKey);
  }

  /**
   * Row-image overload of {@link #primaryKeyRow(Map)}. Extracts the primary-key projection
   * from an {@link ImmutableRowImage} without going through the {@code Map} surface. Returns a
   * new primary-key {@link ImmutableRowImage} whose {@link RowLayout} is
   * shared via {@link #primaryKeyRowLayout()} so per-row allocation is just the values array.
   */
  public ImmutableRowImage primaryKeyRow(ImmutableRowImage row) {
    Objects.requireNonNull(row, "row");
    List<ColumnDefinition> primaryKeyDefinitions = primaryKeyDefinitions();
    Object[] values = new Object[primaryKeyDefinitions.size()];
    for (int index = 0; index < primaryKeyDefinitions.size(); index++) {
      String primaryKeyColumn = primaryKeyDefinitions.get(index).name();
      if (!row.containsKey(primaryKeyColumn)) {
        throw new IllegalArgumentException("row is missing primary key column: " + primaryKeyColumn);
      }
      values[index] = row.get(primaryKeyColumn);
    }
    return ImmutableRowImage.ofLayout(primaryKeyRowLayout(), values);
  }

  public Map<String, Object> primaryKeyRowFromLiteral(String literal) {
    PrimaryKeyTuple primaryKeyTuple = primaryKeyTupleFromLiteral(literal);
    List<ColumnDefinition> primaryKeyDefinitions = primaryKeyDefinitions();
    if (primaryKeyDefinitions.size() == 1) {
      ColumnDefinition definition = primaryKeyDefinitions.getFirst();
      return Map.of(definition.name(), primaryKeyTuple.values().getFirst().normalizedValue());
    }
    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>(primaryKeyDefinitions.size());
    for (int index = 0; index < primaryKeyDefinitions.size(); index++) {
      primaryKey.put(
          primaryKeyDefinitions.get(index).name(),
          primaryKeyTuple.values().get(index).normalizedValue());
    }
    return Collections.unmodifiableMap(primaryKey);
  }

  public PrimaryKeyTuple primaryKeyTupleFromLiteral(String literal) {
    Objects.requireNonNull(literal, "literal");
    return PrimaryKeyTuple.fromLiteral(primaryKeyDefinitions(), literal);
  }

  public String primaryKeyLiteralFor(PrimaryKeyTuple tuple) {
    return normalizePrimaryKeyTuple(tuple).literal();
  }

  public PrimaryKeyValue primaryKeyValueFromLiteral(String literal) {
    Objects.requireNonNull(literal, "literal");
    return PrimaryKeyValue.fromLiteral(primaryKeyDefinition(), literal);
  }

  public String primaryKeyLiteralFor(Map<String, Object> row) {
    Objects.requireNonNull(row, "row");
    List<ColumnDefinition> primaryKeyDefinitions = primaryKeyDefinitions();
    if (primaryKeyDefinitions.size() == 1) {
      ColumnDefinition definition = primaryKeyDefinitions.getFirst();
      Object value = row.get(definition.name());
      if (value == null) {
        throw new IllegalArgumentException(
            "row is missing primary key column: " + definition.name());
      }
      return PrimaryKeyValue.fromColumn(definition, value).literal();
    }
    StringBuilder builder = new StringBuilder();
    builder.append('{');
    for (int index = 0; index < primaryKeyDefinitions.size(); index++) {
      ColumnDefinition definition = primaryKeyDefinitions.get(index);
      Object value = row.get(definition.name());
      if (value == null) {
        throw new IllegalArgumentException(
            "row is missing primary key column: " + definition.name());
      }
      if (index > 0) {
        builder.append(',');
      }
      builder.append(escapePrimaryKeyLiteralComponent(definition.name()));
      builder.append('=');
      builder.append(
          escapePrimaryKeyLiteralComponent(PrimaryKeyValue.fromColumn(definition, value).literal()));
    }
    builder.append('}');
    return builder.toString();
  }

  public String canonicalPrimaryKeyLiteral(String literal) {
    return primaryKeyTupleFromLiteral(literal).literal();
  }

  public int comparePrimaryKeyRows(Map<String, Object> left, Map<String, Object> right) {
    return primaryKeyTupleFor(left).compareTo(primaryKeyTupleFor(right));
  }

  public int comparePrimaryKeyTuples(PrimaryKeyTuple left, PrimaryKeyTuple right) {
    return normalizePrimaryKeyTuple(left).compareTo(normalizePrimaryKeyTuple(right));
  }

  public int comparePrimaryKeyLiterals(String left, String right) {
    return primaryKeyTupleFromLiteral(left).compareTo(primaryKeyTupleFromLiteral(right));
  }

  public Map<String, Object> primaryKeyRowFromTuple(PrimaryKeyTuple tuple) {
    Objects.requireNonNull(tuple, "tuple");
    List<ColumnDefinition> primaryKeyDefinitions = primaryKeyDefinitions();
    if (tuple.values().size() != primaryKeyDefinitions.size()) {
      throw new IllegalArgumentException(
          "primary-key tuple does not match schema width: expected "
              + primaryKeyDefinitions.size()
              + " values but was "
              + tuple.values().size());
    }
    if (primaryKeyDefinitions.size() == 1) {
      ColumnDefinition definition = primaryKeyDefinitions.getFirst();
      return Map.of(definition.name(), tuple.values().getFirst().normalizedValue());
    }
    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>(primaryKeyDefinitions.size());
    for (int index = 0; index < primaryKeyDefinitions.size(); index++) {
      primaryKey.put(
          primaryKeyDefinitions.get(index).name(),
          tuple.values().get(index).normalizedValue());
    }
    return Collections.unmodifiableMap(primaryKey);
  }

  private PrimaryKeyTuple normalizePrimaryKeyTuple(PrimaryKeyTuple tuple) {
    Objects.requireNonNull(tuple, "tuple");
    if (!tuple.columnNames().equals(primaryKeyColumns)) {
      throw new IllegalArgumentException(
          "primary-key tuple columns do not match schema primary key columns: expected "
              + primaryKeyColumns
              + " but was "
              + tuple.columnNames());
    }
    return PrimaryKeyTuple.fromValues(primaryKeyColumns, tuple.values());
  }

  private ColumnDefinition requireSinglePrimaryKeyDefinition(String message) {
    if (!hasSinglePrimaryKey()) {
      throw new IllegalStateException(message + ": " + primaryKeyColumns);
    }
    return cachedViews().singlePrimaryKeyDefinition();
  }

  private CachedViews cachedViews() {
    synchronized (CACHED_VIEWS) {
      return CACHED_VIEWS.computeIfAbsent(this, CachedViews::from);
    }
  }

  private static String escapePrimaryKeyLiteralComponent(String value) {
    return value
        .replace("\\", "\\\\")
        .replace(",", "\\,")
        .replace("=", "\\=")
        .replace("{", "\\{")
        .replace("}", "\\}");
  }

  private static String fingerprintFor(
      TableId tableId, List<ColumnDefinition> columns, List<String> ignoredColumns) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(tableId.displayName().getBytes(StandardCharsets.UTF_8));
      Set<String> ignored = Set.copyOf(ignoredColumns);
      for (ColumnDefinition column : columns) {
        if (ignored.contains(column.name())) {
          continue;
        }
        digest.update((byte) '|');
        digest.update(column.name().getBytes(StandardCharsets.UTF_8));
        digest.update((byte) ':');
        digest.update(column.sourceType().getBytes(StandardCharsets.UTF_8));
        digest.update((byte) ':');
        digest.update(column.neutralType().name().getBytes(StandardCharsets.UTF_8));
        digest.update((byte) ':');
        digest.update(Boolean.toString(column.primaryKey()).getBytes(StandardCharsets.UTF_8));
        digest.update((byte) ':');
        digest.update(Boolean.toString(column.nullable()).getBytes(StandardCharsets.UTF_8));
      }
      return HexFormat.of().formatHex(digest.digest());
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 should always exist", e);
    }
  }

  private record PrimaryKeyCandidate(int ordinal, int index, String name) {}

  private record CachedViews(
      List<ColumnDefinition> selectedColumns,
      List<String> selectedColumnNames,
      List<ColumnDefinition> primaryKeyDefinitions,
      ColumnDefinition singlePrimaryKeyDefinition,
      RowLayout selectedRowLayout,
      RowLayout primaryKeyRowLayout) {
    private static CachedViews from(TableSchema schema) {
      List<ColumnDefinition> selectedColumns =
          buildSelectedColumns(schema.columns(), schema.ignoredColumns());
      List<String> selectedColumnNames = buildSelectedColumnNames(selectedColumns);
      List<ColumnDefinition> primaryKeyDefinitions =
          buildPrimaryKeyDefinitions(schema.columns(), schema.primaryKeyColumns());
      ColumnDefinition singlePrimaryKeyDefinition =
          primaryKeyDefinitions.size() == 1 ? primaryKeyDefinitions.getFirst() : null;
      return new CachedViews(
          selectedColumns,
          selectedColumnNames,
          primaryKeyDefinitions,
          singlePrimaryKeyDefinition,
          RowLayout.forColumns(selectedColumnNames),
          RowLayout.forColumns(schema.primaryKeyColumns()));
    }

    private static List<ColumnDefinition> buildSelectedColumns(
        List<ColumnDefinition> columns, List<String> ignoredColumns) {
      Set<String> ignored = Set.copyOf(ignoredColumns);
      return columns.stream().filter(column -> !ignored.contains(column.name())).toList();
    }

    private static List<String> buildSelectedColumnNames(List<ColumnDefinition> selectedColumns) {
      return selectedColumns.stream().map(ColumnDefinition::name).toList();
    }

    private static List<ColumnDefinition> buildPrimaryKeyDefinitions(
        List<ColumnDefinition> columns, List<String> primaryKeyColumns) {
      LinkedHashMap<String, ColumnDefinition> byName = new LinkedHashMap<>();
      for (ColumnDefinition column : columns) {
        byName.put(column.name(), column);
      }
      List<ColumnDefinition> primaryKeyDefinitions = new ArrayList<>(primaryKeyColumns.size());
      for (String primaryKeyColumn : primaryKeyColumns) {
        ColumnDefinition definition = byName.get(primaryKeyColumn);
        if (definition == null) {
          throw new IllegalStateException(
              "schema is missing declared primary key column: " + primaryKeyColumn);
        }
        primaryKeyDefinitions.add(definition);
      }
      return List.copyOf(primaryKeyDefinitions);
    }
  }
}
