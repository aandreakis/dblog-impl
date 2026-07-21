package io.github.aandreakis.dblog.core.reconcile;

import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import io.github.aandreakis.dblog.core.model.PrimaryKeyHash;
import io.github.aandreakis.dblog.core.model.RowLayout;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record Chunk(
    String jobId,
    String tableName,
    TableSchema schema,
    PrimaryKeyTuple startAfterPrimaryKeyTuple,
    List<ImmutableRowImage> rowImages,
    List<PrimaryKeyHash> rowPrimaryKeyHashes,
    PrimaryKeyTuple lastPrimaryKeyTuple,
    boolean finalChunk,
    List<PrimaryKeyTuple> matchedRequestedPrimaryKeyTuples) {

  public Chunk {
    Objects.requireNonNull(jobId, "jobId");
    Objects.requireNonNull(tableName, "tableName");
    Objects.requireNonNull(schema, "schema");
    rowImages = List.copyOf(Objects.requireNonNull(rowImages, "rowImages"));
    rowPrimaryKeyHashes =
        rowPrimaryKeyHashes == null
            ? derivePrimaryKeyHashes(schema, rowImages)
            : List.copyOf(rowPrimaryKeyHashes);
    if (rowImages.size() != rowPrimaryKeyHashes.size()) {
      throw new IllegalArgumentException("rowImages and rowPrimaryKeyHashes must have the same size");
    }
    matchedRequestedPrimaryKeyTuples =
        matchedRequestedPrimaryKeyTuples == null
            ? List.of()
            : List.copyOf(matchedRequestedPrimaryKeyTuples);
  }

  public Chunk(
      String jobId,
      String tableName,
      TableSchema schema,
      PrimaryKeyTuple startAfterPrimaryKeyTuple,
      List<ImmutableRowImage> rowImages,
      List<PrimaryKeyHash> rowPrimaryKeyHashes,
      PrimaryKeyTuple lastPrimaryKeyTuple,
      boolean finalChunk) {
    this(
        jobId,
        tableName,
        schema,
        startAfterPrimaryKeyTuple,
        rowImages,
        rowPrimaryKeyHashes,
        lastPrimaryKeyTuple,
        finalChunk,
        List.of());
  }

  /**
   * Map-based factory — existing call sites hand rows in as {@code List<Map<String, Object>>}.
   * Conversion to {@link ImmutableRowImage} using the schema's shared {@link RowLayout} happens
   * once here, so the reconciler hot path avoids per-emit row allocations.
   */
  public static Chunk fromMapRows(
      String jobId,
      String tableName,
      TableSchema schema,
      PrimaryKeyTuple startAfterPrimaryKeyTuple,
      List<Map<String, Object>> rows,
      PrimaryKeyTuple lastPrimaryKeyTuple,
      boolean finalChunk) {
    return new Chunk(
        jobId,
        tableName,
        schema,
        startAfterPrimaryKeyTuple,
        mapsToRowImages(schema, rows),
        null,
        lastPrimaryKeyTuple,
        finalChunk);
  }

  public static Chunk fromMapRows(
      String jobId,
      String tableName,
      TableSchema schema,
      String startAfterPrimaryKey,
      List<Map<String, Object>> rows,
      String lastPrimaryKey,
      boolean finalChunk) {
    return fromMapRows(
        jobId,
        tableName,
        schema,
        startAfterPrimaryKey == null ? null : schema.primaryKeyTupleFromLiteral(startAfterPrimaryKey),
        rows,
        lastPrimaryKey == null ? null : schema.primaryKeyTupleFromLiteral(lastPrimaryKey),
        finalChunk);
  }


  /**
   * Zero-row chunk used to drive {@link WindowReconciler} across an empty-chunk drain window
   * (coordinator observed {@code Optional.empty()} from the watermark-scoped SELECT). The
   * returned chunk has no row images and no start/end primary keys — it exists only so the
   * reconciler can process LW/HW transitions and collect non-watermark log events between them.
   * The result never reaches progress tracking or the sink as a chunk.
   */
  public static Chunk drainOnly(String jobId, TableSchema schema) {
    return new Chunk(
        Objects.requireNonNull(jobId, "jobId"),
        Objects.requireNonNull(schema, "schema").tableId().displayName(),
        schema,
        null,
        List.of(),
        List.of(),
        null,
        true);
  }

  /** Map-backed view of the chunk rows, for consumers that need the legacy shape. */
  public List<Map<String, Object>> rows() {
    List<Map<String, Object>> materialized = new ArrayList<>(rowImages.size());
    for (ImmutableRowImage image : rowImages) {
      materialized.add(image.asMap());
    }
    return List.copyOf(materialized);
  }

  /**
   * Derives the per-row {@link PrimaryKeyTuple} list on demand. Not cached — the dump path
   * never calls this, and the only consumer (targeted-repair coordination) invokes it once per
   * chunk. Every entry in the returned list is a fresh tuple allocation, which is still cheap
   * compared to the per-row tuple pre-compute at chunk construction that this replaces.
   */
  public List<PrimaryKeyTuple> rowPrimaryKeyTuples() {
    List<PrimaryKeyTuple> tuples = new ArrayList<>(rowImages.size());
    for (ImmutableRowImage image : rowImages) {
      tuples.add(schema.primaryKeyTupleFor(image));
    }
    return List.copyOf(tuples);
  }

  public Map<PrimaryKeyTuple, ImmutableRowImage> rowImagesByPrimaryKeyTuples() {
    Map<PrimaryKeyTuple, ImmutableRowImage> ordered =
        LinkedHashMap.newLinkedHashMap(rowImages.size());
    for (ImmutableRowImage image : rowImages) {
      ordered.put(schema.primaryKeyTupleFor(image), image);
    }
    return ordered;
  }

  /**
   * Lightweight-hash view used by the reconciler's hot lookup path. Hashes are cached on the
   * chunk, so session open is just a map fill, not a re-hash of every row.
   */
  public Map<PrimaryKeyHash, ImmutableRowImage> rowImagesByPrimaryKeyHashes() {
    Map<PrimaryKeyHash, ImmutableRowImage> ordered =
        LinkedHashMap.newLinkedHashMap(rowImages.size());
    for (int index = 0; index < rowImages.size(); index++) {
      ordered.put(rowPrimaryKeyHashes.get(index), rowImages.get(index));
    }
    return ordered;
  }

  public Chunk withSchema(TableSchema schema) {
    return new Chunk(
        jobId,
        tableName,
        Objects.requireNonNull(schema, "schema"),
        startAfterPrimaryKeyTuple,
        rowImages,
        null,
        lastPrimaryKeyTuple,
        finalChunk,
        matchedRequestedPrimaryKeyTuples);
  }

  public String startAfterPrimaryKey() {
    return startAfterPrimaryKeyTuple == null ? null : startAfterPrimaryKeyTuple.literal();
  }

  public String lastPrimaryKey() {
    return lastPrimaryKeyTuple == null ? null : lastPrimaryKeyTuple.literal();
  }

  private static List<ImmutableRowImage> mapsToRowImages(
      TableSchema schema, List<Map<String, Object>> rows) {
    Objects.requireNonNull(schema, "schema");
    List<Map<String, Object>> requiredRows = List.copyOf(Objects.requireNonNull(rows, "rows"));
    RowLayout layout = schema.selectedRowLayout();
    List<ColumnDefinition> selectedColumns = schema.selectedColumns();
    List<ImmutableRowImage> images = new ArrayList<>(requiredRows.size());
    for (Map<String, Object> row : requiredRows) {
      Object[] values = new Object[selectedColumns.size()];
      for (int index = 0; index < selectedColumns.size(); index++) {
        values[index] = row.get(selectedColumns.get(index).name());
      }
      images.add(ImmutableRowImage.ofLayout(layout, values));
    }
    return List.copyOf(images);
  }

  private static List<PrimaryKeyHash> derivePrimaryKeyHashes(
      TableSchema schema, List<ImmutableRowImage> rowImages) {
    List<PrimaryKeyHash> hashes = new ArrayList<>(rowImages.size());
    for (ImmutableRowImage image : rowImages) {
      hashes.add(schema.primaryKeyHashFor(image));
    }
    return List.copyOf(hashes);
  }
}
