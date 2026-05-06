package io.github.aandreakis.dblog.sink.jdbc;

import io.github.aandreakis.dblog.core.model.TableId;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Optional source-to-target table remapping for JDBC apply sinks. */
public final class TargetTableResolver {
  private static final TargetTableResolver IDENTITY = new TargetTableResolver(Map.of());

  private final Map<TableId, TableId> overrides;

  private TargetTableResolver(Map<TableId, TableId> overrides) {
    this.overrides = Map.copyOf(overrides);
  }

  public static TargetTableResolver identity() {
    return IDENTITY;
  }

  public static TargetTableResolver of(Map<TableId, TableId> overrides) {
    Objects.requireNonNull(overrides, "overrides");
    LinkedHashMap<TableId, TableId> normalized = new LinkedHashMap<>();
    for (Map.Entry<TableId, TableId> entry : overrides.entrySet()) {
      normalized.put(
          Objects.requireNonNull(entry.getKey(), "sourceTableId"),
          Objects.requireNonNull(entry.getValue(), "targetTableId"));
    }
    return normalized.isEmpty() ? IDENTITY : new TargetTableResolver(normalized);
  }

  public TableId resolve(TableId sourceTableId) {
    Objects.requireNonNull(sourceTableId, "sourceTableId");
    return overrides.getOrDefault(sourceTableId, sourceTableId);
  }

  public boolean isIdentity() {
    return overrides.isEmpty();
  }
}
