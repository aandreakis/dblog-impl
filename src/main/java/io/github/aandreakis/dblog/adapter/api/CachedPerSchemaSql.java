package io.github.aandreakis.dblog.adapter.api;

import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;
import java.util.function.Function;

/**
 * Generic per-schema cache keyed by {@link TableSchema} with weak references, so cache entries
 * are reclaimed automatically when a schema is no longer referenced by the active runtime. Schema
 * drift produces new {@code TableSchema} instances that replace older ones in the captured-schema
 * list, and the old schema's cached entry becomes eligible for GC without needing explicit
 * invalidation from schema-change signals.
 *
 * <p>Typical use: each adapter's SQL-builder class instantiates one {@code CachedPerSchemaSql}
 * over its private {@code CachedSql}-style inner class, so pre-rendered SQL strings per schema
 * are computed once and reused. The cached value type is dialect-specific; this utility only
 * owns the cache plumbing.
 *
 * <p>Access is explicitly synchronized because {@link WeakHashMap} is not thread-safe AND
 * {@code computeIfAbsent} on {@link Collections#synchronizedMap} is not atomic (the default
 * method runs outside the wrapper's synchronized block). Only {@code computeIfAbsent} is used
 * (never iteration), so the known WeakHashMap iteration-during-GC quirk does not apply here.
 *
 * @param <T> the dialect-specific cached SQL bundle associated with each schema
 */
public final class CachedPerSchemaSql<T> {
  private final Map<TableSchema, T> cache =
      Collections.synchronizedMap(new WeakHashMap<>());
  private final Function<TableSchema, T> factory;

  public CachedPerSchemaSql(Function<TableSchema, T> factory) {
    this.factory = Objects.requireNonNull(factory, "factory");
  }

  public T get(TableSchema schema) {
    Objects.requireNonNull(schema, "schema");
    synchronized (cache) {
      return cache.computeIfAbsent(schema, factory);
    }
  }
}
