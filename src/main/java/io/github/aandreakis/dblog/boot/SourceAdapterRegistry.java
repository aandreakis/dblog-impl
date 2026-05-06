package io.github.aandreakis.dblog.boot;

import io.github.aandreakis.dblog.adapter.api.SourceAdapter;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public final class SourceAdapterRegistry {
  private final Map<String, SourceAdapter> adapters;

  public SourceAdapterRegistry(Collection<? extends SourceAdapter> adapters) {
    Objects.requireNonNull(adapters, "adapters");
    LinkedHashMap<String, SourceAdapter> indexed = new LinkedHashMap<>();
    for (SourceAdapter adapter : adapters) {
      SourceAdapter required = Objects.requireNonNull(adapter, "adapter");
      indexed.put(normalize(required.key()), required);
      if (normalize(required.key()).equals("postgres")) {
        indexed.put("postgresql", required);
      }
    }
    this.adapters = Map.copyOf(indexed);
  }

  public SourceAdapter require(String key) {
    SourceAdapter adapter = adapters.get(normalize(Objects.requireNonNull(key, "key")));
    if (adapter == null) {
      throw new IllegalArgumentException("Unsupported source adapter: " + key);
    }
    return adapter;
  }

  public Map<String, SourceAdapter> adapters() {
    return adapters;
  }

  private static String normalize(String key) {
    String normalized = key.trim().toLowerCase(Locale.ROOT);
    if (normalized.isBlank()) {
      throw new IllegalArgumentException("adapter key must not be blank");
    }
    return normalized;
  }
}
