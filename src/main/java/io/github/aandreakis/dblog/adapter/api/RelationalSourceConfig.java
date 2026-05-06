package io.github.aandreakis.dblog.adapter.api;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record RelationalSourceConfig(
    String sourceId,
    String jdbcUrl,
    String username,
    String password,
    String databaseName,
    List<String> capturedTables,
    Map<String, String> options,
    boolean retainTransactionHistory) {
  public RelationalSourceConfig {
    Objects.requireNonNull(sourceId, "sourceId");
    Objects.requireNonNull(jdbcUrl, "jdbcUrl");
    Objects.requireNonNull(username, "username");
    capturedTables = capturedTables == null ? List.of() : List.copyOf(capturedTables);
    options = options == null ? Map.of() : Map.copyOf(new LinkedHashMap<>(options));
    password = password == null ? "" : password;
    databaseName = databaseName == null || databaseName.isBlank() ? null : databaseName.trim();
    if (sourceId.isBlank()) {
      throw new IllegalArgumentException("sourceId must not be blank");
    }
    if (jdbcUrl.isBlank()) {
      throw new IllegalArgumentException("jdbcUrl must not be blank");
    }
    if (username.isBlank()) {
      throw new IllegalArgumentException("username must not be blank");
    }
  }
}
