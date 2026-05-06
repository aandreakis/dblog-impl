package io.github.aandreakis.dblog.adapter.postgres.internal;

import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfigValidator;
import io.github.aandreakis.dblog.adapter.postgres.PostgresDialect;
import io.github.aandreakis.dblog.config.PostgresResourceOwnership;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.reconcile.HeartbeatMetadata;
import io.github.aandreakis.dblog.core.reconcile.WatermarkMetadata;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Derives a {@link PostgresReplicationResourcesRequest} from a {@link RelationalSourceConfig}
 * and the captured contract schemas. Shared between {@link PostgresLiveRuntimeFactory} and
 * {@link PostgresSourcePreflight} so option parsing and publication-table derivation exist in
 * exactly one place.
 */
final class PostgresReplicationResourcesRequestFactory {
  private PostgresReplicationResourcesRequestFactory() {}

  static PostgresReplicationResourcesRequest build(
      RelationalSourceConfig config, List<TableSchema> contractSchemas) {
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(contractSchemas, "contractSchemas");

    String databaseName =
        config.databaseName() == null
            ? RelationalSourceConfigValidator.databaseNameFromJdbcUrl(
                config.jdbcUrl(), PostgresDialect.JDBC_PREFIX, PostgresDialect.DISPLAY_NAME)
            : config.databaseName();
    String publicationName =
        stringOption(
            config.options(),
            "postgres.publicationName",
            "pub_" + sanitizeIdentifier(config.sourceId()));
    String slotName =
        stringOption(
            config.options(),
            "postgres.slotName",
            "slot_" + sanitizeIdentifier(config.sourceId()));
    PostgresResourceOwnership publicationOwnership =
        ownershipOption(
            config.options(),
            "postgres.publicationOwnership",
            PostgresResourceOwnership.DBLOG_MANAGED);
    PostgresResourceOwnership slotOwnership =
        ownershipOption(
            config.options(), "postgres.slotOwnership", PostgresResourceOwnership.DBLOG_MANAGED);
    boolean slotFailover = booleanOption(config.options(), "postgres.slotFailover", false);

    return new PostgresReplicationResourcesRequest(
        databaseName,
        new PostgresPublicationConfig(
            databaseName,
            publicationName,
            publicationTables(databaseName, contractSchemas),
            publicationOwnership),
        new PostgresReplicationSlotConfig(
            slotName, databaseName, "pgoutput", false, false, slotFailover, slotOwnership));
  }

  static String stringOption(Map<String, String> options, String key, String defaultValue) {
    String raw = options.get(key);
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    return raw.trim();
  }

  static boolean booleanOption(Map<String, String> options, String key, boolean defaultValue) {
    String raw = options.get(key);
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    return Boolean.parseBoolean(raw.trim());
  }

  static PostgresResourceOwnership ownershipOption(
      Map<String, String> options, String key, PostgresResourceOwnership defaultValue) {
    String raw = options.get(key);
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    return PostgresResourceOwnership.valueOf(raw.trim().toUpperCase(Locale.ROOT));
  }

  private static List<TableId> publicationTables(
      String databaseName, List<TableSchema> contractSchemas) {
    LinkedHashSet<TableId> tables = new LinkedHashSet<>();
    for (TableSchema schema : contractSchemas) {
      tables.add(schema.tableId());
    }
    tables.add(WatermarkMetadata.tableIdFor(databaseName));
    tables.add(HeartbeatMetadata.tableIdFor(databaseName));
    return List.copyOf(new ArrayList<>(tables));
  }

  /**
   * Mirrors {@code PostgresLiveRuntimeFactory#sanitizeIdentifier} so that preflight derives the
   * same publication and slot names the runtime factory would. Keep behavior in lockstep.
   */
  private static final int POSTGRES_IDENTIFIER_MAX_LENGTH = 63;

  private static String sanitizeIdentifier(String value) {
    Objects.requireNonNull(value, "value");
    if (value.isBlank()) {
      throw new IllegalArgumentException("value must not be blank");
    }
    String sanitized = value.trim().toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_]", "_");
    if (sanitized.isBlank()) {
      return "runtime";
    }
    if (sanitized.length() > POSTGRES_IDENTIFIER_MAX_LENGTH) {
      return sanitized.substring(0, POSTGRES_IDENTIFIER_MAX_LENGTH);
    }
    return sanitized;
  }
}
