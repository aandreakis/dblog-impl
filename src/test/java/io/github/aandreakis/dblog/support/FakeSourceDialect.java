package io.github.aandreakis.dblog.support;

import io.github.aandreakis.dblog.adapter.api.RawColumnMetadata;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.SourceDialect;
import io.github.aandreakis.dblog.adapter.api.TableNamingPolicy;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;

/**
 * No-op {@link SourceDialect} for tests whose fake {@code SourceAdapter} implementations do not
 * rely on dialect behavior — they override {@code validateSourceConfig} directly.
 */
public final class FakeSourceDialect implements SourceDialect {
  public static final FakeSourceDialect INSTANCE = new FakeSourceDialect();

  private FakeSourceDialect() {}

  @Override
  public String key() {
    return "fake";
  }

  @Override
  public String displayName() {
    return "Fake";
  }

  @Override
  public String jdbcUrlPrefix() {
    return "jdbc:fake://";
  }

  @Override
  public TableNamingPolicy tableNamingPolicy() {
    return TableNamingPolicy.TWO_PART_SCHEMA_TABLE;
  }

  @Override
  public void validateNativeConfig(RelationalSourceConfig config) {}

  @Override
  public NeutralColumnType neutralType(RawColumnMetadata column) {
    return NeutralColumnType.UNSUPPORTED;
  }
}
