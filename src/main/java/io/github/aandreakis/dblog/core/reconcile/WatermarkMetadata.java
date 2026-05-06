package io.github.aandreakis.dblog.core.reconcile;

import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import io.github.aandreakis.dblog.core.model.TableId;
import java.util.Map;

public final class WatermarkMetadata {
  public static final String SCHEMA_NAME = "dblog_meta";
  public static final String TABLE_NAME = "watermarks";
  public static final String PRIMARY_KEY_COLUMN = "id";
  public static final long SINGLETON_ROW_ID = 1L;
  public static final String RUN_ID_COLUMN = "run_id";
  public static final String TOKEN_COLUMN = "token";

  private static final ImmutableRowImage SINGLETON_PRIMARY_KEY =
      ImmutableRowImage.of(Map.of(PRIMARY_KEY_COLUMN, SINGLETON_ROW_ID));

  private WatermarkMetadata() {}

  public static TableId tableIdFor(String databaseName) {
    return new TableId(databaseName, SCHEMA_NAME, TABLE_NAME);
  }

  public static ImmutableRowImage singletonPrimaryKey() {
    return SINGLETON_PRIMARY_KEY;
  }
}
