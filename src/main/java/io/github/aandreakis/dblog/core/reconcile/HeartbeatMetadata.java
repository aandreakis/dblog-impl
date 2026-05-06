package io.github.aandreakis.dblog.core.reconcile;

import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import io.github.aandreakis.dblog.core.model.TableId;
import java.util.Map;

public final class HeartbeatMetadata {
  public static final String SCHEMA_NAME = WatermarkMetadata.SCHEMA_NAME;
  public static final String TABLE_NAME = "heartbeats";
  public static final String PRIMARY_KEY_COLUMN = "id";
  public static final long SINGLETON_ROW_ID = 1L;
  public static final String RUN_ID_COLUMN = WatermarkMetadata.RUN_ID_COLUMN;
  public static final String SOURCE_STREAM_ID_COLUMN = "source_stream_id";
  public static final String TIMESTAMP_COLUMN = "last_beat_at";

  private static final ImmutableRowImage SINGLETON_PRIMARY_KEY =
      ImmutableRowImage.of(Map.of(PRIMARY_KEY_COLUMN, SINGLETON_ROW_ID));

  private HeartbeatMetadata() {}

  public static TableId tableIdFor(String databaseName) {
    return new TableId(databaseName, SCHEMA_NAME, TABLE_NAME);
  }

  public static ImmutableRowImage singletonPrimaryKey() {
    return SINGLETON_PRIMARY_KEY;
  }
}
