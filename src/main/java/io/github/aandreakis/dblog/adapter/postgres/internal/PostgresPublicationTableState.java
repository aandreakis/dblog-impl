package io.github.aandreakis.dblog.adapter.postgres.internal;

import io.github.aandreakis.dblog.core.model.TableId;
import java.util.Objects;

/** Concrete publication membership for one table plus narrow feature flags we currently reject. */
public record PostgresPublicationTableState(
    TableId tableId, boolean rowFilterPresent, boolean columnListPresent) {
  public PostgresPublicationTableState {
    Objects.requireNonNull(tableId, "tableId");
  }
}
