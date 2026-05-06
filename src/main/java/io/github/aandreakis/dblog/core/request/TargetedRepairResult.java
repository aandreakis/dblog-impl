package io.github.aandreakis.dblog.core.request;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public record TargetedRepairResult<TX extends SourceTransaction<?>>(
    Optional<TargetedRepairOutcome<TX>> outcome,
    List<PrimaryKeyTuple> missingPrimaryKeyTuples) {
  public TargetedRepairResult {
    outcome = Objects.requireNonNull(outcome, "outcome");
    missingPrimaryKeyTuples =
        List.copyOf(Objects.requireNonNull(missingPrimaryKeyTuples, "missingPrimaryKeyTuples"));
    if (outcome.isPresent()
        && !outcome.orElseThrow().missingPrimaryKeyTuples().equals(missingPrimaryKeyTuples)) {
      throw new IllegalArgumentException(
          "targeted repair result missing keys must match outcome missing keys");
    }
  }

  public List<String> missingPrimaryKeys() {
    return missingPrimaryKeyTuples.stream().map(PrimaryKeyTuple::literal).toList();
  }
}
