package io.github.aandreakis.dblog.runtime.host;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.sink.jdbc.TargetApplyContractException;
import io.github.aandreakis.dblog.sink.jdbc.TargetApplyFailure;
import io.github.aandreakis.dblog.sink.jdbc.TargetApplyFailureType;
import java.net.ConnectException;
import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTransientConnectionException;
import java.util.List;
import org.junit.jupiter.api.Test;

class RuntimeFailureClassifierTests {
  @Test
  void classifiesSqlAuthenticationStateAsHardContractBreachEvenWhenConnectionException() {
    RuntimeException failure =
        new RuntimeException(
            "target apply failed",
            new SQLNonTransientConnectionException("Access denied for user dblog", "28000"));

    assertThat(RuntimeFailureClassifier.classifySink(failure))
        .isEqualTo(RuntimeFailureDisposition.FAIL_HARD_CONTRACT_BREACH);
  }

  @Test
  void classifiesMissingDatabaseStateAsHardContractBreachEvenWhenConnectionException() {
    RuntimeException failure =
        new RuntimeException(
            "source reconnect failed",
            new SQLNonTransientConnectionException("database appdb does not exist", "3D000"));

    assertThat(RuntimeFailureClassifier.classifySource(failure))
        .isEqualTo(RuntimeFailureDisposition.FAIL_HARD_CONTRACT_BREACH);
  }

  @Test
  void classifiesSqlConnectionAvailabilityStateAsRetryable() {
    RuntimeException failure =
        new RuntimeException(
            "source read failed",
            new SQLTransientConnectionException("connection refused", "08001"));

    assertThat(RuntimeFailureClassifier.classifySource(failure))
        .isEqualTo(RuntimeFailureDisposition.RETRYABLE_AVAILABILITY);
  }

  @Test
  void classifiesTransientConnectionExceptionWithoutSqlStateAsRetryable() {
    SQLTransientConnectionException failure =
        new SQLTransientConnectionException("MySQL binlog stream disconnected unexpectedly");

    assertThat(RuntimeFailureClassifier.classifySource(failure))
        .isEqualTo(RuntimeFailureDisposition.RETRYABLE_AVAILABILITY);
  }

  @Test
  void classifiesHardSqlMessageAsHardEvenWhenTransientConnectionException() {
    SQLTransientConnectionException failure =
        new SQLTransientConnectionException("Access denied for user dblog");

    assertThat(RuntimeFailureClassifier.classifySource(failure))
        .isEqualTo(RuntimeFailureDisposition.FAIL_HARD_CONTRACT_BREACH);
  }

  @Test
  void classifiesPlainNetworkFailuresAsRetryableAvailability() {
    RuntimeException failure =
        new RuntimeException("source read failed", new ConnectException("connection refused"));

    assertThat(RuntimeFailureClassifier.classifySource(failure))
        .isEqualTo(RuntimeFailureDisposition.RETRYABLE_AVAILABILITY);
  }

  @Test
  void classifiesBatchUpdateNextExceptionHardSqlStateAsHardContractBreach() {
    BatchUpdateException batch = new BatchUpdateException("batch failed", null, new int[0]);
    batch.setNextException(new SQLException("target table does not exist", "42P01"));

    assertThat(RuntimeFailureClassifier.classifySink(new RuntimeException("sink failed", batch)))
        .isEqualTo(RuntimeFailureDisposition.FAIL_HARD_CONTRACT_BREACH);
  }

  @Test
  void classifiesBatchUpdateNextExceptionAvailabilitySqlStateAsRetryable() {
    BatchUpdateException batch = new BatchUpdateException("batch failed", null, new int[0]);
    batch.setNextException(new SQLTransientConnectionException("connection reset", "08006"));

    assertThat(RuntimeFailureClassifier.classifySink(new RuntimeException("sink failed", batch)))
        .isEqualTo(RuntimeFailureDisposition.RETRYABLE_AVAILABILITY);
  }

  @Test
  void classifiesBatchUpdateHardSqlStateAsHardEvenWhenLaterNextExceptionIsAvailability() {
    BatchUpdateException batch =
        new BatchUpdateException("target table does not exist", "42P01", new int[0]);
    batch.setNextException(new SQLTransientConnectionException("connection reset", "08006"));

    assertThat(RuntimeFailureClassifier.classifySink(new RuntimeException("sink failed", batch)))
        .isEqualTo(RuntimeFailureDisposition.FAIL_HARD_CONTRACT_BREACH);
  }

  @Test
  void classifiesSqlCauseHardSqlStateAsHardEvenWhenOuterSqlStateIsAvailability() {
    SQLException availabilityWrapper =
        new SQLTransientConnectionException(
            "connection reset",
            "08006",
            new SQLException("target table does not exist", "42P01"));

    assertThat(
            RuntimeFailureClassifier.classifySink(
                new RuntimeException("sink failed", availabilityWrapper)))
        .isEqualTo(RuntimeFailureDisposition.FAIL_HARD_CONTRACT_BREACH);
  }

  @Test
  void classifiesTargetApplyContractExceptionAsHardEvenWhenCauseLooksRetryable() {
    TargetApplyContractException contractFailure =
        new TargetApplyContractException(
            targetApplyFailure(),
            "Target apply contract failed",
            new SQLTransientConnectionException("connection reset", "08006"));

    assertThat(RuntimeFailureClassifier.classifySink(contractFailure))
        .isEqualTo(RuntimeFailureDisposition.FAIL_HARD_CONTRACT_BREACH);
  }

  @Test
  void classifiesWrappedHardSqlStatesAsHardContractBreaches() {
    assertThat(
            RuntimeFailureClassifier.classifySink(
                new RuntimeException("target apply failed", new SQLException("missing table", "42P01"))))
        .isEqualTo(RuntimeFailureDisposition.FAIL_HARD_CONTRACT_BREACH);
    assertThat(
            RuntimeFailureClassifier.classifySink(
                new RuntimeException("target apply failed", new SQLException("missing schema", "3F000"))))
        .isEqualTo(RuntimeFailureDisposition.FAIL_HARD_CONTRACT_BREACH);
    assertThat(
            RuntimeFailureClassifier.classifySink(
                new RuntimeException("target apply failed", new SQLException("access denied", "28000"))))
        .isEqualTo(RuntimeFailureDisposition.FAIL_HARD_CONTRACT_BREACH);
  }

  @Test
  void classifiesWrappedAvailabilitySqlStateAsRetryable() {
    RuntimeException failure =
        new RuntimeException(
            "target apply failed",
            new SQLTransientConnectionException("connection reset", "08006"));

    assertThat(RuntimeFailureClassifier.classifySink(failure))
        .isEqualTo(RuntimeFailureDisposition.RETRYABLE_AVAILABILITY);
  }

  private static TargetApplyFailure targetApplyFailure() {
    return new TargetApplyFailure(
        TargetApplyFailureType.TARGET_TABLE_MISSING,
        new TableId("sourceA", "appdb", "widgets"),
        new TableId("targetA", "public", "widgets"),
        null,
        List.of(),
        List.of(),
        null,
        null,
        null,
        null,
        null,
        "ALIGN_TARGET_TABLE");
  }
}
