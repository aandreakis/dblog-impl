package io.github.aandreakis.dblog.runtime.host;

import io.github.aandreakis.dblog.adapter.api.ProtocolDriftException;
import io.github.aandreakis.dblog.adapter.api.UnsupportedSourceStateException;
import io.github.aandreakis.dblog.core.checkpoint.CheckpointAdvanceException;
import io.github.aandreakis.dblog.core.reconcile.MetadataCorruptionException;
import io.github.aandreakis.dblog.core.schema.SchemaCompatibilityException;
import io.github.aandreakis.dblog.sink.jdbc.TargetApplyContractException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Set;

/** Classifies runtime failures into retryable availability issues or hard contract breaches. */
public final class RuntimeFailureClassifier {
  private RuntimeFailureClassifier() {}

  public static RuntimeFailureDisposition classifySource(Throwable failure) {
    return classify(failure);
  }

  public static RuntimeFailureDisposition classifySink(Throwable failure) {
    return classify(failure);
  }

  private static RuntimeFailureDisposition classify(Throwable failure) {
    Throwable current = failure;
    while (current != null) {
      if (current instanceof ProtocolDriftException
          || current instanceof MetadataCorruptionException
          || current instanceof UnsupportedSourceStateException
          || current instanceof SchemaCompatibilityException
          || current instanceof CheckpointAdvanceException
          || current instanceof TargetApplyContractException
          || current instanceof IllegalArgumentException) {
        return RuntimeFailureDisposition.FAIL_HARD_CONTRACT_BREACH;
      }
      if (current instanceof SQLException sqlException) {
        return classifySql(sqlException);
      }
      if (current instanceof ConnectException
          || current instanceof UnknownHostException
          || current instanceof SocketTimeoutException
          || current instanceof SocketException) {
        return RuntimeFailureDisposition.RETRYABLE_AVAILABILITY;
      }
      current = current.getCause();
    }
    return RuntimeFailureDisposition.FAIL_HARD_CONTRACT_BREACH;
  }

  private static RuntimeFailureDisposition classifySql(SQLException failure) {
    boolean availabilityObserved = false;
    ArrayDeque<SQLException> pending = new ArrayDeque<>();
    Set<SQLException> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    pending.add(failure);
    while (!pending.isEmpty()) {
      SQLException current = pending.removeFirst();
      if (!visited.add(current)) {
        continue;
      }
      SqlFailureSignal signal = classifySingleSql(current);
      if (signal == SqlFailureSignal.HARD_CONTRACT) {
        return RuntimeFailureDisposition.FAIL_HARD_CONTRACT_BREACH;
      }
      if (signal == SqlFailureSignal.AVAILABILITY) {
        availabilityObserved = true;
      }
      if (current.getNextException() != null) {
        pending.add(current.getNextException());
      }
      if (current.getCause() instanceof SQLException causeSql) {
        pending.add(causeSql);
      }
    }

    if (availabilityObserved) {
      return RuntimeFailureDisposition.RETRYABLE_AVAILABILITY;
    }
    return RuntimeFailureDisposition.FAIL_HARD_CONTRACT_BREACH;
  }

  private static SqlFailureSignal classifySingleSql(SQLException sql) {
    String sqlState = sql.getSQLState();
    if (sqlState != null) {
      if (sqlState.startsWith("28")
          || sqlState.startsWith("3D")
          || sqlState.startsWith("3F")
          || sqlState.startsWith("42")) {
        return SqlFailureSignal.HARD_CONTRACT;
      }
      if (sqlState.startsWith("08")) {
        return SqlFailureSignal.AVAILABILITY;
      }
    }

    String message = flattenedMessage(sql);
    if (containsAny(
        message,
        "access denied",
        "permission denied",
        "authentication failed",
        "password authentication failed",
        "not authorized",
        "must not be blank")) {
      return SqlFailureSignal.HARD_CONTRACT;
    }
    if (containsAny(
        message,
        "unknown database",
        "unknown table",
        "doesn't exist",
        "does not exist",
        "publication is missing",
        "logical slot is missing")) {
      return SqlFailureSignal.HARD_CONTRACT;
    }
    if (sql instanceof SQLTransientConnectionException) {
      return SqlFailureSignal.AVAILABILITY;
    }
    if (containsAny(
        message,
        "communications link failure",
        "communication failure",
        "connection refused",
        "connection reset",
        "connection timed out",
        "could not connect",
        "the connection attempt failed",
        "server closed the connection unexpectedly",
        "connection is closed",
        "unavailable",
        "broken pipe",
        "temporarily unavailable")) {
      return SqlFailureSignal.AVAILABILITY;
    }

    return SqlFailureSignal.UNKNOWN;
  }

  private static String flattenedMessage(Throwable failure) {
    StringBuilder builder = new StringBuilder();
    Throwable current = failure;
    while (current != null) {
      if (current.getMessage() != null) {
        if (!builder.isEmpty()) {
          builder.append(' ');
        }
        builder.append(current.getMessage());
      }
      current = current.getCause();
    }
    return builder.toString().toLowerCase(Locale.ROOT);
  }

  private static boolean containsAny(String message, String... needles) {
    for (String needle : needles) {
      if (message.contains(needle)) {
        return true;
      }
    }
    return false;
  }

  private enum SqlFailureSignal {
    HARD_CONTRACT,
    AVAILABILITY,
    UNKNOWN
  }
}
