package io.github.aandreakis.dblog.verification.scenario;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Stronger end-to-end scenario assertions over persisted source-mutation telemetry and sink events.
 */
public final class ScenarioLedgerVerifier {
  private static final Base64.Encoder BASE64_ENCODER = Base64.getUrlEncoder().withoutPadding();
  private static final Base64.Decoder BASE64_DECODER = Base64.getUrlDecoder();

  private ScenarioLedgerVerifier() {}

  public static String encodeSourceMutationDetail(
      String operation,
      String tableDisplayName,
      String primaryKeyLiteral,
      String beforePayload,
      String afterPayload) {
    return "operation="
        + encodeNullable(operation)
        + ";table="
        + encodeNullable(tableDisplayName)
        + ";pk="
        + encodeNullable(primaryKeyLiteral)
        + ";before="
        + encodeNullable(beforePayload)
        + ";after="
        + encodeNullable(afterPayload);
  }

  public static void verifyObservedLedger(
      ScenarioStore scenarioStore, String scenarioId, Set<String> capturedTables) {
    Objects.requireNonNull(scenarioStore, "scenarioStore");
    Objects.requireNonNull(scenarioId, "scenarioId");
    Objects.requireNonNull(capturedTables, "capturedTables");

    List<SourceMutationRecord> sourceMutations =
        loadSourceMutationRecords(scenarioStore.loadTelemetry(scenarioId), capturedTables);
    if (sourceMutations.isEmpty()) {
      return;
    }

    List<ScenarioEventRecord> sinkEvents =
        scenarioStore.loadEvents(scenarioId).stream()
            .filter(record -> capturedTables.contains(record.tableDisplayName()))
            .toList();

    verifyPerKeyLogLedger(sourceMutations, sinkEvents);
    verifyRefreshUpdatesDoNotContradictLatestLogState(sinkEvents);
  }

  static List<SourceMutationRecord> loadSourceMutationRecords(
      List<ScenarioTelemetryRecord> telemetryRecords, Set<String> capturedTables) {
    List<SourceMutationRecord> mutations = new ArrayList<>();
    for (ScenarioTelemetryRecord record : telemetryRecords) {
      if (!record.category().equals("source-mutation")) {
        continue;
      }
      SourceMutationRecord mutation = decodeSourceMutation(record);
      if (capturedTables.contains(mutation.tableDisplayName())) {
        mutations.add(mutation);
      }
    }
    return List.copyOf(mutations);
  }

  static SourceMutationRecord decodeSourceMutation(ScenarioTelemetryRecord record) {
    Objects.requireNonNull(record, "record");
    Map<String, String> fields = parseFields(record.detail());
    return new SourceMutationRecord(
        record.sequenceNumber(),
        requireField(fields, "operation"),
        requireField(fields, "table"),
        requireField(fields, "pk"),
        fields.get("before"),
        fields.get("after"));
  }

  private static void verifyPerKeyLogLedger(
      List<SourceMutationRecord> sourceMutations, List<ScenarioEventRecord> sinkEvents) {
    Map<EventKey, List<SourceMutationRecord>> sourceByKey = new LinkedHashMap<>();
    for (SourceMutationRecord mutation : sourceMutations) {
      sourceByKey.computeIfAbsent(mutation.key(), ignored -> new ArrayList<>()).add(mutation);
    }

    Map<EventKey, Integer> highestContiguousMutationIndexByKey = new HashMap<>();
    for (ScenarioEventRecord event : sinkEvents) {
      if (!event.captureOrigin().equals("LOG") || !isUserRowOperation(event.operationType())) {
        continue;
      }

      EventKey key = new EventKey(event.tableDisplayName(), event.primaryKeyLiteral());
      List<SourceMutationRecord> expectedMutations = sourceByKey.get(key);
      if (expectedMutations == null || expectedMutations.isEmpty()) {
        throw new IllegalStateException(
            "Observed captured LOG event without a matching recorded source mutation for "
                + key.tableDisplayName()
                + " pk="
                + key.primaryKeyLiteral()
                + ": operation="
                + event.operationType()
                + " before="
                + event.beforePayload()
                + " after="
                + event.afterPayload());
      }

      int matchedIndex = findMatchingMutationIndex(expectedMutations, event);
      if (matchedIndex < 0) {
        throw new IllegalStateException(
            "Observed captured LOG event did not match any recorded source mutation for "
                + key.tableDisplayName()
                + " pk="
                + key.primaryKeyLiteral()
                + ": operation="
                + event.operationType()
                + " before="
                + event.beforePayload()
                + " after="
                + event.afterPayload());
      }

      int highestContiguousIndex = highestContiguousMutationIndexByKey.getOrDefault(key, -1);
      if (matchedIndex <= highestContiguousIndex) {
        continue;
      }
      if (matchedIndex > highestContiguousIndex + 1) {
        throw new IllegalStateException(
            "Observed captured LOG event skipped an earlier source mutation for "
                + key.tableDisplayName()
                + " pk="
                + key.primaryKeyLiteral()
                + ": saw index "
                + matchedIndex
                + " before index "
                + (highestContiguousIndex + 1));
      }
      highestContiguousMutationIndexByKey.put(key, matchedIndex);
    }

    for (Map.Entry<EventKey, List<SourceMutationRecord>> entry : sourceByKey.entrySet()) {
      int highestContiguousIndex =
          highestContiguousMutationIndexByKey.getOrDefault(entry.getKey(), -1);
      if (highestContiguousIndex < entry.getValue().size() - 1) {
        SourceMutationRecord firstMissing = entry.getValue().get(highestContiguousIndex + 1);
        throw new IllegalStateException(
            "Sink LOG ledger did not surface all recorded source mutations for "
                + entry.getKey().tableDisplayName()
                + " pk="
                + entry.getKey().primaryKeyLiteral()
                + "; first missing mutation operation="
                + firstMissing.operation()
                + " before="
                + firstMissing.beforePayload()
                + " after="
                + firstMissing.afterPayload());
      }
    }
  }

  private static void verifyRefreshUpdatesDoNotContradictLatestLogState(
      List<ScenarioEventRecord> sinkEvents) {
    Map<EventKey, String> latestLogStateByKey = new HashMap<>();
    Map<EventKey, Boolean> sawLogStateByKey = new HashMap<>();

    for (ScenarioEventRecord event : sinkEvents) {
      EventKey key = new EventKey(event.tableDisplayName(), event.primaryKeyLiteral());
      if (event.captureOrigin().equals("LOG") && isUserRowOperation(event.operationType())) {
        sawLogStateByKey.put(key, Boolean.TRUE);
        latestLogStateByKey.put(key, event.afterPayload());
        continue;
      }

      if (!event.captureOrigin().equals("SELECT") || !event.operationType().equals("UPDATE")) {
        continue;
      }
      if (!sawLogStateByKey.getOrDefault(key, Boolean.FALSE)) {
        continue;
      }

      String latestLogState = latestLogStateByKey.get(key);
      if (!payloadsMatch(latestLogState, event.afterPayload())) {
        throw new IllegalStateException(
            "Observed SELECT refresh UPDATE contradicted the latest prior LOG state for "
                + key.tableDisplayName()
                + " pk="
                + key.primaryKeyLiteral()
                + ": latestLogState="
                + latestLogState
                + " refreshUpdate="
                + event.afterPayload());
      }
    }
  }

  private static boolean isUserRowOperation(String operationType) {
    return operationType.equals("INSERT")
        || operationType.equals("UPDATE")
        || operationType.equals("DELETE");
  }

  private static boolean payloadsMatch(String left, String right) {
    return Objects.equals(left, right);
  }

  private static int findMatchingMutationIndex(
      List<SourceMutationRecord> expectedMutations, ScenarioEventRecord event) {
    for (int index = 0; index < expectedMutations.size(); index++) {
      SourceMutationRecord mutation = expectedMutations.get(index);
      if (mutation.operation().equals(event.operationType())
          && payloadsMatch(mutation.beforePayload(), event.beforePayload())
          && payloadsMatch(mutation.afterPayload(), event.afterPayload())) {
        return index;
      }
    }
    return -1;
  }

  private static Map<String, String> parseFields(String detail) {
    Objects.requireNonNull(detail, "detail");
    Map<String, String> fields = new LinkedHashMap<>();
    for (String segment : detail.split(";")) {
      int equalsIndex = segment.indexOf('=');
      if (equalsIndex <= 0 || equalsIndex == segment.length() - 1) {
        throw new IllegalArgumentException("Invalid source-mutation telemetry detail: " + detail);
      }
      fields.put(
          segment.substring(0, equalsIndex),
          decodeNullable(segment.substring(equalsIndex + 1)));
    }
    return fields;
  }

  private static String requireField(Map<String, String> fields, String key) {
    String value = fields.get(key);
    if (value == null) {
      throw new IllegalArgumentException("Missing source-mutation telemetry field: " + key);
    }
    return value;
  }

  private static String encodeNullable(String value) {
    if (value == null) {
      return "~";
    }
    return BASE64_ENCODER.encodeToString(value.getBytes(StandardCharsets.UTF_8));
  }

  private static String decodeNullable(String value) {
    if (value.equals("~")) {
      return null;
    }
    return new String(BASE64_DECODER.decode(value), StandardCharsets.UTF_8);
  }

  private record EventKey(String tableDisplayName, String primaryKeyLiteral) {}

  record SourceMutationRecord(
      long telemetrySequenceNumber,
      String operation,
      String tableDisplayName,
      String primaryKeyLiteral,
      String beforePayload,
      String afterPayload) {
    private EventKey key() {
      return new EventKey(tableDisplayName, primaryKeyLiteral);
    }
  }
}
