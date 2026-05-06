package io.github.aandreakis.dblog.runtime.observer;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.runtime.sql.SourceFlowControlSnapshot;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Objects;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Shared runtime observability state for one application context on the final runtime surface. */
public final class DbLogRuntimeObservability {
  private static final Logger log = LoggerFactory.getLogger(DbLogRuntimeObservability.class);
  private static final String DEFAULT_REQUEST_SUBMISSION_MESSAGE =
      "Request submission is unavailable because no request-processing runtime is active in this process.";

  private volatile String adapter = "none";
  private volatile String lastFailureType = null;
  private volatile String lastFailureMessage = null;
  private volatile boolean requestSubmissionAvailable = false;
  private volatile String requestSubmissionMessage = DEFAULT_REQUEST_SUBMISSION_MESSAGE;
  private volatile Supplier<SourceFlowControlSnapshot> sourceFlowControlProvider =
      SourceFlowControlSnapshot::unavailable;
  private volatile SourceFlowControlSnapshot lastSourceFlowControlSnapshot =
      SourceFlowControlSnapshot.unavailable();
  private volatile ComponentStatusSnapshot sourceStatus =
      new ComponentStatusSnapshot("INACTIVE", null, null, null, null, null, null, null, null);
  private volatile ComponentStatusSnapshot sinkStatus =
      new ComponentStatusSnapshot("INACTIVE", null, null, null, null, null, null, null, null);

  public DbLogRuntimeObservability() {}

  public <TX extends SourceTransaction<?>> RuntimeLoopObserver<TX> observerCurrent(
      String adapterLabel, MeterRegistry meterRegistry) {
    return new ObservabilityRuntimeLoopObserver<>(this, adapterLabel, meterRegistry);
  }

  public void runtimeStarted(String modeName, String adapterLabel) {
    adapter = adapterLabel;
    lastFailureType = null;
    lastFailureMessage = null;
    sourceStatus = inactiveStatus();
    sinkStatus = inactiveStatus();
    sourceFlowControlProvider = SourceFlowControlSnapshot::unavailable;
    lastSourceFlowControlSnapshot = SourceFlowControlSnapshot.unavailable();
    requestSubmissionUnavailable(DEFAULT_REQUEST_SUBMISSION_MESSAGE);
    log.info("DBLog runtime started mode={} adapter={}", modeName, adapterLabel);
  }

  public void runtimeStopped(String modeName, String adapterLabel) {
    adapter = adapterLabel;
    lastFailureType = null;
    lastFailureMessage = null;
    sourceFlowControlProvider = SourceFlowControlSnapshot::unavailable;
    lastSourceFlowControlSnapshot = SourceFlowControlSnapshot.unavailable();
    sourceStatus = inactiveStatus();
    sinkStatus = inactiveStatus();
    requestSubmissionUnavailable(
        "Request submission is unavailable because the active request-processing runtime has stopped.");
    log.info("DBLog runtime stopped mode={} adapter={}", modeName, adapterLabel);
  }

  public void runtimeFailed(
      String modeName, String adapterLabel, Throwable failure, MeterRegistry meterRegistry) {
    adapter = adapterLabel;
    lastFailureType = failure.getClass().getSimpleName();
    lastFailureMessage = failure.getMessage();
    requestSubmissionUnavailable(
        "Request submission is unavailable because the active request-processing runtime failed closed.");
    Counter.builder("dblog.runtime.fail_closed.total")
        .tags(
            "mode",
            modeName,
            "adapter",
            adapterLabel,
            "exception",
            lastFailureType)
        .register(Objects.requireNonNull(meterRegistry, "meterRegistry"))
        .increment();
    log.warn(
        "DBLog runtime marked failed closed mode={} adapter={} failureType={} failureMessage={}",
        modeName,
        adapterLabel,
        lastFailureType,
        lastFailureMessage);
  }

  public void requestSubmissionAvailable() {
    requestSubmissionAvailable = true;
    requestSubmissionMessage = null;
  }

  public void requestSubmissionUnavailable(String message) {
    String requiredMessage = requireNonBlank(message, "message");
    requestSubmissionAvailable = false;
    requestSubmissionMessage = requiredMessage;
  }

  public RequestSubmissionAvailability requestSubmissionAvailability() {
    return new RequestSubmissionAvailability(requestSubmissionAvailable, requestSubmissionMessage);
  }

  public void sourceFlowControlProvider(Supplier<SourceFlowControlSnapshot> provider) {
    sourceFlowControlProvider = provider;
  }

  public void sourceFlowControlProviderCurrent(Supplier<SourceFlowControlSnapshot> provider) {
    sourceFlowControlProvider(provider);
  }

  public void clearSourceFlowControlProvider() {
    sourceFlowControlProvider = null;
  }

  public void sourceFlowControlSnapshot(SourceFlowControlSnapshot snapshot) {
    lastSourceFlowControlSnapshot =
        snapshot == null ? SourceFlowControlSnapshot.unavailable() : snapshot;
  }

  public void sourceFlowControlSnapshotCurrent(SourceFlowControlSnapshot snapshot) {
    sourceFlowControlSnapshot(snapshot);
  }

  public SourceFlowControlSnapshot sourceFlowControlSnapshot() {
    try {
      Supplier<SourceFlowControlSnapshot> provider = sourceFlowControlProvider;
      SourceFlowControlSnapshot snapshot = provider == null ? null : provider.get();
      if (snapshot != null) {
        lastSourceFlowControlSnapshot = snapshot;
        return snapshot;
      }
    } catch (RuntimeException ex) {
      // Fall through to the last successful snapshot so short-lived provider failures
      // do not erase operator-facing source-flow visibility.
    }
    return lastSourceFlowControlSnapshot == null
        ? SourceFlowControlSnapshot.unavailable()
        : lastSourceFlowControlSnapshot;
  }

  public SourceFlowControlSnapshot sourceFlowControlSnapshotCurrent() {
    return sourceFlowControlSnapshot();
  }

  public void sourceStarting() {
    updateSourceStatus(
        new ComponentStatusSnapshot("STARTING", null, null, null, null, null, null, null, null));
  }

  public void sourceUp() {
    ComponentStatusSnapshot current = sourceStatus;
    updateSourceStatus(
        new ComponentStatusSnapshot(
            "UP",
            null,
            null,
            null,
            current.checkpoint(),
            current.checkpointUpdatedAt(),
            current.lastHeartbeatAt(),
            null,
            null));
  }

  public void sourceRetrying(Throwable failure) {
    updateSourceStatus(retryingStatus(sourceStatus, failure));
  }

  public void sourceFailed(Throwable failure) {
    updateSourceStatus(failedStatus(failure));
  }

  public void sourceInactive() {
    updateSourceStatus(inactiveStatus());
  }

  public void sinkStarting() {
    updateSinkStatus(
        new ComponentStatusSnapshot("STARTING", null, null, null, null, null, null, null, null));
  }

  public void sinkUp() {
    ComponentStatusSnapshot current = sinkStatus;
    updateSinkStatus(
        new ComponentStatusSnapshot(
            "UP",
            null,
            null,
            null,
            null,
            null,
            null,
            current.lastAppliedSourcePosition(),
            current.lastApplySuccessAt()));
  }

  public void sinkRetrying(Throwable failure) {
    updateSinkStatus(retryingStatus(sinkStatus, failure));
  }

  public void sinkFailed(Throwable failure) {
    updateSinkStatus(failedStatus(failure));
  }

  public void sinkInactive() {
    updateSinkStatus(inactiveStatus());
  }

  public void sourceCheckpointLoaded(String checkpoint) {
    sourceStatus =
        new ComponentStatusSnapshot(
            sourceStatus.state(),
            sourceStatus.failureType(),
            sourceStatus.failureMessage(),
            sourceStatus.retryingSince(),
            checkpoint,
            sourceStatus.checkpointUpdatedAt(),
            sourceStatus.lastHeartbeatAt(),
            sourceStatus.lastAppliedSourcePosition(),
            sourceStatus.lastApplySuccessAt());
  }

  public void sourceCheckpointAdvanced(String checkpoint, String updatedAt) {
    sourceStatus =
        new ComponentStatusSnapshot(
            sourceStatus.state(),
            sourceStatus.failureType(),
            sourceStatus.failureMessage(),
            sourceStatus.retryingSince(),
            checkpoint,
            updatedAt,
            sourceStatus.lastHeartbeatAt(),
            sourceStatus.lastAppliedSourcePosition(),
            sourceStatus.lastApplySuccessAt());
  }

  public void sourceHeartbeatSucceeded(String heartbeatAt) {
    sourceStatus =
        new ComponentStatusSnapshot(
            sourceStatus.state(),
            sourceStatus.failureType(),
            sourceStatus.failureMessage(),
            sourceStatus.retryingSince(),
            sourceStatus.checkpoint(),
            sourceStatus.checkpointUpdatedAt(),
            heartbeatAt,
            sourceStatus.lastAppliedSourcePosition(),
            sourceStatus.lastApplySuccessAt());
  }

  public void sinkApplySucceeded(String sourcePosition, String appliedAt) {
    sinkStatus =
        new ComponentStatusSnapshot(
            sinkStatus.state(),
            sinkStatus.failureType(),
            sinkStatus.failureMessage(),
            sinkStatus.retryingSince(),
            sinkStatus.checkpoint(),
            sinkStatus.checkpointUpdatedAt(),
            sinkStatus.lastHeartbeatAt(),
            sourcePosition,
            appliedAt);
  }

  private static ComponentStatusSnapshot retryingStatus(
      ComponentStatusSnapshot current, Throwable failure) {
    String retryingSince =
        "RETRYING".equals(current.state()) && current.retryingSince() != null
            ? current.retryingSince()
            : java.time.Instant.now().toString();
    return new ComponentStatusSnapshot(
        "RETRYING",
        failure.getClass().getSimpleName(),
        failure.getMessage(),
        retryingSince,
        current.checkpoint(),
        current.checkpointUpdatedAt(),
        current.lastHeartbeatAt(),
        current.lastAppliedSourcePosition(),
        current.lastApplySuccessAt());
  }

  private static ComponentStatusSnapshot failedStatus(Throwable failure) {
    return new ComponentStatusSnapshot(
        "FAILED_CLOSED",
        failure.getClass().getSimpleName(),
        failure.getMessage(),
        null,
        null,
        null,
        null,
        null,
        null);
  }

  private static ComponentStatusSnapshot inactiveStatus() {
    return new ComponentStatusSnapshot("INACTIVE", null, null, null, null, null, null, null, null);
  }

  private void updateSourceStatus(ComponentStatusSnapshot status) {
    ComponentStatusSnapshot previous = sourceStatus;
    sourceStatus = status;
    logComponentStatusTransition("source", previous, status);
  }

  private void updateSinkStatus(ComponentStatusSnapshot status) {
    ComponentStatusSnapshot previous = sinkStatus;
    sinkStatus = status;
    logComponentStatusTransition("sink", previous, status);
  }

  private void logComponentStatusTransition(
      String component,
      ComponentStatusSnapshot previous,
      ComponentStatusSnapshot current) {
    if (sameStatusForLogging(previous, current)) {
      return;
    }
    String previousState = previous == null ? "<none>" : previous.state();
    if ("RETRYING".equals(current.state()) || "FAILED_CLOSED".equals(current.state())) {
      log.warn(
          "DBLog {} status changed adapter={} from={} to={} failureType={} failureMessage={}",
          component,
          adapter,
          previousState,
          current.state(),
          current.failureType(),
          current.failureMessage());
      return;
    }
    log.info(
        "DBLog {} status changed adapter={} from={} to={}",
        component,
        adapter,
        previousState,
        current.state());
  }

  private static boolean sameStatusForLogging(
      ComponentStatusSnapshot previous, ComponentStatusSnapshot current) {
    if (previous == null) {
      return false;
    }
    return Objects.equals(previous.state(), current.state())
        && Objects.equals(previous.failureType(), current.failureType())
        && Objects.equals(previous.failureMessage(), current.failureMessage());
  }

  public record RequestSubmissionAvailability(boolean available, String message) {}

  public record ComponentStatusSnapshot(
      String state,
      String failureType,
      String failureMessage,
      String retryingSince,
      String checkpoint,
      String checkpointUpdatedAt,
      String lastHeartbeatAt,
      String lastAppliedSourcePosition,
      String lastApplySuccessAt) {}

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

}
