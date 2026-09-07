package com.purbon.kafka.topology.model.users;

import java.util.Optional;

public class GroupConfig {

  public static final Long DEFAULT_HEARTBEAT_INTERVAL_MS = 5000L;
  public static final Integer DEFAULT_NUM_STANDBY_REPLICAS = 0;
  public static final Long DEFAULT_SESSION_TIMEOUT_MS = 45000L;
  public static final Long DEFAULT_INITIAL_REBALANCE_MS = 3000L;

  private Optional<String> groupId;
  private Optional<Long> sessionTimeoutMs;
  private Optional<Long> heartbeatIntervalMs;
  private Optional<Integer> numStandbyReplicas;
  private Optional<Long> initialRebalanceDelayMs;

  public GroupConfig() {
    this.groupId = Optional.empty();
    this.sessionTimeoutMs = Optional.empty();
    this.heartbeatIntervalMs = Optional.empty();
    this.numStandbyReplicas = Optional.empty();
    this.initialRebalanceDelayMs = Optional.empty();
  }

  public Optional<String> getGroupId() {
    return groupId;
  }

  public void setGroupId(Optional<String> groupId) {
    this.groupId = groupId;
  }

  public Optional<Long> getSessionTimeoutMs() {
    return sessionTimeoutMs;
  }

  public void setSessionTimeoutMs(Optional<Long> sessionTimeoutMs) {
    this.sessionTimeoutMs = sessionTimeoutMs;
  }

  public Optional<Long> getHeartbeatIntervalMs() {
    return heartbeatIntervalMs;
  }

  public void setHeartbeatIntervalMs(Optional<Long> heartbeatIntervalMs) {
    this.heartbeatIntervalMs = heartbeatIntervalMs;
  }

  public Optional<Integer> getNumStandbyReplicas() {
    return numStandbyReplicas;
  }

  public void setNumStandbyReplicas(Optional<Integer> numStandbyReplicas) {
    this.numStandbyReplicas = numStandbyReplicas;
  }

  public Optional<Long> getInitialRebalanceDelayMs() {
    return initialRebalanceDelayMs;
  }

  public void setInitialRebalanceDelayMs(Optional<Long> initialRebalanceDelayMs) {
    this.initialRebalanceDelayMs = initialRebalanceDelayMs;
  }
}
