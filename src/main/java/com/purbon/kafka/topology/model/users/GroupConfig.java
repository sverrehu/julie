package com.purbon.kafka.topology.model.users;

import static org.apache.kafka.coordinator.group.GroupConfig.STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.STREAMS_INITIAL_REBALANCE_DELAY_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.STREAMS_NUM_STANDBY_REPLICAS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.STREAMS_SESSION_TIMEOUT_MS_CONFIG;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class GroupConfig {

  private static final List<String> configs =
      List.of(
          STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG,
          STREAMS_NUM_STANDBY_REPLICAS_CONFIG,
          STREAMS_SESSION_TIMEOUT_MS_CONFIG,
          STREAMS_INITIAL_REBALANCE_DELAY_MS_CONFIG);

  private String groupId;
  private Optional<Integer> sessionTimeoutMs;
  private Optional<Integer> heartbeatIntervalMs;
  private Optional<Integer> numStandbyReplicas;
  private Optional<Integer> initialRebalanceDelayMs;

  public GroupConfig() {
    this.sessionTimeoutMs = Optional.empty();
    this.heartbeatIntervalMs = Optional.empty();
    this.numStandbyReplicas = Optional.empty();
    this.initialRebalanceDelayMs = Optional.empty();
  }

  public String getGroupId() {
    return groupId;
  }

  public void setGroupId(final String applicationId) {
    this.groupId = applicationId;
  }

  public Optional<Integer> getSessionTimeoutMs() {
    return sessionTimeoutMs;
  }

  public void setSessionTimeoutMs(Optional<Integer> sessionTimeoutMs) {
    this.sessionTimeoutMs = sessionTimeoutMs;
  }

  public Optional<Integer> getHeartbeatIntervalMs() {
    return heartbeatIntervalMs;
  }

  public void setHeartbeatIntervalMs(Optional<Integer> heartbeatIntervalMs) {
    this.heartbeatIntervalMs = heartbeatIntervalMs;
  }

  public Optional<Integer> getNumStandbyReplicas() {
    return numStandbyReplicas;
  }

  public void setNumStandbyReplicas(Optional<Integer> numStandbyReplicas) {
    this.numStandbyReplicas = numStandbyReplicas;
  }

  public Optional<Integer> getInitialRebalanceDelayMs() {
    return initialRebalanceDelayMs;
  }

  public void setInitialRebalanceDelayMs(Optional<Integer> initialRebalanceDelayMs) {
    this.initialRebalanceDelayMs = initialRebalanceDelayMs;
  }

  public static List<String> getConfigs() {
    return configs;
  }

  // TODO: this is just nasty, find a better approach
  public static Optional<Integer> getConfig(final GroupConfig groupConfig, final String configKey) {
    return Map.of(
            STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG, groupConfig.getSessionTimeoutMs(),
            STREAMS_NUM_STANDBY_REPLICAS_CONFIG, groupConfig.getNumStandbyReplicas(),
            STREAMS_SESSION_TIMEOUT_MS_CONFIG, groupConfig.getSessionTimeoutMs(),
            STREAMS_INITIAL_REBALANCE_DELAY_MS_CONFIG, groupConfig.getInitialRebalanceDelayMs())
        .get(configKey);
  }
}
