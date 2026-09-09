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
  private Optional<Long> sessionTimeoutMs;
  private Optional<Long> heartbeatIntervalMs;
  private Optional<Integer> numStandbyReplicas;
  private Optional<Long> initialRebalanceDelayMs;

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

  public static List<String> getConfigs() {
    return configs;
  }

  // TODO: this is just nasty, find a better approach
  public static Optional<? extends Number> getConfig(
      final GroupConfig groupConfig, final String configKey) {
    return Map.of(
            STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG, groupConfig.getSessionTimeoutMs(),
            STREAMS_NUM_STANDBY_REPLICAS_CONFIG, groupConfig.getNumStandbyReplicas(),
            STREAMS_SESSION_TIMEOUT_MS_CONFIG, groupConfig.getSessionTimeoutMs(),
            STREAMS_INITIAL_REBALANCE_DELAY_MS_CONFIG, groupConfig.getInitialRebalanceDelayMs())
        .get(configKey);
  }
}
