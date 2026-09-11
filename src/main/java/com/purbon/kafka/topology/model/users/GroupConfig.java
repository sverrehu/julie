package com.purbon.kafka.topology.model.users;

import static org.apache.kafka.coordinator.group.GroupConfig.STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.STREAMS_INITIAL_REBALANCE_DELAY_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.STREAMS_NUM_STANDBY_REPLICAS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.STREAMS_SESSION_TIMEOUT_MS_CONFIG;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class GroupConfig {

  private String groupId;
  private Optional<Integer> sessionTimeoutMs;
  private Optional<Integer> heartbeatIntervalMs;
  private Optional<Integer> numStandbyReplicas;
  private Optional<Integer> initialRebalanceDelayMs;
  private final Map<String, Optional<Integer>> configs;

  // TODO: specify broker defaults centrally. For now, these are configured
  // based on current defaults (v4.3.X) using min values where applicable
  public GroupConfig() {
    this.sessionTimeoutMs = Optional.of(60000);
    this.heartbeatIntervalMs = Optional.of(5000);
    this.numStandbyReplicas = Optional.of(0);
    this.initialRebalanceDelayMs = Optional.of(3000);
    this.configs = new HashMap<>();
  }

  public String getGroupId() {
    return groupId;
  }

  public void setGroupId(final String groupId) {
    this.groupId = groupId;
  }

  public Set<String> getConfigProperties() {
    return this.configs.keySet();
  }

  public Optional<Integer> getSessionTimeoutMs() {
    return sessionTimeoutMs;
  }

  public void setSessionTimeoutMs(final Optional<Integer> sessionTimeoutMs) {
    this.sessionTimeoutMs = sessionTimeoutMs;
    updateConfigValue(STREAMS_SESSION_TIMEOUT_MS_CONFIG, this.sessionTimeoutMs);
  }

  public Optional<Integer> getHeartbeatIntervalMs() {
    return heartbeatIntervalMs;
  }

  public void setHeartbeatIntervalMs(Optional<Integer> heartbeatIntervalMs) {
    this.heartbeatIntervalMs = heartbeatIntervalMs;
    updateConfigValue(STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG, this.heartbeatIntervalMs);
  }

  public Optional<Integer> getNumStandbyReplicas() {
    return numStandbyReplicas;
  }

  public void setNumStandbyReplicas(Optional<Integer> numStandbyReplicas) {
    this.numStandbyReplicas = numStandbyReplicas;
    updateConfigValue(STREAMS_NUM_STANDBY_REPLICAS_CONFIG, this.numStandbyReplicas);
  }

  public Optional<Integer> getInitialRebalanceDelayMs() {
    return initialRebalanceDelayMs;
  }

  public void setInitialRebalanceDelayMs(Optional<Integer> initialRebalanceDelayMs) {
    this.initialRebalanceDelayMs = initialRebalanceDelayMs;
    updateConfigValue(STREAMS_INITIAL_REBALANCE_DELAY_MS_CONFIG, this.initialRebalanceDelayMs);
  }

  public Map<String, Optional<Integer>> getConfigs() {
    return configs;
  }

  private void updateConfigValue(final String configKey, final Optional<Integer> configValue) {
    if (this.configs.get(configKey) != null) {
      this.configs.replace(configKey, configValue);
    } else {
      this.configs.put(configKey, configValue);
    }
  }

  public Optional<Integer> getConfigValueForKey(final String configKey) {
    Optional<Integer> configValue = Optional.empty();
    for (Map.Entry<String, Optional<Integer>> entry : configs.entrySet()) {
      if (entry.getKey().equals(configKey)) {
        configValue = entry.getValue();
      }
    }
    return configValue;
  }
}
