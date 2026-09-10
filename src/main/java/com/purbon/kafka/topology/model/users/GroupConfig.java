package com.purbon.kafka.topology.model.users;

import static org.apache.kafka.coordinator.group.GroupConfig.STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.STREAMS_INITIAL_REBALANCE_DELAY_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.STREAMS_NUM_STANDBY_REPLICAS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.STREAMS_SESSION_TIMEOUT_MS_CONFIG;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class GroupConfig {

  @Getter
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

  // TODO: this is just nasty, find a better approach
  public static Optional<Integer> getConfig(final GroupConfig groupConfig, final String configKey) {
    return Map.of(
            STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG, groupConfig.getHeartbeatIntervalMs(),
            STREAMS_NUM_STANDBY_REPLICAS_CONFIG, groupConfig.getNumStandbyReplicas(),
            STREAMS_SESSION_TIMEOUT_MS_CONFIG, groupConfig.getSessionTimeoutMs(),
            STREAMS_INITIAL_REBALANCE_DELAY_MS_CONFIG, groupConfig.getInitialRebalanceDelayMs())
        .get(configKey);
  }
}
