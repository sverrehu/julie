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

  // TODO: specify broker defaults centrally. For now, these are configured
  // based on current defaults (v4.3.X) using min values where applicable
  public GroupConfig() {
    this.sessionTimeoutMs = Optional.of(60000);
    this.heartbeatIntervalMs = Optional.of(5000);
    this.numStandbyReplicas = Optional.of(0);
    this.initialRebalanceDelayMs = Optional.of(3000);
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
