package com.purbon.kafka.topology;

import static org.apache.kafka.coordinator.group.GroupConfig.*;

import com.purbon.kafka.topology.actions.Action;
import com.purbon.kafka.topology.actions.groups.ResetGroupConfigAction;
import com.purbon.kafka.topology.actions.groups.UpdateGroupConfigAction;
import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import com.purbon.kafka.topology.model.Project;
import com.purbon.kafka.topology.model.Topology;
import java.io.IOException;
import java.io.PrintStream;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class GroupConfigManager implements ExecutionPlanUpdater {

  //  private static final List<String> configSettings = List.of(
  //          STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG,
  //          STREAMS_NUM_STANDBY_REPLICAS_CONFIG,
  //          STREAMS_SESSION_TIMEOUT_MS_CONFIG,
  //          STREAMS_INITIAL_REBALANCE_DELAY_MS_CONFIG
  //  );

  private final TopologyBuilderAdminClient adminClient;

  public GroupConfigManager(TopologyBuilderAdminClient adminClient) {
    this.adminClient = adminClient;
  }

  @Override
  public void updatePlan(ExecutionPlan plan, Map<String, Topology> topologies) throws IOException {
    for (Map.Entry<String, Topology> entry : topologies.entrySet()) {
      Topology topology = entry.getValue();
      Set<String> existingGroupIDs = adminClient.listGroups();
      Set<Action> createGroups = new LinkedHashSet<>();
      Set<Action> deleteGroups = new LinkedHashSet<>();
      for (Project project : topology.getProjects()) {
        project
            .getStreams()
            .forEach(
                stream -> {
                  if (stream.getGroupConfig().isPresent()) {
                    createGroups.add(
                        new UpdateGroupConfigAction(adminClient, stream.getGroupConfig().get()));
                  }
                  if (!existingGroupIDs.contains(stream.getApplicationId().orElseThrow())) {
                    deleteGroups.add(
                        new ResetGroupConfigAction(adminClient, stream.getGroupConfig().get()));
                  }
                });
      }
      if (!createGroups.isEmpty()) {
        createGroups.forEach(plan::add);
      }
      if (!deleteGroups.isEmpty()) {
        deleteGroups.forEach(plan::add);
      }
    }
  }

  @Override
  public void printCurrentState(PrintStream out) throws IOException {
    out.println("List of groups");
    out.println(adminClient.listGroups());
  }

  //  public static List<String> getConfigSettings() {
  //    return configSettings;
  //  }
}
