package com.purbon.kafka.topology;

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
                  final String applicationId = stream.getApplicationId().orElseThrow();
                  if (stream.getGroupConfig().isPresent()) {
                    // If GroupID is not in cluster, but in topology: create/update
                    // If GroupID is in cluster, but not topology: delete
                    if (!existingGroupIDs.contains(applicationId)) {
                      createGroups.add(
                          new UpdateGroupConfigAction(adminClient, stream.getGroupConfig().get()));
                    } else {
                      deleteGroups.add(
                          new ResetGroupConfigAction(adminClient, stream.getGroupConfig().get()));
                    }
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
}
