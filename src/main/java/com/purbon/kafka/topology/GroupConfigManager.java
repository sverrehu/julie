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
  private final Configuration config;

  public GroupConfigManager(TopologyBuilderAdminClient adminClient, Configuration config) {
    this.adminClient = adminClient;
      this.config = config;
  }

  @Override
  public void updatePlan(ExecutionPlan plan, Map<String, Topology> topologies) throws IOException {
    for (Map.Entry<String, Topology> entry : topologies.entrySet()) {
      Topology topology = entry.getValue();
      // TODO: returns 0 on second run for GroupManagerIT
      //Set<String> existingGroupIDs = this.adminClient.listGroups();
      Set<String> existingGroupIDs = loadClusterState(plan);
      Set<Action> createGroups = new LinkedHashSet<>();
      Set<Action> deleteGroups = new LinkedHashSet<>();
      for (Project project : topology.getProjects()) {
        project
            .getStreams()
            .forEach(
                stream -> {
                  if (stream.getGroupConfig().isPresent()) {
                    // NOTE: internal testing showed that not all streams applications use a 1:1
                    // relationship between application ID and group ID. For now, require users to
                    // update applicationID for every group update, and in the future consider
                    // adding
                    // an optional `groupId` field with fallback value to `applicationId`
                    final String applicationId = stream.getApplicationId().orElseThrow();
                    // If GroupID is not in cluster, but in topology: create/update
                    // If GroupID is in cluster, but not topology: delete
                    if (!existingGroupIDs.contains(applicationId)) {
                      createGroups.add(
                          new UpdateGroupConfigAction(
                              this.adminClient, stream.getGroupConfig().get()));
                    } else {
                      deleteGroups.add(
                          new ResetGroupConfigAction(
                              this.adminClient, stream.getGroupConfig().get()));
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

  private Set<String> loadClusterState(final ExecutionPlan plan) {
    if(config.fetchStateFromTheCluster()) {
      return this.adminClient.listGroups();
    }
    return plan.getStreamGroups();
  }

  @Override
  public void printCurrentState(PrintStream out) throws IOException {
    out.println("List of groups");
    out.println(this.adminClient.listGroups());
  }
}
