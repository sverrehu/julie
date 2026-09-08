package com.purbon.kafka.topology;

import com.purbon.kafka.topology.actions.groups.UpdateGroupConfigAction;
import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import com.purbon.kafka.topology.model.Project;
import com.purbon.kafka.topology.model.Topology;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;

public class GroupConfigManager implements ExecutionPlanUpdater {

  private final TopologyBuilderAdminClient adminClient;

  public GroupConfigManager(TopologyBuilderAdminClient adminClient) {
    this.adminClient = adminClient;
  }

  @Override
  public void updatePlan(ExecutionPlan plan, Map<String, Topology> topologies) throws IOException {
    for (Map.Entry<String, Topology> entry : topologies.entrySet()) {
      Topology topology = entry.getValue();
      for (Project project : topology.getProjects()) {
        project.getStreams().stream()
            .filter(stream -> stream.getGroupConfig().isPresent())
            .map(stream -> new UpdateGroupConfigAction(adminClient, stream.getGroupConfig().get()))
            .forEach(plan::add);
      }
    }
  }

  @Override
  public void printCurrentState(PrintStream out) throws IOException {
    out.println("List of groups");
    out.println(adminClient.listGroups());
  }
}
