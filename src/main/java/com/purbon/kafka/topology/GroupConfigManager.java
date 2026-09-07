package com.purbon.kafka.topology;

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
        project
            .getStreams()
            .forEach(
                stream -> {
                  if (stream.getGroupConfig().isPresent()) {
                    adminClient.updateGroupConfig(stream.getGroupConfig().get());
                  }
                });
      }
    }
  }

  @Override
  public void printCurrentState(PrintStream out) throws IOException {
    out.println("List of groups");
    out.println(adminClient.listGroups());
  }
}
