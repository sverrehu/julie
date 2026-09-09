package com.purbon.kafka.topology.actions.groups;

import com.purbon.kafka.topology.actions.BaseAction;
import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import com.purbon.kafka.topology.model.users.GroupConfig;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResetGroupConfigAction extends BaseAction {

  private final TopologyBuilderAdminClient adminClient;
  private final GroupConfig groupConfig;

  public ResetGroupConfigAction(TopologyBuilderAdminClient adminClient, GroupConfig groupConfig) {
    this.adminClient = adminClient;
    this.groupConfig = groupConfig;
  }

  @Override
  protected Map<String, Object> props() {
    Map<String, Object> map = new HashMap<>();
    map.put("Operation", getClass().getName());
    map.put("GroupConfigs", groupConfig);
    map.put("Action", "delete");
    return map;
  }

  @Override
  protected List<Map<String, Object>> detailedProps() {
    return List.of(props());
  }

  @Override
  public void run() throws IOException {
    this.adminClient.resetGroupConfig(groupConfig);
  }
}
