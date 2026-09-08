package com.purbon.kafka.topology.actions.groups;

import com.purbon.kafka.topology.actions.BaseAction;
import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import com.purbon.kafka.topology.model.users.GroupConfig;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class UpdateGroupConfigAction extends BaseAction {

  private final Logger LOGGER = LogManager.getLogger(this.getClass());

  private final TopologyBuilderAdminClient adminClient;
  private final GroupConfig groupConfig;

  public UpdateGroupConfigAction(TopologyBuilderAdminClient adminClient, GroupConfig groupConfig) {
    this.adminClient = adminClient;
    this.groupConfig = groupConfig;
  }

  @Override
  protected Map<String, Object> props() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("Operation", getClass().getName());
    map.put("GroupConfigs", groupConfig);
    map.put("Action", "update");
    return map;
  }

  @Override
  protected List<Map<String, Object>> detailedProps() {
    return List.of(props());
  }

  @Override
  public void run() throws IOException {
    this.adminClient.updateGroupConfig(groupConfig);
  }
}
