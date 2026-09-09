package com.purbon.kafka.topology.integration;

import com.purbon.kafka.topology.BackendController;
import com.purbon.kafka.topology.ExecutionPlan;
import com.purbon.kafka.topology.GroupConfigManager;
import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import com.purbon.kafka.topology.integration.containerutils.ContainerTestUtils;
import com.purbon.kafka.topology.integration.containerutils.SaslPlaintextKafkaContainer;
import com.purbon.kafka.topology.model.Impl.ProjectImpl;
import com.purbon.kafka.topology.model.Impl.TopologyImpl;
import com.purbon.kafka.topology.model.Project;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.model.User;
import com.purbon.kafka.topology.model.users.GroupConfig;
import com.purbon.kafka.topology.model.users.KStream;
import java.io.IOException;
import java.util.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class GroupConfigManagerIT {

  private static SaslPlaintextKafkaContainer container;
  private GroupConfigManager groupConfigManager;
  private AdminClient kafkaAdminClient;

  private ExecutionPlan plan;

  @BeforeClass
  public static void setup() {
    container = new SaslPlaintextKafkaContainer();
    container.start();
  }

  @Before
  public void before() throws IOException {
    ContainerTestUtils.clearAclsAndTopics(container);
    kafkaAdminClient = ContainerTestUtils.getSaslJulieAdminClient(container);
    TopologyBuilderAdminClient topologyBuilderAdminClient =
        new TopologyBuilderAdminClient(kafkaAdminClient);
    plan = ExecutionPlan.init(new BackendController(), System.out);
    groupConfigManager = new GroupConfigManager(topologyBuilderAdminClient);
  }

  @Test
  public void testAddThenDeleteGroupConfig() throws IOException {
    Topology topology = new TopologyImpl();

    Project project = new ProjectImpl("project");

    Map<String, List<String>> topics = new HashMap<>();
    List<User> observerPrincipals = new ArrayList<>();
    GroupConfig groupConfig = new GroupConfig();
    groupConfig.setHeartbeatIntervalMs(Optional.of(5500L));
    groupConfig.setInitialRebalanceDelayMs(Optional.of(4000L));
    groupConfig.setSessionTimeoutMs(Optional.of(50000L));

    //    Topology topology = TestTopologyBuilder.createProject()
    //            .addKStream(new KStream(
    //                    "streams-app-a",
    //                    topics,
    //                    observerPrincipals,
    //                    Optional.of("app_stream_id"),
    //                    Optional.of(true),
    //                    Optional.of(groupConfig)))
    //            .addTopic("topic-A")
    //            .buildTopology();

    KStream stream =
        new KStream(
            "streams-app-a",
            topics,
            observerPrincipals,
            Optional.of("app_stream_id"),
            Optional.of(true),
            Optional.of(groupConfig));

    project.setStreams(List.of(stream));

    topology.addProject(project);

    groupConfigManager.updatePlan(plan, Map.of(project.getName(), topology));
    plan.run();

    KStream observedStream = topology.getProjects().getFirst().getStreams().getFirst();
    Assert.assertNotNull(observedStream);
    Assert.assertTrue(observedStream.getApplicationId().isPresent());
    Assert.assertTrue(observedStream.getGroupConfig().isPresent());
    Assert.assertEquals(
        stream.getApplicationId().get(), observedStream.getGroupConfig().get().getGroupId());
    Assert.assertEquals(
        groupConfig.getHeartbeatIntervalMs().get(),
        observedStream.getGroupConfig().get().getHeartbeatIntervalMs());
    Assert.assertEquals(
        groupConfig.getSessionTimeoutMs().get(),
        observedStream.getGroupConfig().get().getSessionTimeoutMs());
    Assert.assertEquals(
        groupConfig.getNumStandbyReplicas().get(),
        observedStream.getGroupConfig().get().getNumStandbyReplicas());
    Assert.assertEquals(
        groupConfig.getInitialRebalanceDelayMs().get(),
        observedStream.getGroupConfig().get().getInitialRebalanceDelayMs());

    stream.setGroupConfig(Optional.empty());
    project.setStreams(List.of(stream));

    plan.getActions().clear();

    groupConfigManager.updatePlan(plan, Map.of(project.getName(), topology));
    plan.run();

    KStream observedResetStream = topology.getProjects().getFirst().getStreams().getFirst();
    Assert.assertNotNull(stream);
    Assert.assertTrue(observedResetStream.getGroupConfig().isPresent());
    final GroupConfig observedResetStreamGroupConfig = observedResetStream.getGroupConfig().get();
    Assert.assertTrue(observedResetStreamGroupConfig.getSessionTimeoutMs().isPresent());
    final long resetSessionTimeoutMs = observedResetStreamGroupConfig.getSessionTimeoutMs().get();
    Assert.assertEquals(45000L, resetSessionTimeoutMs);
    Assert.assertTrue(observedResetStreamGroupConfig.getHeartbeatIntervalMs().isPresent());
    final long resetHeartbeatIntervalMs =
        observedResetStreamGroupConfig.getHeartbeatIntervalMs().get();
    Assert.assertEquals(5000L, resetHeartbeatIntervalMs);
    Assert.assertTrue(observedResetStreamGroupConfig.getNumStandbyReplicas().isPresent());
    final int resetNumStandbyReplicas =
        observedResetStreamGroupConfig.getNumStandbyReplicas().get();
    Assert.assertEquals(0, resetNumStandbyReplicas);
    Assert.assertTrue(observedResetStreamGroupConfig.getInitialRebalanceDelayMs().isPresent());
    final long resetInitialRebalanceDelayMs =
        observedResetStreamGroupConfig.getInitialRebalanceDelayMs().get();
    Assert.assertEquals(3000L, resetInitialRebalanceDelayMs);
  }
}
