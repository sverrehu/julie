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
import org.junit.*;

public class GroupConfigManagerIT {

  private static SaslPlaintextKafkaContainer container;
  private GroupConfigManager groupConfigManager;
  private AdminClient kafkaAdminClient;

  private ExecutionPlan plan;

  @BeforeClass
  public static void setup() {
    container =
        new SaslPlaintextKafkaContainer()
            .withUser(ContainerTestUtils.PRODUCER_USERNAME)
            .withUser(ContainerTestUtils.CONSUMER_USERNAME)
            .withUser(ContainerTestUtils.BACKUP_USERNAME)
            .withUser("streams-app-a");
    container.start();
  }

  @AfterClass
  public static void tearDown() {
    container.stop();
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
    final String applicationId = "app_stream_id";
    GroupConfig groupConfig = new GroupConfig(applicationId);
    groupConfig.setHeartbeatIntervalMs(Optional.of(6500));
    groupConfig.setInitialRebalanceDelayMs(Optional.of(4000));
    groupConfig.setSessionTimeoutMs(Optional.of(50000));
    groupConfig.setNumStandbyReplicas(Optional.of(1));

    KStream stream =
        new KStream(
            "streams-app-a",
            topics,
            observerPrincipals,
            Optional.of(applicationId),
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

    Assert.assertTrue(stream.getApplicationId().isPresent());
    final GroupConfig observedGroupConfig = observedStream.getGroupConfig().get();

    Assert.assertEquals(stream.getApplicationId().get(), observedGroupConfig.getGroupId());

    Assert.assertTrue(groupConfig.getHeartbeatIntervalMs().isPresent());
    Assert.assertTrue(observedGroupConfig.getHeartbeatIntervalMs().isPresent());
    Assert.assertEquals(
        groupConfig.getHeartbeatIntervalMs().get(),
        observedGroupConfig.getHeartbeatIntervalMs().get());

    Assert.assertTrue(groupConfig.getSessionTimeoutMs().isPresent());
    Assert.assertTrue(observedGroupConfig.getSessionTimeoutMs().isPresent());
    Assert.assertEquals(
        groupConfig.getSessionTimeoutMs().get(), observedGroupConfig.getSessionTimeoutMs().get());

    Assert.assertTrue(groupConfig.getNumStandbyReplicas().isPresent());
    Assert.assertTrue(observedGroupConfig.getNumStandbyReplicas().isPresent());
    Assert.assertEquals(
            groupConfig.getNumStandbyReplicas().get(),
            observedGroupConfig.getNumStandbyReplicas().get());

    Assert.assertTrue(groupConfig.getInitialRebalanceDelayMs().isPresent());
    Assert.assertTrue(observedGroupConfig.getInitialRebalanceDelayMs().isPresent());
    Assert.assertEquals(
        groupConfig.getInitialRebalanceDelayMs().get(),
        observedGroupConfig.getInitialRebalanceDelayMs().get());

    stream.setGroupConfig(Optional.of(new GroupConfig(applicationId)));
    project.setStreams(List.of(stream));

    plan.getActions().clear();

    groupConfigManager.updatePlan(plan, Map.of(project.getName(), topology));
    plan.run();

    KStream observedResetStream = topology.getProjects().getFirst().getStreams().getFirst();
    Assert.assertNotNull(stream);
    Assert.assertTrue(observedResetStream.getGroupConfig().isPresent());
    final GroupConfig observedResetStreamGroupConfig = observedResetStream.getGroupConfig().get();
    Assert.assertTrue(observedResetStreamGroupConfig.getSessionTimeoutMs().isPresent());
    final int resetSessionTimeoutMs = observedResetStreamGroupConfig.getSessionTimeoutMs().get();
    Assert.assertEquals(60000, resetSessionTimeoutMs);
    Assert.assertTrue(observedResetStreamGroupConfig.getHeartbeatIntervalMs().isPresent());
    final int resetHeartbeatIntervalMs =
        observedResetStreamGroupConfig.getHeartbeatIntervalMs().get();
    Assert.assertEquals(5000, resetHeartbeatIntervalMs);
    Assert.assertTrue(observedResetStreamGroupConfig.getNumStandbyReplicas().isPresent());
    final int resetNumStandbyReplicas =
        observedResetStreamGroupConfig.getNumStandbyReplicas().get();
    Assert.assertEquals(0, resetNumStandbyReplicas);
    Assert.assertTrue(observedResetStreamGroupConfig.getInitialRebalanceDelayMs().isPresent());
    final int resetInitialRebalanceDelayMs =
        observedResetStreamGroupConfig.getInitialRebalanceDelayMs().get();
    Assert.assertEquals(3000, resetInitialRebalanceDelayMs);
  }
}
