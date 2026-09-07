package com.purbon.kafka.topology.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.purbon.kafka.topology.integration.containerutils.ContainerTestUtils;
import com.purbon.kafka.topology.integration.containerutils.SaslPlaintextKafkaContainer;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public final class StreamsGroupsIT {

  public static final String TOPIC_A = "topic-A";
  public static final String STREAMS_APP_ID = "streams-appid";

  public static final String HEARTBEAT_INTERVAL_MS = "6000";
  public static final String NUM_STANDBY_REPLICAS = "2";
  public static final String SESSION_TIMEOUT_MS = "60000";
  public static final String INITIAL_REBALANCE_MS = "2000";
  private static final String CONSUMER_GROUP = "streams-appid";

  private static SaslPlaintextKafkaContainer container;
  private AdminClient adminClient;

  @BeforeClass
  public static void beforeClass() {
    container =
        new SaslPlaintextKafkaContainer()
            .withUser(ContainerTestUtils.PRODUCER_USERNAME)
            .withUser(ContainerTestUtils.CONSUMER_USERNAME)
            .withUser(ContainerTestUtils.BACKUP_USERNAME)
            .withUser("streamsapp-a")
            .withUser("streamsapp-b");
    container.start();
  }

  @AfterClass
  public static void afterClass() {
    container.stop();
  }

  @Before
  public void before() {
    ContainerTestUtils.clearAclsAndTopics(container);
    ContainerTestUtils.populateAcls(
        container, "/streams-groups-it.yaml", "/integration-tests.properties");
    adminClient = ContainerTestUtils.getSaslJulieAdminClient(container);
  }

  @Test
  public void shouldOverrideGroupConfigs() {
    try {
      ConfigResource groupResource =
          new ConfigResource(ConfigResource.Type.GROUP, "streamsapp-group-a");
      Map<ConfigResource, Config> result =
          adminClient
              .describeConfigs(
                  Collections.singleton(
                      new ConfigResource(ConfigResource.Type.GROUP, "streamsapp-group-a")),
                  new DescribeConfigsOptions())
              .all()
              .get();
      assertTrue(result.containsKey(groupResource));
      // DEBUG
      System.out.println("###GROUPCONFIG###");
      result
          .get(groupResource)
          .entries()
          .forEach(
              entry -> {
                System.out.println(entry.name() + " : " + entry.value());
              });
      Config config = result.get(groupResource);
      assertEquals(SESSION_TIMEOUT_MS, config.get("streams.session.timeout.ms").value());
      assertEquals(HEARTBEAT_INTERVAL_MS, config.get("streams.heartbeat.interval.ms").value());
      assertEquals(NUM_STANDBY_REPLICAS, config.get("streams.num.standby.replicas").value());
      assertEquals(INITIAL_REBALANCE_MS, config.get("streams.initial.rebalance.delay.ms").value());
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void shouldFallbackToDefaultsIfNotSpecified() {
    ConfigResource groupResource =
        new ConfigResource(ConfigResource.Type.GROUP, "streams-app-group-b");
    try {
      Map<ConfigResource, Config> result =
          adminClient
              .describeConfigs(
                  Collections.singleton(
                      new ConfigResource(ConfigResource.Type.GROUP, "streamsapp-group-b")),
                  new DescribeConfigsOptions())
              .all()
              .get();
      assertTrue(result.containsKey(groupResource));
      Config config = result.get(groupResource);
      assertEquals("50000", config.get("streams.session.timeout.ms").value());
      assertEquals("5000", config.get("streams.heartbeat.interval.ms").value());
      assertEquals("6", config.get("streams.num.standby.replicas").value());
      assertEquals("3000", config.get("streams.initial.rebalance.delay.ms").value());
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
