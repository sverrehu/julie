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
          new ConfigResource(ConfigResource.Type.GROUP, "streams-app-a");
      Map<ConfigResource, Config> result =
          adminClient
              .describeConfigs(
                  Collections.singleton(
                      new ConfigResource(ConfigResource.Type.GROUP, "streams-app-a")),
                  new DescribeConfigsOptions())
              .all()
              .get();
      assertTrue(result.containsKey(groupResource));
      Config config = result.get(groupResource);
      assertEquals("60000", config.get("streams.session.timeout.ms").value());
      assertEquals("6000", config.get("streams.heartbeat.interval.ms").value());
      assertEquals("1", config.get("streams.num.standby.replicas").value());
      assertEquals("2000", config.get("streams.initial.rebalance.delay.ms").value());
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void shouldFallbackToDefaultsIfNotSpecified() {
    ConfigResource groupResource =
        new ConfigResource(ConfigResource.Type.GROUP, "streams-app-b");
    try {
      Map<ConfigResource, Config> result =
          adminClient
              .describeConfigs(
                  Collections.singleton(
                      new ConfigResource(ConfigResource.Type.GROUP, "streams-app-b")),
                  new DescribeConfigsOptions())
              .all()
              .get();
      assertTrue(result.containsKey(groupResource));
      Config config = result.get(groupResource);
      assertEquals("50000", config.get("streams.session.timeout.ms").value());
      assertEquals("5000", config.get("streams.heartbeat.interval.ms").value());
      assertEquals("2", config.get("streams.num.standby.replicas").value());
      assertEquals("3000", config.get("streams.initial.rebalance.delay.ms").value());
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
