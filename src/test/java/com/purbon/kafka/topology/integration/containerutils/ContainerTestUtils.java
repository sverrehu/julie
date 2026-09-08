package com.purbon.kafka.topology.integration.containerutils;

import com.purbon.kafka.topology.AccessControlProvider;
import com.purbon.kafka.topology.BindingsBuilderProvider;
import com.purbon.kafka.topology.Configuration;
import com.purbon.kafka.topology.JulieOps;
import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import com.purbon.kafka.topology.roles.SimpleAclsProvider;
import com.purbon.kafka.topology.roles.acls.AclsBindingsBuilder;
import com.purbon.kafka.topology.utils.TestUtils;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.*;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

public final class ContainerTestUtils {

  public static final String DEFAULT_SUPER_USERNAME = "kafka";
  public static final String DEFAULT_SUPER_PASSWORD = "kafka";
  public static final String JULIE_USERNAME = "julie";
  public static final String JULIE_PASSWORD = "julie-secret";
  public static final String NO_ACCESS_USERNAME = "no-access-user";
  public static final String PRODUCER_USERNAME = "producer";
  public static final String CONSUMER_USERNAME = "consumer";
  public static final String BACKUP_USERNAME = "backup-user";
  public static final String OTHER_PRODUCER_USERNAME = "other-producer";
  public static final String OTHER_CONSUMER_USERNAME = "other-consumer";
  public static final String STREAMS_USERNAME = "streamsapp";
  public static final int NUM_JULIE_INITIAL_ACLS = 12;
  static final String DEFAULT_CP_KAFKA_VERSION = "8.3.1";
  static final String DEFAULT_KSQLDB_SERVER_VERSION = "0.29.0";
  public static final String REDIS_VERSION = "8.2.3";

  private ContainerTestUtils() {}

  public static AdminClient getSaslJulieAdminClient(final AlternativeKafkaContainer container) {
    return getSaslJulieAdminClient(container.getBootstrapServers());
  }

  public static AdminClient getSaslJulieAdminClient(final String boostrapServers) {
    return AdminClient.create(getSaslConfig(boostrapServers, JULIE_USERNAME, JULIE_PASSWORD));
  }

  public static AdminClient getSaslSuperUserAdminClient(final String boostrapServers) {
    return AdminClient.create(
        getSaslConfig(boostrapServers, DEFAULT_SUPER_USERNAME, DEFAULT_SUPER_PASSWORD));
  }

  public static Map<String, Object> getSaslConfig(
      final String bootstrapServers, final String username, final String password) {
    final Map<String, Object> map = getBaseConfig(bootstrapServers);
    map.put("security.protocol", "SASL_PLAINTEXT");
    map.put("sasl.mechanism", "PLAIN");
    map.put(
        "sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required username="
            + escape(username)
            + " password="
            + escape(password)
            + ";");
    return map;
  }

  private static Map<String, Object> getBaseConfig(final String bootstrapServers) {
    final Map<String, Object> map = new HashMap<>();
    map.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    return map;
  }

  private static String escape(final String s) {
    return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }

  public static void populateAcls(
      final SaslPlaintextKafkaContainer container,
      final String topologyResource,
      final String configResource) {
    TestUtils.deleteStateFile();
    try (final AdminClient kafkaAdminClient = getSaslJulieAdminClient(container)) {
      final JulieOps julieOps =
          getKafkaTopologyBuilder(kafkaAdminClient, topologyResource, configResource);
      try {
        julieOps.run();
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    } finally {
      /* Clean up static stuff buried deep below TopologyBuilderConfig.
       * I don't like this "hack", but I didn't immediately see
       * how to do it correctly. */
      System.clearProperty("config.file");
      ConfigFactory.invalidateCaches();
    }
  }

  private static JulieOps getKafkaTopologyBuilder(
      final AdminClient kafkaAdminClient,
      final String topologyResource,
      final String configResource) {
    final String fileOrDirPath = TestUtils.getResourceFilename(topologyResource);
    final Map<String, String> cliParams = new HashMap<>();
    final Configuration builderConfig =
        Configuration.build(cliParams, TestUtils.getResourceFilename(configResource));
    final TopologyBuilderAdminClient topologyAdminClient =
        new TopologyBuilderAdminClient(kafkaAdminClient);
    final AccessControlProvider accessControlProvider = new SimpleAclsProvider(topologyAdminClient);
    final BindingsBuilderProvider bindingsBuilderProvider = new AclsBindingsBuilder(builderConfig);
    try {
      return JulieOps.build(
          fileOrDirPath,
          builderConfig,
          topologyAdminClient,
          accessControlProvider,
          bindingsBuilderProvider);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void clearAclsAndTopics(AlternativeKafkaContainer container) {
    AdminClient admin = getSaslSuperUserAdminClient(container.getBootstrapServers());
    clearAllTopics(admin);
    clearAllAcls(admin);
    setupJulieAcls(admin);
  }

  private static void clearAllAcls(AdminClient admin) {
    try {
      admin.deleteAcls(Collections.singleton(AclBindingFilter.ANY)).all().get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void clearAllTopics(AdminClient admin) {
    try {
      admin
          .deleteTopics(admin.listTopics(new ListTopicsOptions().listInternal(true)).names().get())
          .all()
          .get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void setupJulieAcls(AdminClient admin) {
    List<AclBinding> bindings = new ArrayList<>();
    /* Generic permissions to let JulieOps manage topics and ACLs. */
    bindings.addAll(
        getJulieBindings(
            ResourceType.CLUSTER,
            "kafka-cluster",
            AclOperation.ALTER,
            AclOperation.ALTER_CONFIGS,
            AclOperation.DESCRIBE,
            AclOperation.DESCRIBE_CONFIGS));
    bindings.addAll(
        getJulieBindings(
            ResourceType.TOPIC,
            "*",
            AclOperation.CREATE,
            AclOperation.ALTER,
            AclOperation.ALTER_CONFIGS,
            AclOperation.DELETE));
    bindings.addAll(
        getJulieBindings(
            ResourceType.GROUP,
            "*",
            AclOperation.READ,
            AclOperation.ALTER,
            AclOperation.ALTER_CONFIGS,
            AclOperation.DELETE));
    /* Specific permissions for storing cluster state in a topic, needed in some tests. */
    bindings.addAll(
        getJulieBindings(
            ResourceType.TOPIC, "__julieops_state", AclOperation.WRITE, AclOperation.READ));
    bindings.addAll(getJulieBindings(ResourceType.GROUP, "julieops", AclOperation.READ));
    try {
      admin.createAcls(bindings).all().get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static List<AclBinding> getJulieBindings(
      ResourceType resourceType, String resourceName, AclOperation... ops) {
    List<AclBinding> bindings = new ArrayList<>();
    for (AclOperation op : ops) {
      ResourcePattern resourcePattern =
          new ResourcePattern(resourceType, resourceName, PatternType.LITERAL);
      AccessControlEntry accessControlEntry =
          new AccessControlEntry("User:" + JULIE_USERNAME, "*", op, AclPermissionType.ALLOW);
      bindings.add(new AclBinding(resourcePattern, accessControlEntry));
    }
    return bindings;
  }
}
