package com.purbon.kafka.topology;

import static com.purbon.kafka.topology.CommandLineInterface.*;
import static com.purbon.kafka.topology.Constants.*;
import static com.purbon.kafka.topology.model.SubjectNameStrategy.TOPIC_NAME_STRATEGY;
import static com.purbon.kafka.topology.model.SubjectNameStrategy.TOPIC_RECORD_NAME_STRATEGY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.purbon.kafka.topology.exceptions.TopologyParsingException;
import com.purbon.kafka.topology.model.*;
import com.purbon.kafka.topology.model.Impl.ProjectImpl;
import com.purbon.kafka.topology.model.Impl.TopologyImpl;
import com.purbon.kafka.topology.model.artefact.KafkaConnectArtefact;
import com.purbon.kafka.topology.model.artefact.KsqlArtefacts;
import com.purbon.kafka.topology.model.artefact.KsqlStreamArtefact;
import com.purbon.kafka.topology.model.artefact.KsqlTableArtefact;
import com.purbon.kafka.topology.model.users.*;
import com.purbon.kafka.topology.model.users.platform.KsqlServerInstance;
import com.purbon.kafka.topology.model.users.platform.SchemaRegistryInstance;
import com.purbon.kafka.topology.serdes.TopologySerdes;
import com.purbon.kafka.topology.serdes.TopologySerdes.FileType;
import com.purbon.kafka.topology.utils.TestUtils;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

public class TopologySerdesTest {

  private TopologySerdes parser;

  @Before
  public void setup() {
    Properties props = new Properties();
    props.put(PLATFORM_SERVERS_CONNECT + ".0", "connector0:foo");
    HashMap<String, String> cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");
    Configuration config = new Configuration(cliOps, props);
    parser = new TopologySerdes(config, new PlanMap());
  }

  @Test
  public void testMetadata() {
    Topology topology =
        parser.deserialise(TestUtils.getResourceFile("/descriptor-with-metadata.yaml"));
    Project project = topology.getProjects().getFirst();
    assertThat(project.getConsumers().getFirst().getMetadata()).containsKey("system");
    assertThat(project.getProducers().getFirst().getMetadata()).containsKey("contactInfo");
    assertThat(project.getStreams().getFirst().getMetadata()).containsKey("system");
    assertThat(project.getConnectors().getFirst().getMetadata()).containsKey("system");
    assertThat(project.getTopics().get(0).getMetadata()).containsKey("domain");
    assertThat(project.getTopics().get(1).getMetadata()).containsKey("domain");
    assertThat(project.getTopics().get(1).getMetadata()).containsKey("owner");
    assertThat(project.getTopics().get(0).getConsumers().getFirst().getMetadata())
        .containsKey("system");
  }

  @Test
  public void testStreamsParsingOnlyReadTopicsShouldNotParseAsNull() {
    Topology topology =
        parser.deserialise(TestUtils.getResourceFile("/descriptor-streams-only-read.yaml"));
    Project p = topology.getProjects().getFirst();
    for (KStream s : p.getStreams()) {
      assertThat(s.getTopics().get(KStream.READ_TOPICS)).isNotNull();
      assertThat(s.getTopics().get(KStream.READ_TOPICS)).isInstanceOf(List.class);
      assertThat(s.getTopics().get(KStream.READ_TOPICS)).contains("topicA", "topicB");
      assertThat(s.getTopics().get(KStream.WRITE_TOPICS)).isNotNull();
      assertThat(s.getTopics().get(KStream.WRITE_TOPICS)).isInstanceOf(List.class);
    }
  }

  @Test(expected = TopologyParsingException.class)
  public void testStreamsParsingOnlyWriteTopicsShoulRaiseAnException() {
    parser.deserialise(TestUtils.getResourceFile("/descriptor-streams-only-write.yaml"));
  }

  @Test
  public void testStreamsParsingWithGroupConfig() {
    Topology topology =
        parser.deserialise(TestUtils.getResourceFile("/descriptor-streams-with-groups.yaml"));
    Project project = topology.getProjects().getFirst();

    if (project.getStreams().getFirst().getGroupConfig().isEmpty()) {
      throw new TopologyParsingException("GroupConfig is not present");
    }

    GroupConfig groupConfig = project.getStreams().getFirst().getGroupConfig().get();

    String groupId = groupConfig.getGroupId().orElseThrow();
    assertEquals("GroupA", groupId);

    Long sessionTimeoutMs = groupConfig.getSessionTimeoutMs().orElseThrow();
    assertEquals(45000L, sessionTimeoutMs.longValue());

    Long heartbeatIntervalMs = groupConfig.getHeartbeatIntervalMs().orElseThrow();
    assertEquals(3000L, heartbeatIntervalMs.longValue());

    Integer numStandbyReplicas = groupConfig.getNumStandbyReplicas().orElseThrow();
    assertEquals(1, numStandbyReplicas.intValue());

    Long initialRebalanceDelayMs = groupConfig.getInitialRebalanceDelayMs().orElseThrow();
    assertEquals(0L, initialRebalanceDelayMs.longValue());
  }

  @Test
  public void testDynamicFirstLevelAttributes() {
    Topology topology =
        parser.deserialise(TestUtils.getResourceFile("/descriptor-with-others.yml"));
    Project project = topology.getProjects().getFirst();
    assertThat(project.namePrefix()).startsWith("contextOrg.source.foo.bar.zet");
    Topology anotherTopology = parser.deserialise(TestUtils.getResourceFile("/descriptor.yaml"));
    Project anotherProject = anotherTopology.getProjects().getFirst();
    assertEquals("contextOrg.source.foo.", anotherProject.namePrefix());
  }

  @Test
  public void testFileWithoutTopicsShouldNotError() {
    parser.deserialise(TestUtils.getResourceFile("/descriptor-without-topics.yml"));
  }

  @Test
  public void testTopologySerialisation() throws IOException {
    Topology topology = new TopologyImpl();
    topology.setContext("contextOrg");
    topology.setProjects(buildProjects());
    String topologyYamlString = parser.serialise(topology);
    Topology deserTopology = parser.deserialise(topologyYamlString);
    assertEquals(topology.getContext(), deserTopology.getContext());
    assertEquals(topology.getProjects().size(), deserTopology.getProjects().size());
  }

  @Test
  public void testTopicConfigSerdes() throws IOException {
    Topology topology = new TopologyImpl();
    topology.setContext("team");
    HashMap<String, String> topicConfig = new HashMap<>();
    topicConfig.put("num.partitions", "1");
    topicConfig.put("replication.factor", "1");
    Topic topic = new Topic("foo", topicConfig);
    HashMap<String, String> topicBarConfig = new HashMap<>();
    topicBarConfig.put("num.partitions", "1");
    topicBarConfig.put("replication.factor", "1");
    Topic topicBar = new Topic("bar", "avro", topicBarConfig);
    Project project = new ProjectImpl("foo");
    KStream kstreamApp = new KStream();
    kstreamApp.setPrincipal("App0");
    HashMap<String, List<String>> topics = new HashMap<>();
    topics.put(KStream.READ_TOPICS, Arrays.asList("topicA", "topicB"));
    topics.put(KStream.WRITE_TOPICS, Arrays.asList("topicC", "topicD"));
    kstreamApp.setTopics(topics);
    project.setStreams(Collections.singletonList(kstreamApp));
    Connector connector1 = new Connector();
    connector1.setPrincipal("Connect1");
    HashMap<String, List<String>> topics1 = new HashMap<>();
    topics1.put(KStream.READ_TOPICS, Arrays.asList("topicA", "topicB"));
    connector1.setTopics(topics1);
    Connector connector2 = new Connector();
    connector2.setPrincipal("Connect2");
    HashMap<String, List<String>> topics2 = new HashMap<>();
    topics2.put(KStream.WRITE_TOPICS, Arrays.asList("topicC", "topicD"));
    connector2.setTopics(topics2);
    project.setConnectors(Arrays.asList(connector1, connector2));
    Consumer consumer0 = new Consumer("app0");
    Consumer consumer1 = new Consumer("app1");
    project.setConsumers(Arrays.asList(consumer0, consumer1));
    project.setTopics(Arrays.asList(topic, topicBar));
    Project project2 = new ProjectImpl("bar");
    project2.setTopics(Arrays.asList(topicBar));
    topology.setProjects(Arrays.asList(project, project2));
    String topologyYamlString = parser.serialise(topology);
    Topology deserTopology = parser.deserialise(topologyYamlString);
    Project serdesProject = deserTopology.getProjects().getFirst();
    Topic serdesTopic = serdesProject.getTopics().getFirst();
    assertEquals(topic.getName(), serdesTopic.getName());
    assertEquals(topic.partitionsCount(), serdesTopic.partitionsCount());
  }

  @Test
  public void testTopicWithDataType() throws IOException {
    Project project = new ProjectImpl("foo");
    Topology topology = new TopologyImpl();
    topology.setContext("team");
    topology.addProject(project);
    HashMap<String, String> topicConfig = new HashMap<>();
    topicConfig.put("num.partitions", "3");
    topicConfig.put("replication.factor", "2");
    Topic topic = new Topic("foo", "json", topicConfig);
    project.addTopic(topic);
    Topic topic2 = new Topic("topic2", topicConfig);
    project.addTopic(topic2);
    String topologyYamlString = parser.serialise(topology);
    Topology deserTopology = parser.deserialise(topologyYamlString);
    Project serdesProject = deserTopology.getProjects().getFirst();
    Topic serdesTopic = serdesProject.getTopics().getFirst();
    assertEquals(topic.getDataType(), serdesTopic.getDataType());
    assertEquals(topic.getDataType().get(), serdesTopic.getDataType().get());
    Topic serdesTopic2 = serdesProject.getTopics().get(1);
    assertEquals(topic2.getDataType(), serdesTopic2.getDataType());
  }

  @Test(expected = TopologyParsingException.class)
  public void testTopologyWithNoTeam() {
    parser.deserialise(TestUtils.getResourceFile("/descriptor-with-no-context.yaml"));
  }

  @Test
  public void testTopologyWithNoProjectShouldNotError() {
    parser.deserialise(TestUtils.getResourceFile("/descriptor-with-no-project.yaml"));
  }

  @Test
  public void testCoreElementsProcessing() {
    Topology topology = parser.deserialise(TestUtils.getResourceFile("/descriptor.yaml"));
    assertThat(topology.getProjects()).hasSize(3);
    Project project = topology.getProjects().getFirst();
    assertThat(project.getProducers()).hasSize(3);
    assertThat(project.getConsumers()).hasSize(2);
    assertThat(project.getStreams()).hasSize(1);
    assertThat(project.getConnectors()).hasSize(2);
    assertThat(project.getStreams().getFirst().getObserverPrincipals()).hasSize(0);
    assertThat(project.getProducers().get(0).getIdempotence()).isEmpty();
    assertThat(project.getProducers().get(1).getTransactionId()).isEqualTo(Optional.of("1234"));
    assertThat(project.getProducers().get(2).getIdempotence()).isNotEmpty();
    List<Topic> topics = topology.getProjects().get(2).getTopics();
    assertThat(topics).hasSize(2);
    assertThat(topics.getFirst().toString()).isEqualTo("contextOrg.source.baz.topicE");
    final List<KStream> streams = topology.getProjects().get(2).getStreams();
    assertThat(streams).hasSize(1);
    assertThat(streams.getFirst().getObserverPrincipals()).hasSize(2);
    assertThat(streams.getFirst().getObserverPrincipals().get(0).getPrincipal())
        .isEqualTo("User:baz-observer1");
    assertThat(streams.getFirst().getObserverPrincipals().get(1).getPrincipal())
        .isEqualTo("User:baz-observer2");
  }

  @Test
  public void testStreamsApps() {
    Topology topology = parser.deserialise(TestUtils.getResourceFile("/descriptor.yaml"));
    Project project1 = topology.getProjects().getFirst();
    assertThat(project1.getStreams()).hasSize(1);
    assertThat(project1.getStreams()).noneMatch(s -> s.getApplicationId().isPresent());
    Project project3 = topology.getProjects().get(2);
    assertThat(project3.getStreams()).hasSize(1);
    assertThat(project3.getStreams())
        .allMatch(s -> s.getApplicationId().orElse("notFound").equals("applicationId-1"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSchemaSerdes() {
    parser.deserialise(TestUtils.getResourceFile("/descriptor-wrong-schemas.yaml"));
  }

  @Test
  public void testSchemaSerdes() {
    Topology topology = parser.deserialise(TestUtils.getResourceFile("/descriptor.yaml"));
    Project project = topology.getProjects().getFirst();
    List<Topic> topics = project.getTopics();
    Optional<Topic> topicBar = topics.stream().filter(t -> t.getName().equals("bar")).findFirst();
    Optional<Topic> topicCat = topics.stream().filter(t -> t.getName().equals("cat")).findFirst();
    assertThat(topicBar).isPresent();
    assertThat(topicBar.get().getSchemas()).hasSize(1);
    assertThat(topicBar.get().getSchemas().getFirst().getValueSubject().hasSchemaFile()).isTrue();
    assertThat(topicBar.get().getSubjectNameStrategy()).isEqualTo(TOPIC_NAME_STRATEGY);
    assertThat(topicCat).isPresent();
    assertThat(topicCat.get().getSchemas()).hasSize(2);
    assertThat(topicCat.get().getSchemas().get(0).getValueSubject().hasSchemaFile()).isTrue();
    assertThat(topicCat.get().getSchemas().get(1).getValueSubject().hasSchemaFile()).isTrue();
    assertThat(topicCat.get().getSubjectNameStrategy()).isEqualTo(TOPIC_RECORD_NAME_STRATEGY);
  }

  @Test
  public void testOtherSerdes() {
    Topology topology = parser.deserialise(TestUtils.getResourceFile("/descriptor.yaml"));
    Project project = topology.getProjects().getFirst();
    var others = project.getOthers();
    assertThat(others).hasSize(2);
    var foos = others.get("app");
    assertThat(foos.getFirst().getPrincipal()).isEqualTo("User:banana");
    assertThat(foos.getFirst().groupString()).isEqualTo("foo");
    var bars = others.get("bar");
    assertThat(bars.getFirst().getPrincipal()).isEqualTo("User:banana");
    assertThat(bars.getFirst().groupString()).isEqualTo("bar");
  }

  @Test
  public void testKsqlSerdes() {
    Topology topology = parser.deserialise(TestUtils.getResourceFile("/descriptor.yaml"));
    Project project = topology.getProjects().getFirst();
    KSqlApp kSqlApp = project.getKSqls().getFirst();
    assertThat(kSqlApp.getPrincipal()).isEqualTo("User:ksql0");
    assertThat(kSqlApp.getTopics().get("read")).hasSize(1);
    assertThat(kSqlApp.getTopics().get("write")).hasSize(1);
    KsqlArtefacts artefacts = project.getKsqlArtefacts();
    assertThat(artefacts.getStreams()).hasSize(1);
    KsqlStreamArtefact artefact = artefacts.getStreams().getFirst();
    assertThat(artefact.getName()).isEqualTo("riderLocations");
    assertThat(artefacts.getTables()).hasSize(1);
    KsqlTableArtefact tableArtefact = artefacts.getTables().getFirst();
    assertThat(tableArtefact.getName()).isEqualTo("users");
  }

  @Test
  public void testKsqlWithVarsSerdes() {
    Topology topology = parser.deserialise(TestUtils.getResourceFile("/descriptor.yaml"));
    Project project = topology.getProjects().getFirst();
    KsqlArtefacts artefacts = project.getKsqlArtefacts();
    assertThat(artefacts.getVars().getSessionVars()).hasSize(2);
    String firstVar = artefacts.getVars().getSessionVars().get("foo");
    assertThat(firstVar).isEqualTo("bar");
  }

  @Test
  public void testPlatformProcessing() {
    Topology topology = parser.deserialise(TestUtils.getResourceFile("/descriptor.yaml"));
    assertEquals("contextOrg", topology.getContext());
    List<SchemaRegistryInstance> listOfSR =
        topology.getPlatform().getSchemaRegistry().getInstances();
    assertEquals(2, listOfSR.size());
    assertEquals("User:SchemaRegistry01", listOfSR.getFirst().getPrincipal());
    assertEquals("foo", listOfSR.get(0).topicString());
    assertEquals("bar", listOfSR.get(0).groupString());
    assertEquals("User:SchemaRegistry02", listOfSR.get(1).getPrincipal());
    List<KsqlServerInstance> listOfKsql = topology.getPlatform().getKsqlServer().getInstances();
    assertEquals(2, listOfKsql.size());
    assertEquals("User:ksql", listOfKsql.getFirst().getPrincipal());
    assertEquals("ksql-server1", listOfKsql.get(0).getKsqlDbId());
    assertEquals("User:foo", listOfKsql.get(0).getOwner());
    assertEquals("User:ksql", listOfKsql.get(1).getPrincipal());
    assertEquals("ksql-server2", listOfKsql.get(1).getKsqlDbId());
    assertEquals("User:foo", listOfKsql.get(1).getOwner());
  }

  @Test
  public void testOnlyTopics() {
    Topology topology =
        parser.deserialise(TestUtils.getResourceFile("/descriptor-only-topics.yaml"));
    assertEquals("contextOrg", topology.getContext());
    assertTrue(topology.getProjects().getFirst().getConnectors().isEmpty());
    assertTrue(topology.getProjects().getFirst().getProducers().isEmpty());
    assertTrue(topology.getProjects().getFirst().getStreams().isEmpty());
  }

  @Test
  public void testOnlyConnectors() {
    Topology topology =
        parser.deserialise(TestUtils.getResourceFile("/descriptor-connector-alone.yaml"));
    var project = topology.getProjects().getFirst();
    assertEquals("context", topology.getContext());
    assertTrue(project.getConnectors().isEmpty());
  }

  @Test
  public void testConnectorArtefactsRetrieval() {
    Topology topology = parser.deserialise(TestUtils.getResourceFile("/descriptor-connector.yaml"));
    Project project = topology.getProjects().getFirst();
    List<KafkaConnectArtefact> artefacts = project.getConnectorArtefacts().getConnectors();
    assertEquals(2, artefacts.size());
    assertThat(artefacts.get(0)).hasFieldOrPropertyWithValue("path", "connectors/source-jdbc.json");
    assertThat(artefacts.get(0)).hasFieldOrPropertyWithValue("serverLabel", "connector0");
    assertThat(artefacts.get(1)).hasFieldOrPropertyWithValue("path", "connectors/sink-jdbc.json");
    assertThat(artefacts.get(1)).hasFieldOrPropertyWithValue("serverLabel", "connector0");
  }

  @Test
  public void testBackwardsCompatibleDescriptorForConnectors() {
    Topology topology =
        parser.deserialise(TestUtils.getResourceFile("/backwards-comp-descriptor.yaml"));
    Project fooProject = topology.getProjects().getFirst();
    assertEquals("foo", fooProject.getName());
    assertEquals(2, fooProject.getConnectors().size());
    assertEquals("User:Connect1", fooProject.getConnectors().get(0).getPrincipal());
    assertEquals("User:Connect2", fooProject.getConnectors().get(1).getPrincipal());
  }

  @Test
  public void testTopicNameWithCustomSeparator() {
    Map<String, String> cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");
    cliOps.put(CLIENT_CONFIG_OPTION, "/fooBar");
    Properties props = new Properties();
    props.put(TOPIC_PREFIX_SEPARATOR_CONFIG, "_");
    Configuration config = new Configuration(cliOps, props);
    TopologySerdes parser = new TopologySerdes(config, new PlanMap());
    Topology topology =
        parser.deserialise(TestUtils.getResourceFile("/descriptor-only-topics.yaml"));
    assertEquals("contextOrg", topology.getContext());
    Project p = topology.getProjects().getFirst();
    assertEquals(2, p.getTopics().size());
    assertEquals("contextOrg_source_foo_foo", p.getTopics().get(0).toString());
    assertEquals("contextOrg_source_foo_bar_avro", p.getTopics().get(1).toString());
  }

  @Test
  public void testTopicNameWithCustomPattern() {
    Map<String, String> cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");
    cliOps.put(CLIENT_CONFIG_OPTION, "/fooBar");
    Properties props = new Properties();
    props.put(TOPIC_PREFIX_FORMAT_CONFIG, "{{source}}.{{context}}.{{project}}.{{topic}}");
    Configuration config = new Configuration(cliOps, props);
    TopologySerdes parser = new TopologySerdes(config, new PlanMap());
    Topology topology =
        parser.deserialise(TestUtils.getResourceFile("/descriptor-only-topics.yaml"));
    assertEquals("contextOrg", topology.getContext());
    Project p = topology.getProjects().getFirst();
    assertEquals(2, p.getTopics().size());
    assertEquals("source.contextOrg.foo.foo", p.getTopics().get(0).toString());
    assertEquals("source.contextOrg.foo.bar", p.getTopics().get(1).toString());
  }

  @Test
  public void testTopologyWithRepeatedAndMissingDataType() {
    Map<String, String> cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");
    cliOps.put(CLIENT_CONFIG_OPTION, "/fooBar");
    Properties props = new Properties();
    props.put(
        TOPIC_PREFIX_FORMAT_CONFIG,
        "{{context}}.{{project}}.{{topic}}{% if dataType is defined %}.{{dataType}}{% endif %}");
    Configuration config = new Configuration(cliOps, props);
    TopologySerdes parser = new TopologySerdes(config, new PlanMap());
    Topology topology =
        parser.deserialise(TestUtils.getResourceFile("/descriptor-only-topics-datatype.yaml"));
    assertEquals("context", topology.getContext());
    Project p = topology.getProjects().getFirst();
    assertEquals(2, p.getTopics().size());
    assertEquals("context.foo.foo", p.getTopics().get(0).toString());
    assertEquals("context.foo.bar.avro", p.getTopics().get(1).toString());
  }

  @Test
  public void testTopicsWithDLQNoFilter() {
    Map<String, String> cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");
    cliOps.put(CLIENT_CONFIG_OPTION, "/fooBar");
    Properties props = new Properties();
    props.put(TOPOLOGY_DLQ_TOPICS_GENERATE, "true");
    Configuration config = new Configuration(cliOps, props);
    TopologySerdes parser = new TopologySerdes(config, new PlanMap());
    Topology topology =
        parser.deserialise(TestUtils.getResourceFile("/descriptor-only-topics.yaml"));
    Project p = topology.getProjects().getFirst();
    assertThat(p.getTopics()).hasSize(4);
    assertEquals("contextOrg.source.foo.foo", p.getTopics().get(0).toString());
    assertEquals("contextOrg.source.foo.foo.dlq", p.getTopics().get(1).toString());
    assertEquals("contextOrg.source.foo.bar.avro", p.getTopics().get(2).toString());
    assertEquals("contextOrg.source.foo.bar.avro.dlq", p.getTopics().get(3).toString());
  }

  @Test
  public void testTopicsWithDLQWithDenyList() {
    Map<String, String> cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");
    cliOps.put(CLIENT_CONFIG_OPTION, "/fooBar");
    Properties props = new Properties();
    props.put(TOPOLOGY_DLQ_TOPICS_GENERATE, "true");
    props.put(TOPOLOGY_DQL_TOPICS_DENY_LIST + ".0", "contextOrg.source.foo.bar.avro");
    Configuration config = new Configuration(cliOps, props);
    TopologySerdes parser = new TopologySerdes(config, new PlanMap());
    Topology topology =
        parser.deserialise(TestUtils.getResourceFile("/descriptor-only-topics.yaml"));
    Project p = topology.getProjects().getFirst();
    assertThat(p.getTopics()).hasSize(3);
    assertEquals("contextOrg.source.foo.foo", p.getTopics().get(0).toString());
    assertEquals("contextOrg.source.foo.foo.dlq", p.getTopics().get(1).toString());
    assertEquals("contextOrg.source.foo.bar.avro", p.getTopics().get(2).toString());
  }

  @Test
  public void testTopicsWithDLQWithDenyListAndPattern() {
    Map<String, String> cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");
    cliOps.put(CLIENT_CONFIG_OPTION, "/fooBar");
    Properties props = new Properties();
    props.put(TOPOLOGY_DLQ_TOPICS_GENERATE, "true");
    props.put(TOPOLOGY_DQL_TOPICS_DENY_LIST + ".0", "^.*source.foo.foo$");
    Configuration config = new Configuration(cliOps, props);
    TopologySerdes parser = new TopologySerdes(config, new PlanMap());
    Topology topology =
        parser.deserialise(TestUtils.getResourceFile("/descriptor-only-topics.yaml"));
    Project p = topology.getProjects().getFirst();
    assertThat(p.getTopics()).hasSize(3);
    assertEquals("contextOrg.source.foo.foo", p.getTopics().get(0).toString());
    assertEquals("contextOrg.source.foo.bar.avro", p.getTopics().get(1).toString());
    assertEquals("contextOrg.source.foo.bar.avro.dlq", p.getTopics().get(2).toString());
  }

  @Test
  public void testTopicsWithDLQWithDenyListAndPatternFullDeny() {
    Map<String, String> cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");
    cliOps.put(CLIENT_CONFIG_OPTION, "/fooBar");
    Properties props = new Properties();
    props.put(TOPOLOGY_DLQ_TOPICS_GENERATE, "true");
    props.put(TOPOLOGY_DQL_TOPICS_DENY_LIST + ".0", "^.*source.foo.*$");
    Configuration config = new Configuration(cliOps, props);
    TopologySerdes parser = new TopologySerdes(config, new PlanMap());
    Topology topology =
        parser.deserialise(TestUtils.getResourceFile("/descriptor-only-topics.yaml"));
    Project p = topology.getProjects().getFirst();
    assertThat(p.getTopics()).hasSize(2);
    assertEquals("contextOrg.source.foo.foo", p.getTopics().get(0).toString());
    assertEquals("contextOrg.source.foo.bar.avro", p.getTopics().get(1).toString());
  }

  @Test
  public void testTopicsWithDLQWithTopicPattern() {
    Map<String, String> cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");
    cliOps.put(CLIENT_CONFIG_OPTION, "/fooBar");
    Properties props = new Properties();
    props.put(DLQ_TOPIC_PREFIX_FORMAT_CONFIG, "{{dlq}}.{{context}}.{{project}}");
    props.put(TOPOLOGY_DLQ_TOPICS_GENERATE, "true");
    props.put(TOPOLOGY_DQL_TOPICS_ALLOW_LIST + ".0", "contextOrg.source.foo.bar.avro");
    Configuration config = new Configuration(cliOps, props);
    TopologySerdes parser = new TopologySerdes(config, new PlanMap());
    Topology topology =
        parser.deserialise(TestUtils.getResourceFile("/descriptor-only-topics.yaml"));
    Project p = topology.getProjects().getFirst();
    assertThat(p.getTopics()).hasSize(3);
    assertEquals("contextOrg.source.foo.foo", p.getTopics().get(0).toString());
    assertEquals("contextOrg.source.foo.bar.avro", p.getTopics().get(1).toString());
    assertEquals("dlq.contextOrg.foo", p.getTopics().get(2).toString());
  }

  @Test
  public void testTopicsWithDLQConfigVerify() {
    Map<String, String> cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");
    cliOps.put(CLIENT_CONFIG_OPTION, "/fooBar");
    Properties props = new Properties();
    props.put(TOPOLOGY_DLQ_TOPICS_GENERATE, "true");
    props.put(TOPOLOGY_DQL_TOPICS_ALLOW_LIST + ".0", "contextOrg.source.foo.bar.avro");
    Configuration config = new Configuration(cliOps, props);
    TopologySerdes parser = new TopologySerdes(config, new PlanMap());
    Topology topology =
        parser.deserialise(TestUtils.getResourceFile("/descriptor-only-topics.yaml"));
    Project p = topology.getProjects().getFirst();
    assertThat(p.getTopics()).hasSize(3);
    Topic mainTopic = p.getTopics().get(1);
    Topic dlqTopic = p.getTopics().get(2);
    assertEquals("contextOrg.source.foo.bar.avro", mainTopic.toString());
    assertEquals("contextOrg.source.foo.bar.avro.dlq", dlqTopic.toString());
    assertThat(mainTopic.getConfig()).usingRecursiveComparison().isEqualTo(dlqTopic.getConfig());
  }

  @Test
  public void testTopicsWithDLQWithAllowList() {
    Map<String, String> cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");
    cliOps.put(CLIENT_CONFIG_OPTION, "/fooBar");
    Properties props = new Properties();
    props.put(TOPOLOGY_DLQ_TOPICS_GENERATE, "true");
    props.put(TOPOLOGY_DQL_TOPICS_ALLOW_LIST + ".0", "contextOrg.source.foo.bar.avro");
    Configuration config = new Configuration(cliOps, props);
    TopologySerdes parser = new TopologySerdes(config, new PlanMap());
    Topology topology =
        parser.deserialise(TestUtils.getResourceFile("/descriptor-only-topics.yaml"));
    Project p = topology.getProjects().getFirst();
    assertThat(p.getTopics()).hasSize(3);
    assertEquals("contextOrg.source.foo.foo", p.getTopics().get(0).toString());
    assertEquals("contextOrg.source.foo.bar.avro", p.getTopics().get(1).toString());
    assertEquals("contextOrg.source.foo.bar.avro.dlq", p.getTopics().get(2).toString());
  }

  @Test
  public void testTopicsWithDLQWithAllowAndDenyList() {
    Map<String, String> cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");
    cliOps.put(CLIENT_CONFIG_OPTION, "/fooBar");
    Properties props = new Properties();
    props.put(TOPOLOGY_DLQ_TOPICS_GENERATE, "true");
    props.put(TOPOLOGY_DQL_TOPICS_ALLOW_LIST + ".0", "contextOrg.source.foo.bar.avro");
    props.put(TOPOLOGY_DQL_TOPICS_DENY_LIST + ".0", "contextOrg.source.bar.bar.avro");
    Configuration config = new Configuration(cliOps, props);
    TopologySerdes parser = new TopologySerdes(config, new PlanMap());
    Topology topology =
        parser.deserialise(TestUtils.getResourceFile("/descriptor-only-topics.yaml"));
    Project p = topology.getProjects().getFirst();
    assertThat(p.getTopics()).hasSize(3);
    assertEquals("contextOrg.source.foo.foo", p.getTopics().get(0).toString());
    assertEquals("contextOrg.source.foo.bar.avro", p.getTopics().get(1).toString());
    assertEquals("contextOrg.source.foo.bar.avro.dlq", p.getTopics().get(2).toString());
    p = topology.getProjects().get(1);
    assertThat(p.getTopics()).hasSize(1);
    assertEquals("contextOrg.source.bar.bar.avro", p.getTopics().getFirst().toString());
  }

  @Test(expected = TopologyParsingException.class)
  public void testTopicNameWithUTFCharacters() {
    parser.deserialise(TestUtils.getResourceFile("/descriptor-only-topics-utf.yaml"));
  }

  @Test
  public void testJsonDescriptorFileSerdes() {
    TopologySerdes parser = new TopologySerdes(new Configuration(), FileType.JSON, new PlanMap());
    Topology topology = parser.deserialise(TestUtils.getResourceFile("/descriptor.json"));
    assertEquals(1, topology.getProjects().size());
    assertEquals("foo", topology.getProjects().getFirst().getName());
    assertEquals(2, topology.getProjects().getFirst().getTopics().size());
  }

  @Test
  public void shouldParseSpecialTopics() {
    Topology topology = parser.deserialise(TestUtils.getResourceFile("/descriptor.yaml"));
    var topics = topology.getSpecialTopics();
    assertThat(topics).hasSize(2);
    var topicNames = topics.stream().map(Topic::getName).collect(Collectors.toList());
    assertThat(topicNames).contains("foo");
    assertThat(topicNames).contains("bar");
    Properties props = new Properties();
    props.put(PROJECT_PREFIX_FORMAT_CONFIG, "{{project}}");
    props.put(TOPIC_PREFIX_FORMAT_CONFIG, "{{project}}.{{topic}}");
    HashMap<String, String> cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");
    Configuration config = new Configuration(cliOps, props);
    TopologySerdes parser = new TopologySerdes(config, FileType.YAML, new PlanMap());
    topology = parser.deserialise(TestUtils.getResourceFile("/descriptor.yaml"));
    topics = topology.getSpecialTopics();
    assertThat(topics).hasSize(2);
    var topicFullNames = topics.stream().map(Topic::toString).collect(Collectors.toList());
    assertThat(topicFullNames).contains("foo");
    assertThat(topicFullNames).contains("bar");
  }

  private List<Project> buildProjects() {
    Project project = new ProjectImpl("project");
    project.setConsumers(buildConsumers());
    project.setProducers(buildProducers());
    project.setStreams(buildStreams());
    return Collections.singletonList(project);
  }

  private List<KStream> buildStreams() {
    List<KStream> streams = new ArrayList<>();
    HashMap<String, List<String>> topics = new HashMap<>();
    topics.put("read", Arrays.asList("topic1", "topic3"));
    topics.put("write", Arrays.asList("topic2", "topic4"));
    streams.add(new KStream("app3", topics, Collections.emptyList()));
    topics = new HashMap<>();
    topics.put("read", Arrays.asList("topic2", "topic4"));
    topics.put("write", Arrays.asList("topic5"));
    streams.add(new KStream("app4", topics, Collections.emptyList()));
    return streams;
  }

  private List<Producer> buildProducers() {
    List<Producer> producers = new ArrayList<>();
    producers.add(new Producer("app1"));
    producers.add(new Producer("app3"));
    return producers;
  }

  private List<Consumer> buildConsumers() {
    List<Consumer> consumers = new ArrayList<>();
    consumers.add(new Consumer("app1"));
    consumers.add(new Consumer("app2"));
    return consumers;
  }
}
