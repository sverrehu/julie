package com.purbon.kafka.topology.roles.acls;

import static java.util.Arrays.asList;

import com.purbon.kafka.topology.BindingsBuilderProvider;
import com.purbon.kafka.topology.Configuration;
import com.purbon.kafka.topology.api.adminclient.AclBuilder;
import com.purbon.kafka.topology.model.DynamicUser;
import com.purbon.kafka.topology.model.JulieRoleAcl;
import com.purbon.kafka.topology.model.User;
import com.purbon.kafka.topology.model.users.Connector;
import com.purbon.kafka.topology.model.users.Consumer;
import com.purbon.kafka.topology.model.users.KSqlApp;
import com.purbon.kafka.topology.model.users.KStream;
import com.purbon.kafka.topology.model.users.Other;
import com.purbon.kafka.topology.model.users.Producer;
import com.purbon.kafka.topology.model.users.platform.KsqlServerInstance;
import com.purbon.kafka.topology.model.users.platform.SchemaRegistryInstance;
import com.purbon.kafka.topology.roles.TopologyAclBinding;
import com.purbon.kafka.topology.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

public class AclsBindingsBuilder implements BindingsBuilderProvider {

  private static final String KAFKA_CLUSTER_NAME = "kafka-cluster";

  private final Configuration config;

  public AclsBindingsBuilder(Configuration config) {
    this.config = config;
  }

  @Override
  public List<TopologyAclBinding> buildBindingsForConnect(
      Connector connector, String topicPrefixNotInUse) {
    assertNoObserverPrincipals(connector);

    String principal = connector.getPrincipal();
    Stream<String> readTopics = Utils.asNullableStream(connector.getTopics().get("read"));
    Stream<String> writeTopics = Utils.asNullableStream(connector.getTopics().get("write"));

    List<AclBinding> acls = new ArrayList<>();

    List<String> topics =
        asList(
            connector.statusTopicString(),
            connector.offsetTopicString(),
            connector.configsTopicString());

    for (String topic : topics) {
      acls.add(buildLiteralTopicLevelAcl(principal, topic, AclOperation.READ));
      acls.add(buildLiteralTopicLevelAcl(principal, topic, AclOperation.WRITE));
    }

    if (config.enabledConnectorTopicCreateAcl()) {
      ResourcePattern resourcePattern =
          new ResourcePattern(ResourceType.CLUSTER, KAFKA_CLUSTER_NAME, PatternType.LITERAL);
      AccessControlEntry entry =
          new AccessControlEntry(principal, "*", AclOperation.CREATE, AclPermissionType.ALLOW);
      acls.add(new AclBinding(resourcePattern, entry));
    }

    ResourcePattern resourcePattern =
        new ResourcePattern(ResourceType.GROUP, connector.groupString(), PatternType.LITERAL);
    AccessControlEntry entry =
        new AccessControlEntry(principal, "*", AclOperation.READ, AclPermissionType.ALLOW);
    acls.add(new AclBinding(resourcePattern, entry));

    readTopics
        .map(topic -> buildLiteralTopicLevelAcl(principal, topic, AclOperation.READ))
        .forEach(acls::add);

    writeTopics
        .map(topic -> buildLiteralTopicLevelAcl(principal, topic, AclOperation.WRITE))
        .forEach(acls::add);

    return toList(acls.stream());
  }

  @Override
  public List<TopologyAclBinding> buildBindingsForKStream(KStream stream, String topicPrefix) {
    String principal = stream.getPrincipal();
    List<User> observerPrincipals = stream.getObserverPrincipals();
    List<String> readTopics = stream.getTopics().get(KStream.READ_TOPICS);
    List<String> writeTopics = stream.getTopics().get(KStream.WRITE_TOPICS);
    boolean eos = stream.getExactlyOnce().orElseThrow();
    return toList(
        streamsAppStream(principal, observerPrincipals, topicPrefix, readTopics, writeTopics, eos));
  }

  @Override
  public List<TopologyAclBinding> buildLiteralBindingsForConsumers(
      final Collection<Consumer> consumers, final String resource) {
    return buildBindingsForConsumers(consumers, resource, false);
  }

  @Override
  public List<TopologyAclBinding> buildPrefixedBindingsForConsumers(
      final Collection<Consumer> consumers, final String resource) {
    return buildBindingsForConsumers(consumers, resource, true);
  }

  @Override
  public List<TopologyAclBinding> buildLiteralBindingsForProducers(
      final Collection<Producer> principals, final String resource) {
    return buildBindingsForProducers(principals, resource, false);
  }

  @Override
  public List<TopologyAclBinding> buildPrefixedBindingsForProducers(
      final Collection<Producer> principals, final String resource) {
    return buildBindingsForProducers(principals, resource, true);
  }

  private List<TopologyAclBinding> buildBindingsForConsumers(
      Collection<Consumer> consumers, String resource, boolean prefixed) {
    return toList(
        consumers.stream().flatMap(consumer -> consumerAclsStream(consumer, resource, prefixed)));
  }

  private List<TopologyAclBinding> buildBindingsForProducers(
      Collection<Producer> producers, String resource, boolean prefixed) {
    return toList(
        producers.stream().flatMap(producer -> producerAclsStream(producer, resource, prefixed)));
  }

  @Override
  public List<TopologyAclBinding> buildBindingsForSchemaRegistry(
      SchemaRegistryInstance schemaRegistry) {
    return toList(schemaRegistryAclsStream(schemaRegistry));
  }

  @Override
  public List<TopologyAclBinding> buildBindingsForControlCenter(String principal, String appId) {
    return toList(controlCenterStream(principal, appId));
  }

  @Override
  public Collection<TopologyAclBinding> buildBindingsForKSqlServer(KsqlServerInstance ksqlServer) {
    return toList(ksqlServerStream(ksqlServer));
  }

  @Override
  public Collection<TopologyAclBinding> buildBindingsForKSqlApp(KSqlApp app, String prefix) {
    assertNoObserverPrincipals(app);
    return toList(ksqlAppStream(app, prefix));
  }

  @Override
  public Collection<TopologyAclBinding> buildBindingsForJulieRole(
      Other other, String name, List<JulieRoleAcl> acls) throws IOException {

    List<TopologyAclBinding> bindings = new ArrayList<>();
    for (JulieRoleAcl acl : acls) {
      var resourceType = ResourceType.fromString(acl.getResourceType());
      var patternType = PatternType.fromString(acl.getPatternType());
      var aclOperation = AclOperation.fromString(acl.getOperation());
      if (resourceType.isUnknown() || patternType.isUnknown() || aclOperation.isUnknown()) {
        throw new IOException(
            "Unknown ACL setting being used resourceType="
                + acl.getResourceType()
                + " ("
                + resourceType
                + ")"
                + ", patternType="
                + acl.getPatternType()
                + " ("
                + patternType
                + ")"
                + ", aclOperation="
                + acl.getOperation()
                + " ("
                + aclOperation
                + ")");
      }
      var binding =
          new AclBuilder(other.getPrincipal())
              .resource(resourceType, acl.getResourceName(), patternType)
              .allow(acl.getHost(), aclOperation)
              .build();
      bindings.add(new TopologyAclBinding(binding));
    }
    return bindings;
  }

  private List<TopologyAclBinding> toList(Stream<AclBinding> bindingStream) {
    return bindingStream.map(TopologyAclBinding::new).collect(Collectors.toList());
  }

  private Stream<AclBinding> producerAclsStream(Producer producer, String topic, boolean prefixed) {
    PatternType patternType = prefixed ? PatternType.PREFIXED : PatternType.LITERAL;

    List<AclBinding> bindings = new ArrayList<>();
    String principal = producer.getPrincipal();
    Stream.of(AclOperation.DESCRIBE, AclOperation.WRITE)
        .map(aclOperation -> buildTopicLevelAcl(principal, topic, patternType, aclOperation))
        .forEach(bindings::add);

    producer
        .getTransactionId()
        .ifPresent(
            transactionId -> {
              bindings.add(
                  buildTransactionIdLevelAcl(
                      producer.getPrincipal(),
                      evaluateResourcePattern(transactionId),
                      evaluateResourcePatternType(transactionId),
                      AclOperation.DESCRIBE));
              bindings.add(
                  buildTransactionIdLevelAcl(
                      producer.getPrincipal(),
                      evaluateResourcePattern(transactionId),
                      evaluateResourcePatternType(transactionId),
                      AclOperation.WRITE));
            });

    if (producer.getTransactionId().isPresent() || producer.getIdempotence().isPresent()) {
      ResourcePattern resourcePattern =
          new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL);
      AccessControlEntry entry =
          new AccessControlEntry(
              producer.getPrincipal(), "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW);
      bindings.add(new AclBinding(resourcePattern, entry));
    }

    return bindings.stream();
  }

  private Stream<AclBinding> consumerAclsStream(Consumer consumer, String topic, boolean prefixed) {
    PatternType patternType = prefixed ? PatternType.PREFIXED : PatternType.LITERAL;
    String principal = consumer.getPrincipal();
    return Stream.of(
        buildTopicLevelAcl(principal, topic, patternType, AclOperation.DESCRIBE),
        buildTopicLevelAcl(principal, topic, patternType, AclOperation.READ),
        buildGroupLevelAcl(
            principal,
            evaluateResourcePattern(consumer.groupString()),
            evaluateResourcePatternType(consumer.groupString()),
            AclOperation.READ));
  }

  private Stream<AclBinding> streamsAppStream(
      String principal,
      List<User> observerPrincipals,
      String prefix,
      List<String> readTopics,
      List<String> writeTopics,
      boolean eos) {

    List<AclBinding> acls = new ArrayList<>();

    readTopics.forEach(
        topic -> acls.add(buildLiteralTopicLevelAcl(principal, topic, AclOperation.READ)));

    writeTopics.forEach(
        topic -> acls.add(buildLiteralTopicLevelAcl(principal, topic, AclOperation.WRITE)));

    acls.add(buildPrefixedTopicLevelAcl(principal, prefix, AclOperation.ALL));
    acls.add(buildPrefixedGroupLevelAcl(principal, prefix, AclOperation.READ));

    if (observerPrincipals != null) {
      observerPrincipals.forEach(
          observerPrincipal ->
              acls.add(
                  buildPrefixedTopicLevelAcl(
                      observerPrincipal.getPrincipal(), prefix, AclOperation.READ)));
      observerPrincipals.forEach(
          observerPrincipal ->
              acls.add(
                  buildPrefixedTopicLevelAcl(
                      observerPrincipal.getPrincipal(), prefix, AclOperation.DESCRIBE)));
      observerPrincipals.forEach(
          observerPrincipal ->
              acls.add(
                  buildPrefixedGroupLevelAcl(
                      observerPrincipal.getPrincipal(), prefix, AclOperation.READ)));
    }

    if (eos) {
      acls.add(buildPrefixedTransactionIdLevelAcl(principal, prefix, AclOperation.WRITE));
      acls.add(buildPrefixedTransactionIdLevelAcl(principal, prefix, AclOperation.DESCRIBE));
      if (observerPrincipals != null) {
        observerPrincipals.forEach(
            observerPrincipal ->
                acls.add(
                    buildPrefixedTransactionIdLevelAcl(
                        observerPrincipal.getPrincipal(), prefix, AclOperation.READ)));
        observerPrincipals.forEach(
            observerPrincipal ->
                acls.add(
                    buildPrefixedTransactionIdLevelAcl(
                        observerPrincipal.getPrincipal(), prefix, AclOperation.DESCRIBE)));
      }
    }

    return acls.stream();
  }

  private Stream<AclBinding> schemaRegistryAclsStream(SchemaRegistryInstance schemaRegistry) {
    String principal = schemaRegistry.getPrincipal();
    List<AclBinding> bindings =
        Stream.of(
                AclOperation.CREATE,
                AclOperation.DESCRIBE_CONFIGS,
                AclOperation.DESCRIBE,
                AclOperation.WRITE,
                AclOperation.READ)
            .map(
                aclOperation ->
                    buildLiteralTopicLevelAcl(
                        principal, schemaRegistry.topicString(), aclOperation))
            .collect(Collectors.toList());

    bindings.add(
        buildLiteralTopicLevelAcl(
            principal, schemaRegistry.consumerOffsetsTopicString(), AclOperation.DESCRIBE));

    bindings.add(
        buildLiteralGroupLevelAcl(principal, schemaRegistry.groupString(), AclOperation.READ));

    return bindings.stream();
  }

  private Stream<AclBinding> controlCenterStream(String principal, String appId) {
    List<AclBinding> bindings = new ArrayList<>();

    bindings.add(buildPrefixedGroupLevelAcl(principal, appId, AclOperation.READ));
    bindings.add(buildPrefixedGroupLevelAcl(principal, appId + "-command", AclOperation.READ));
    bindings.add(buildLiteralGroupLevelAcl(principal, "*", AclOperation.DESCRIBE));

    asList(
            config.getConfluentMonitoringTopic(),
            config.getConfluentCommandTopic(),
            config.getConfluentMetricsTopic())
        .forEach(
            topic ->
                Stream.of(
                        AclOperation.WRITE,
                        AclOperation.READ,
                        AclOperation.CREATE,
                        AclOperation.DESCRIBE)
                    .map(aclOperation -> buildLiteralTopicLevelAcl(principal, topic, aclOperation))
                    .forEach(bindings::add));

    Stream.of(AclOperation.WRITE, AclOperation.READ, AclOperation.CREATE, AclOperation.DESCRIBE)
        .map(
            aclOperation ->
                buildPrefixedTopicLevelAcl(principal, "_confluent-controlcenter", aclOperation))
        .forEach(bindings::add);

    bindings.add(buildLiteralTopicLevelAcl(principal, "*", AclOperation.CREATE));

    ResourcePattern resourcePattern =
        new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL);
    AccessControlEntry entry =
        new AccessControlEntry(principal, "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW);
    bindings.add(new AclBinding(resourcePattern, entry));

    entry =
        new AccessControlEntry(
            principal, "*", AclOperation.DESCRIBE_CONFIGS, AclPermissionType.ALLOW);
    bindings.add(new AclBinding(resourcePattern, entry));
    return bindings.stream();
  }

  private Stream<AclBinding> ksqlServerStream(KsqlServerInstance ksqlServer) {
    String principal = ksqlServer.getPrincipal();

    List<AclBinding> bindings = new ArrayList<>();

    ResourcePattern resourcePattern =
        new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL);
    AccessControlEntry entry =
        new AccessControlEntry(
            principal, "*", AclOperation.DESCRIBE_CONFIGS, AclPermissionType.ALLOW);
    bindings.add(new AclBinding(resourcePattern, entry));

    bindings.add(
        buildPrefixedTopicLevelAcl(principal, ksqlServer.internalTopics(), AclOperation.ALL));
    bindings.add(
        buildLiteralTopicLevelAcl(principal, ksqlServer.processingLogTopic(), AclOperation.ALL));
    bindings.add(
        buildPrefixedGroupLevelAcl(principal, ksqlServer.consumerGroupPrefix(), AclOperation.ALL));

    return bindings.stream();
  }

  private Stream<AclBinding> ksqlAppStream(KSqlApp app, String prefix) {
    String principal = app.getPrincipal();

    List<AclBinding> bindings = new ArrayList<>();

    Optional<List<String>> readTopics = Optional.ofNullable(app.getTopics().get("read"));
    readTopics.ifPresent(
        topics -> {
          for (String topic : topics) {
            bindings.add(buildLiteralTopicLevelAcl(principal, topic, AclOperation.READ));
          }
        });

    Optional<List<String>> writeTopics = Optional.ofNullable(app.getTopics().get("write"));
    writeTopics.ifPresent(
        topics -> {
          for (String topic : topics) {
            bindings.add(buildLiteralTopicLevelAcl(principal, topic, AclOperation.WRITE));
          }
        });

    bindings.add(buildPrefixedTopicLevelAcl(principal, prefix, AclOperation.ALL));
    bindings.add(buildPrefixedGroupLevelAcl(principal, prefix, AclOperation.ALL));

    return bindings.stream();
  }

  private AclBinding buildTopicLevelAcl(
      final String principal,
      final String topic,
      final PatternType patternType,
      final AclOperation aclOperation) {
    switch (patternType) {
      case LITERAL:
        return buildLiteralTopicLevelAcl(principal, topic, aclOperation);
      case PREFIXED:
        return buildPrefixedTopicLevelAcl(principal, topic, aclOperation);
      default:
        throw new RuntimeException("Unsupported pattern type: " + patternType);
    }
  }

  private AclBinding buildLiteralTopicLevelAcl(String principal, String topic, AclOperation op) {
    return new AclBuilder(principal).literalResource(ResourceType.TOPIC, topic).allow(op).build();
  }

  private AclBinding buildPrefixedTopicLevelAcl(String principal, String topic, AclOperation op) {
    return new AclBuilder(principal).prefixedResource(ResourceType.TOPIC, topic).allow(op).build();
  }

  private AclBinding buildTransactionIdLevelAcl(
      final String principal,
      final String transactionId,
      final PatternType patternType,
      final AclOperation aclOperation) {
    switch (patternType) {
      case LITERAL:
        return buildLiteralTransactionIdLevelAcl(principal, transactionId, aclOperation);
      case PREFIXED:
        return buildPrefixedTransactionIdLevelAcl(principal, transactionId, aclOperation);
      default:
        throw new RuntimeException("Unsupported pattern type: " + patternType);
    }
  }

  private AclBinding buildLiteralTransactionIdLevelAcl(
      String principal, String transactionId, AclOperation op) {
    return new AclBuilder(principal)
        .literalResource(ResourceType.TRANSACTIONAL_ID, transactionId)
        .allow(op)
        .build();
  }

  private AclBinding buildPrefixedTransactionIdLevelAcl(
      String principal, String transactionId, AclOperation op) {
    return new AclBuilder(principal)
        .prefixedResource(ResourceType.TRANSACTIONAL_ID, transactionId)
        .allow(op)
        .build();
  }

  private AclBinding buildGroupLevelAcl(
      final String principal,
      final String group,
      final PatternType patternType,
      final AclOperation aclOperation) {
    switch (patternType) {
      case LITERAL:
        return buildLiteralGroupLevelAcl(principal, group, aclOperation);
      case PREFIXED:
        return buildPrefixedGroupLevelAcl(principal, group, aclOperation);
      default:
        throw new RuntimeException("Unsupported pattern type: " + patternType);
    }
  }

  private AclBinding buildLiteralGroupLevelAcl(String principal, String group, AclOperation op) {
    return new AclBuilder(principal).literalResource(ResourceType.GROUP, group).allow(op).build();
  }

  private AclBinding buildPrefixedGroupLevelAcl(String principal, String group, AclOperation op) {
    return new AclBuilder(principal).prefixedResource(ResourceType.GROUP, group).allow(op).build();
  }

  private boolean isResourcePrefixed(String res) {
    return res.length() > 1 && res.endsWith("*");
  }

  private String evaluateResourcePattern(String res) {
    return isResourcePrefixed(res) ? res.replaceFirst(".$", "") : res;
  }

  private PatternType evaluateResourcePatternType(String res) {
    return isResourcePrefixed(res) ? PatternType.PREFIXED : PatternType.LITERAL;
  }

  private void assertNoObserverPrincipals(DynamicUser user) {
    if (user.getObserverPrincipals() != null && !user.getObserverPrincipals().isEmpty()) {
      throw new RuntimeException(
          "Observer principals are not yet supported for " + user.getClass().getName());
    }
  }
}
