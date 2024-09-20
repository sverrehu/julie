package com.purbon.kafka.topology.api.adminclient;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

public class AclBuilder {

  private ResourcePattern resourcePattern;
  private AccessControlEntry entry;
  private String principal;

  public AclBuilder(String principal) {
    this.principal = principal;
  }

  public AclBuilder literalResource(ResourceType resourceType, String name) {
    return resource(resourceType, name, PatternType.LITERAL);
  }

  public AclBuilder prefixedResource(ResourceType resourceType, String name) {
    return resource(resourceType, name, PatternType.PREFIXED);
  }

  public AclBuilder resource(ResourceType resourceType, String name, PatternType patternType) {
    resourcePattern = new ResourcePattern(resourceType, name, patternType);
    return this;
  }

  public AclBuilder allow(AclOperation op) {
    return allow("*", op);
  }

  public AclBuilder allow(String host, AclOperation op) {
    entry = new AccessControlEntry(principal, host, op, AclPermissionType.ALLOW);
    return this;
  }

  public AclBinding build() {
    return new AclBinding(resourcePattern, entry);
  }
}
