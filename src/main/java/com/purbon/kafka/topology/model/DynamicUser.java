package com.purbon.kafka.topology.model;

import java.util.List;
import java.util.Map;

public class DynamicUser extends User {

  private Map<String, List<String>> topics;
  private List<User> observerPrincipals;

  public static final String READ_TOPICS = "read";
  public static final String WRITE_TOPICS = "write";

  public DynamicUser(
      String principal, Map<String, List<String>> topics, List<User> observerPrincipals) {
    super(principal);
    this.topics = topics;
    this.observerPrincipals = observerPrincipals;
  }

  public Map<String, List<String>> getTopics() {
    return topics;
  }

  public void setTopics(Map<String, List<String>> topics) {
    this.topics = topics;
  }

  public List<User> getObserverPrincipals() {
    return observerPrincipals;
  }

  public void setObserverPrincipals(final List<User> observerPrincipals) {
    this.observerPrincipals = observerPrincipals;
  }
}
