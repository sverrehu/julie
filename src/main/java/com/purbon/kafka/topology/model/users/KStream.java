package com.purbon.kafka.topology.model.users;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.purbon.kafka.topology.model.DynamicUser;
import com.purbon.kafka.topology.model.User;
import java.util.*;

public class KStream extends DynamicUser {

  @JsonInclude(Include.NON_EMPTY)
  private Optional<String> applicationId;

  @JsonInclude(Include.NON_EMPTY)
  private Optional<Boolean> exactlyOnce;

  @JsonInclude(Include.NON_EMPTY)
  private Optional<GroupConfig> groupConfig;

  public KStream() {
    this("", new HashMap<>(), Collections.emptyList());
  }

  public KStream(
      String principal,
      Map<String, List<String>> topics,
      List<User> observerPrincipals,
      Optional<String> applicationId,
      Optional<Boolean> exactlyOnce,
      Optional<GroupConfig> groupConfig) {
    super(principal, topics, observerPrincipals);
    this.applicationId = applicationId;
    this.exactlyOnce = exactlyOnce;
    this.groupConfig = groupConfig;
  }

  public KStream(
      String principal,
      Map<String, List<String>> topics,
      List<User> observerPrincipals,
      Optional<String> applicationId) {
    this(
        principal,
        topics,
        observerPrincipals,
        applicationId,
        Optional.of(Boolean.FALSE),
        Optional.empty());
  }

  public KStream(
      String principal, Map<String, List<String>> topics, List<User> observerPrincipals) {
    this(
        principal,
        topics,
        observerPrincipals,
        Optional.empty(),
        Optional.of(Boolean.FALSE),
        Optional.empty());
  }

  public Optional<String> getApplicationId() {
    return applicationId;
  }

  public Optional<Boolean> getExactlyOnce() {
    return exactlyOnce;
  }

  public void setApplicationId(Optional<String> applicationId) {
    this.applicationId = applicationId;
  }

  public void setExactlyOnce(Optional<Boolean> exactlyOnce) {
    this.exactlyOnce = exactlyOnce;
  }

  public Optional<GroupConfig> getGroupConfig() {
    return groupConfig;
  }

  public void setGroupConfig(Optional<GroupConfig> groupConfig) {
    this.groupConfig = groupConfig;
  }
}
