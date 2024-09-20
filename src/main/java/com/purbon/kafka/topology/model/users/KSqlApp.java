package com.purbon.kafka.topology.model.users;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.purbon.kafka.topology.model.DynamicUser;
import com.purbon.kafka.topology.model.User;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class KSqlApp extends DynamicUser {

  @JsonInclude(Include.NON_EMPTY)
  private Optional<String> applicationId;

  private String ksqlDbId;

  public KSqlApp() {
    this("", new HashMap<>(), new ArrayList<>());
  }

  public KSqlApp(
      String principal,
      HashMap<String, List<String>> topics,
      List<User> observerPrincipals,
      Optional<String> applicationId) {
    super(principal, topics, observerPrincipals);
    this.applicationId = applicationId;
  }

  public KSqlApp(
      String principal, HashMap<String, List<String>> topics, List<User> observerPrincipals) {
    this(principal, topics, observerPrincipals, Optional.empty());
  }

  public Optional<String> getApplicationId() {
    return applicationId;
  }

  public void setApplicationId(Optional<String> applicationId) {
    this.applicationId = applicationId;
  }

  public void setKsqlDbId(String ksqlDbId) {
    this.ksqlDbId = ksqlDbId;
  }

  public String getKsqlDbId() {
    return ksqlDbId;
  }
}
