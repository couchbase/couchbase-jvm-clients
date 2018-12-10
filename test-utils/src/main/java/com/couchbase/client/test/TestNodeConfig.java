package com.couchbase.client.test;

import java.util.Map;

public class TestNodeConfig {

  private final String hostname;

  private final Map<Services, Integer> ports;

  public TestNodeConfig(String hostname, Map<Services, Integer> ports) {
    this.hostname = hostname;
    this.ports = ports;
  }

  public String hostname() {
    return hostname;
  }

  public Map<Services, Integer> ports() {
    return ports;
  }
}
