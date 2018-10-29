package com.couchbase.client.util;

import com.couchbase.client.core.service.ServiceType;

import java.util.Map;

public class TestNodeConfig {

  private final String hostname;

  private final Map<ServiceType, Integer> ports;

  public TestNodeConfig(String hostname, Map<ServiceType, Integer> ports) {
    this.hostname = hostname;
    this.ports = ports;
  }

  public String hostname() {
    return hostname;
  }

  public Map<ServiceType, Integer> ports() {
    return ports;
  }
}
