package com.couchbase.client.core.service;

import java.time.Duration;

public class KeyValueServiceConfig implements ServiceConfig {
  public static int DEFAULT_ENDPOINTS = 1;

  private final int endpoints;

  public static KeyValueServiceConfig create() {
    return create(DEFAULT_ENDPOINTS);
  }

  public static KeyValueServiceConfig create(int endpoints) {
    return new KeyValueServiceConfig(endpoints);
  }

  private KeyValueServiceConfig(int endpoints) {
    this.endpoints = endpoints;
  }

  @Override
  public int minEndpoints() {
    return endpoints;
  }

  @Override
  public int maxEndpoints() {
    return endpoints;
  }

  @Override
  public Duration idleTime() {
    return Duration.ZERO;
  }

  @Override
  public boolean pipelined() {
    return true;
  }
}
