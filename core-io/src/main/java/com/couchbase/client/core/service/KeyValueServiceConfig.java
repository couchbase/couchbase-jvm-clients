package com.couchbase.client.core.service;

import com.couchbase.client.core.env.ServiceConfig;

import java.time.Duration;

public class KeyValueServiceConfig implements ServiceConfig {

  private final int endpoints;

  public static KeyValueServiceConfig create() {
    return create(1);
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
