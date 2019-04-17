package com.couchbase.client.core.service;

import java.time.Duration;

public class KeyValueServiceConfig implements ServiceConfig {
  public static int DEFAULT_ENDPOINTS = 1;

  private final int endpoints;

  public static KeyValueServiceConfig.Builder builder() {
    return new Builder().endpoints(DEFAULT_ENDPOINTS);
  }

  private KeyValueServiceConfig(Builder builder) {
    this.endpoints = builder.endpoints;
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

  public static class Builder {
    private int endpoints;

    public Builder endpoints(int endpoints) {
      this.endpoints = endpoints;
      return this;
    }

    public KeyValueServiceConfig build() {
      return new KeyValueServiceConfig(this);
    }
  }

  @Override
  public String toString() {
    return "KeyValueServiceConfig{" +
      "endpoints=" + endpoints +
      '}';
  }
}
