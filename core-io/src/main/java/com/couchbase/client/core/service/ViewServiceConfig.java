package com.couchbase.client.core.service;

import java.time.Duration;

public class ViewServiceConfig implements ServiceConfig {

  public static final int DEFAULT_MAX_ENDPOINTS = 12;
  public static final int DEFAULT_MIN_ENDPOINTS = 0;
  public static final Duration DEFAULT_IDLE_TIME = Duration.ofMinutes(5);

  private final int minEndpoints;
  private final int maxEndpoints;
  private final Duration idleTime;

  public static ViewServiceConfig create() {
    return create(DEFAULT_MIN_ENDPOINTS, DEFAULT_MAX_ENDPOINTS, DEFAULT_IDLE_TIME);
  }

  public static ViewServiceConfig create(int maxEndpoints) {
    return create(DEFAULT_MIN_ENDPOINTS, maxEndpoints, DEFAULT_IDLE_TIME);
  }

  public static ViewServiceConfig create(int minEndpoints, int maxEndpoints) {
    return create(minEndpoints, maxEndpoints, DEFAULT_IDLE_TIME);
  }

  public static ViewServiceConfig create(int minEndpoints, int maxEndpoints, Duration idleTime) {
    return new ViewServiceConfig(minEndpoints, maxEndpoints, idleTime);
  }

  private ViewServiceConfig(int minEndpoints, int maxEndpoints, Duration idleTime) {
    this.minEndpoints = minEndpoints;
    this.maxEndpoints = maxEndpoints;
    this.idleTime = idleTime;
  }

  @Override
  public int minEndpoints() {
    return minEndpoints;
  }

  @Override
  public int maxEndpoints() {
    return maxEndpoints;
  }

  @Override
  public Duration idleTime() {
    return idleTime;
  }

  @Override
  public boolean pipelined() {
    return false;
  }
}
