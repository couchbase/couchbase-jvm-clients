package com.couchbase.client.core.service;

import java.time.Duration;

public class SearchServiceConfig extends AbstractPooledEndpointServiceConfig {

  public static final int DEFAULT_MAX_ENDPOINTS = 12;
  public static final int DEFAULT_MIN_ENDPOINTS = 0;
  public static final Duration DEFAULT_IDLE_TIME = Duration.ofMinutes(5);

  public static Builder builder() {
    return new Builder()
      .minEndpoints(DEFAULT_MIN_ENDPOINTS)
      .maxEndpoints(DEFAULT_MAX_ENDPOINTS)
      .idleTime(DEFAULT_IDLE_TIME);
  }

  private SearchServiceConfig(Builder builder) {
    super(builder);
  }

  public static class Builder extends AbstractPooledEndpointServiceConfig.Builder<Builder> {
    public SearchServiceConfig build() {
      return new SearchServiceConfig(this);
    }
  }

}
