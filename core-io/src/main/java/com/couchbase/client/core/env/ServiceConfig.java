package com.couchbase.client.core.env;

import com.couchbase.client.core.service.*;

import java.util.Optional;

public class ServiceConfig {

  private final KeyValueServiceConfig keyValueServiceConfig;
  private final QueryServiceConfig queryServiceConfig;
  private final ViewServiceConfig viewServiceConfig;
  private final SearchServiceConfig searchServiceConfig;
  private final AnalyticsServiceConfig analyticsServiceConfig;

  public static ServiceConfig create() {
    return builder().build();
  }

  public static Builder builder() {
    return new Builder();
  }

  private ServiceConfig(Builder builder) {
    this.keyValueServiceConfig = Optional
      .ofNullable(builder.keyValueServiceConfig)
      .orElse(KeyValueServiceConfig.create());
    this.queryServiceConfig = Optional
      .ofNullable(builder.queryServiceConfig)
      .orElse(QueryServiceConfig.create());
    this.viewServiceConfig = Optional
      .ofNullable(builder.viewServiceConfig)
      .orElse(ViewServiceConfig.create());
    this.analyticsServiceConfig = Optional
      .ofNullable(builder.analyticsServiceConfig)
      .orElse(AnalyticsServiceConfig.create());
    this.searchServiceConfig = Optional
      .ofNullable(builder.searchServiceConfig)
      .orElse(SearchServiceConfig.create());
  }

  public KeyValueServiceConfig keyValueServiceConfig() {
    return keyValueServiceConfig;
  }

  public QueryServiceConfig queryServiceConfig() {
    return queryServiceConfig;
  }

  public ViewServiceConfig viewServiceConfig() {
    return viewServiceConfig;
  }

  public SearchServiceConfig searchServiceConfig() {
    return searchServiceConfig;
  }

  public AnalyticsServiceConfig analyticsServiceConfig() {
    return analyticsServiceConfig;
  }

  static class Builder {

    private KeyValueServiceConfig keyValueServiceConfig = null;
    private QueryServiceConfig queryServiceConfig = null;
    private ViewServiceConfig viewServiceConfig = null;
    private SearchServiceConfig searchServiceConfig = null;
    private AnalyticsServiceConfig analyticsServiceConfig = null;

    public Builder keyValueServiceConfig(KeyValueServiceConfig keyValueServiceConfig) {
      this.keyValueServiceConfig = keyValueServiceConfig;
      return this;
    }

    public Builder queryServiceConfig(QueryServiceConfig queryServiceConfig) {
      this.queryServiceConfig = queryServiceConfig;
      return this;
    }

    public Builder viewServiceConfig(ViewServiceConfig viewServiceConfig) {
      this.viewServiceConfig = viewServiceConfig;
      return this;
    }

    public Builder searchServiceConfig(SearchServiceConfig searchServiceConfig) {
      this.searchServiceConfig = searchServiceConfig;
      return this;
    }

    public Builder analyticsServiceConfig(AnalyticsServiceConfig analyticsServiceConfig) {
      this.analyticsServiceConfig = analyticsServiceConfig;
      return this;
    }

    public ServiceConfig build() {
      return new ServiceConfig(this);
    }
  }
}
