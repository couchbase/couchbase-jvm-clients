/*
 * Copyright (c) 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

  public static Builder keyValueServiceConfig(KeyValueServiceConfig keyValueServiceConfig) {
    return builder().keyValueServiceConfig(keyValueServiceConfig);
  }

  public static Builder queryServiceConfig(QueryServiceConfig queryServiceConfig) {
    return builder().queryServiceConfig(queryServiceConfig);
  }

  public static Builder viewServiceConfig(ViewServiceConfig viewServiceConfig) {
    return builder().viewServiceConfig(viewServiceConfig);
  }

  public static Builder searchServiceConfig(SearchServiceConfig searchServiceConfig) {
    return builder().searchServiceConfig(searchServiceConfig);
  }

  public static Builder analyticsServiceConfig(AnalyticsServiceConfig analyticsServiceConfig) {
    return builder().analyticsServiceConfig(analyticsServiceConfig);
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

  public static class Builder {

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
