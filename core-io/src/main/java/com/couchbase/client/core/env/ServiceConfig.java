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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.service.AnalyticsServiceConfig;
import com.couchbase.client.core.service.KeyValueServiceConfig;
import com.couchbase.client.core.service.QueryServiceConfig;
import com.couchbase.client.core.service.SearchServiceConfig;
import com.couchbase.client.core.service.ViewServiceConfig;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

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

  public static Builder keyValueServiceConfig(KeyValueServiceConfig.Builder keyValueServiceConfig) {
    return builder().keyValueServiceConfig(keyValueServiceConfig);
  }

  public static Builder queryServiceConfig(QueryServiceConfig.Builder queryServiceConfig) {
    return builder().queryServiceConfig(queryServiceConfig);
  }

  public static Builder viewServiceConfig(ViewServiceConfig.Builder viewServiceConfig) {
    return builder().viewServiceConfig(viewServiceConfig);
  }

  public static Builder searchServiceConfig(SearchServiceConfig.Builder searchServiceConfig) {
    return builder().searchServiceConfig(searchServiceConfig);
  }

  public static Builder analyticsServiceConfig(AnalyticsServiceConfig.Builder analyticsServiceConfig) {
    return builder().analyticsServiceConfig(analyticsServiceConfig);
  }

  private ServiceConfig(Builder builder) {
    this.keyValueServiceConfig = builder.keyValueServiceConfig.build();
    this.queryServiceConfig = builder.queryServiceConfig.build();
    this.viewServiceConfig = builder.viewServiceConfig.build();
    this.analyticsServiceConfig = builder.analyticsServiceConfig.build();
    this.searchServiceConfig = builder.searchServiceConfig.build();
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

  /**
   * Returns this config as a map so it can be exported into i.e. JSON for display.
   */
  @Stability.Volatile
  Map<String, Object> exportAsMap() {
    Map<String, Object> export = new LinkedHashMap<>();

    export.put("kv", keyValueServiceConfig.exportAsMap());
    export.put("query", queryServiceConfig.exportAsMap());
    export.put("view", viewServiceConfig.exportAsMap());
    export.put("search", searchServiceConfig.exportAsMap());
    export.put("analytics", analyticsServiceConfig.exportAsMap());

    return export;
  }


  public static class Builder {

    private KeyValueServiceConfig.Builder keyValueServiceConfig = KeyValueServiceConfig.builder();
    private QueryServiceConfig.Builder queryServiceConfig = QueryServiceConfig.builder();
    private ViewServiceConfig.Builder viewServiceConfig = ViewServiceConfig.builder();
    private SearchServiceConfig.Builder searchServiceConfig = SearchServiceConfig.builder();
    private AnalyticsServiceConfig.Builder analyticsServiceConfig = AnalyticsServiceConfig.builder();

    public Builder keyValueServiceConfig(KeyValueServiceConfig.Builder keyValueServiceConfig) {
      this.keyValueServiceConfig = requireNonNull(keyValueServiceConfig);
      return this;
    }

    public KeyValueServiceConfig.Builder keyValueServiceConfig() {
      return keyValueServiceConfig;
    }

    public Builder queryServiceConfig(QueryServiceConfig.Builder queryServiceConfig) {
      this.queryServiceConfig = requireNonNull(queryServiceConfig);
      return this;
    }

    public QueryServiceConfig.Builder queryServiceConfig() {
      return queryServiceConfig;
    }

    public Builder viewServiceConfig(ViewServiceConfig.Builder viewServiceConfig) {
      this.viewServiceConfig = requireNonNull(viewServiceConfig);
      return this;
    }

    public ViewServiceConfig.Builder viewServiceConfig() {
      return viewServiceConfig;
    }

    public Builder searchServiceConfig(SearchServiceConfig.Builder searchServiceConfig) {
      this.searchServiceConfig = requireNonNull(searchServiceConfig);
      return this;
    }

    public SearchServiceConfig.Builder searchServiceConfig() {
      return searchServiceConfig;
    }

    public Builder analyticsServiceConfig(AnalyticsServiceConfig.Builder analyticsServiceConfig) {
      this.analyticsServiceConfig = requireNonNull(analyticsServiceConfig);
      return this;
    }

    public AnalyticsServiceConfig.Builder analyticsServiceConfig() {
      return analyticsServiceConfig;
    }

    public ServiceConfig build() {
      return new ServiceConfig(this);
    }
  }
}
