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

import java.time.Duration;
import java.util.Optional;

public class TimeoutConfig {

  public static final Duration DEFAULT_KV_TIMEOUT = Duration.ofMillis(2500);
  public static final Duration DEFAULT_MANAGER_TIMEOUT = Duration.ofSeconds(5);
  public static final Duration DEFAULT_QUERY_TIMEOUT = Duration.ofSeconds(75);
  public static final Duration DEFAULT_VIEW_TIMEOUT = Duration.ofSeconds(75);
  public static final Duration DEFAULT_SEARCH_TIMEOUT = Duration.ofSeconds(75);
  public static final Duration DEFAULT_ANALYTICS_TIMEOUT = Duration.ofSeconds(75);
  public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(5);
  public static final Duration DEFAULT_DISCONNECT_TIMEOUT = Duration.ofSeconds(5);
  public static final Duration DEFAULT_STREAM_RELEASE_TIMEOUT = Duration.ofSeconds(5);

  private final Duration kvTimeout;
  private final Duration managerTimeout;
  private final Duration queryTimeout;
  private final Duration viewTimeout;
  private final Duration searchTimeout;
  private final Duration analyticsTimeout;
  private final Duration connectTimeout;
  private final Duration disconnectTimeout;

  private TimeoutConfig(final Builder builder) {
    kvTimeout = Optional.ofNullable(builder.kvTimeout).orElse(DEFAULT_KV_TIMEOUT);
    managerTimeout = Optional.ofNullable(builder.managerTimeout).orElse(DEFAULT_MANAGER_TIMEOUT);
    queryTimeout = Optional.ofNullable(builder.queryTimeout).orElse(DEFAULT_QUERY_TIMEOUT);
    viewTimeout = Optional.ofNullable(builder.viewTimeout).orElse(DEFAULT_VIEW_TIMEOUT);
    searchTimeout = Optional.ofNullable(builder.searchTimeout).orElse(DEFAULT_SEARCH_TIMEOUT);
    analyticsTimeout = Optional.ofNullable(builder.analyticsTimeout).orElse(DEFAULT_ANALYTICS_TIMEOUT);
    connectTimeout = Optional.ofNullable(builder.connectTimeout).orElse(DEFAULT_CONNECT_TIMEOUT);
    disconnectTimeout = Optional.ofNullable(builder.disconnectTimeout).orElse(DEFAULT_DISCONNECT_TIMEOUT);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static TimeoutConfig create() {
    return builder().build();
  }

  public static Builder kvTimeout(Duration kvTimeout) {
    return builder().kvTimeout(kvTimeout);
  }

  public static Builder managerTimeout(Duration managerTimeout) {
    return builder().managerTimeout(managerTimeout);
  }

  public static Builder queryTimeout(Duration queryTimeout) {
    return builder().queryTimeout(queryTimeout);
  }

  public static Builder viewTimeout(Duration viewTimeout) {
    return builder().viewTimeout(viewTimeout);
  }

  public static Builder searchTimeout(Duration searchTimeout) {
    return builder().searchTimeout(searchTimeout);
  }

  public static Builder analyticsTimeout(Duration analyticsTimeout) {
    return builder().analyticsTimeout(analyticsTimeout);
  }

  public static Builder connectTimeout(Duration connectTimeout) {
    return builder().connectTimeout(connectTimeout);

  }

  public static Builder disconnectTimeout(Duration disconnectTimeout) {
    return builder().disconnectTimeout(disconnectTimeout);
  }


  public Duration kvTimeout() {
    return kvTimeout;
  }

  public Duration managerTimeout() {
    return managerTimeout;
  }

  public Duration queryTimeout() {
    return queryTimeout;
  }

  public Duration connectTimeout() {
    return connectTimeout;
  }

  public Duration disconnectTimeout() {
    return disconnectTimeout;
  }

  public Duration viewTimeout() {
    return viewTimeout;
  }

  public Duration searchTimeout() {
    return searchTimeout;
  }

  public Duration analyticsTimeout() {
    return analyticsTimeout;
  }

  public static class Builder {

    private Duration kvTimeout = null;
    private Duration managerTimeout = null;
    private Duration queryTimeout = null;
    private Duration viewTimeout = null;
    private Duration searchTimeout = null;
    private Duration analyticsTimeout = null;
    private Duration connectTimeout = null;
    private Duration disconnectTimeout = null;

    public TimeoutConfig build() {
      return new TimeoutConfig(this);
    }

    public Builder kvTimeout(Duration kvTimeout) {
      this.kvTimeout = kvTimeout;
      return this;
    }

    public Builder managerTimeout(Duration managerTimeout) {
      this.managerTimeout = managerTimeout;
      return this;
    }

    public Builder queryTimeout(Duration queryTimeout) {
      this.queryTimeout = queryTimeout;
      return this;
    }

    public Builder viewTimeout(Duration viewTimeout) {
      this.viewTimeout = viewTimeout;
      return this;
    }

    public Builder searchTimeout(Duration searchTimeout) {
      this.searchTimeout = searchTimeout;
      return this;
    }

    public Builder analyticsTimeout(Duration analyticsTimeout) {
      this.analyticsTimeout = analyticsTimeout;
      return this;
    }

    public Builder connectTimeout(Duration connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    public Builder disconnectTimeout(Duration disconnectTimeout) {
      this.disconnectTimeout = disconnectTimeout;
      return this;
    }
  }
}
