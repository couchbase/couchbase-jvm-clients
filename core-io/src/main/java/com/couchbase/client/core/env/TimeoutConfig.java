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
import com.couchbase.client.core.msg.kv.DurabilityLevel;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class TimeoutConfig {

  public static final Duration DEFAULT_KV_TIMEOUT = Duration.ofMillis(2500);
  public static final Duration DEFAULT_KV_DURABLE_TIMEOUT = Duration.ofSeconds(10);
  public static final Duration DEFAULT_MANAGEMENT_TIMEOUT = Duration.ofSeconds(75);
  public static final Duration DEFAULT_QUERY_TIMEOUT = Duration.ofSeconds(75);
  public static final Duration DEFAULT_VIEW_TIMEOUT = Duration.ofSeconds(75);
  public static final Duration DEFAULT_SEARCH_TIMEOUT = Duration.ofSeconds(75);
  public static final Duration DEFAULT_ANALYTICS_TIMEOUT = Duration.ofSeconds(75);
  public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
  public static final Duration DEFAULT_DISCONNECT_TIMEOUT = Duration.ofSeconds(10);
  public static final Duration DEFAULT_EVENTING_TIMEOUT = Duration.ofSeconds(75);

  @Stability.Volatile
  public static final Duration DEFAULT_BACKUP_TIMEOUT = Duration.ofSeconds(75);

  private final Duration kvTimeout;
  private final Duration kvDurableTimeout;
  private final Duration managementTimeout;
  private final Duration queryTimeout;
  private final Duration viewTimeout;
  private final Duration searchTimeout;
  private final Duration analyticsTimeout;
  private final Duration connectTimeout;
  private final Duration disconnectTimeout;
  private final Duration eventingTimeout;
  private final Duration backupTimeout;

  private TimeoutConfig(final Builder builder) {
    kvTimeout = Optional.ofNullable(builder.kvTimeout).orElse(DEFAULT_KV_TIMEOUT);
    kvDurableTimeout = Optional.ofNullable(builder.kvDurableTimeout).orElse(DEFAULT_KV_DURABLE_TIMEOUT);
    managementTimeout = Optional.ofNullable(builder.managementTimeout).orElse(DEFAULT_MANAGEMENT_TIMEOUT);
    queryTimeout = Optional.ofNullable(builder.queryTimeout).orElse(DEFAULT_QUERY_TIMEOUT);
    viewTimeout = Optional.ofNullable(builder.viewTimeout).orElse(DEFAULT_VIEW_TIMEOUT);
    searchTimeout = Optional.ofNullable(builder.searchTimeout).orElse(DEFAULT_SEARCH_TIMEOUT);
    analyticsTimeout = Optional.ofNullable(builder.analyticsTimeout).orElse(DEFAULT_ANALYTICS_TIMEOUT);
    connectTimeout = Optional.ofNullable(builder.connectTimeout).orElse(DEFAULT_CONNECT_TIMEOUT);
    disconnectTimeout = Optional.ofNullable(builder.disconnectTimeout).orElse(DEFAULT_DISCONNECT_TIMEOUT);
    eventingTimeout = Optional.ofNullable(builder.eventingTimeout).orElse(DEFAULT_EVENTING_TIMEOUT);
    backupTimeout = Optional.ofNullable(builder.backupTimeout).orElse(DEFAULT_BACKUP_TIMEOUT);
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

  @Stability.Volatile
  public static Builder kvDurableTimeout(Duration kvDurableTimeout) {
    return builder().kvDurableTimeout(kvDurableTimeout);
  }

  public static Builder managementTimeout(Duration managementTimeout) {
    return builder().managementTimeout(managementTimeout);
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

  public static Builder eventingTimeout(Duration eventingTimeout) {
    return builder().eventingTimeout(eventingTimeout);
  }

  @Stability.Volatile
  public static Builder backupTimeout(Duration backupTimeout) {
    return builder().backupTimeout(backupTimeout);
  }

  public Duration kvTimeout() {
    return kvTimeout;
  }

  public Duration kvDurableTimeout() {
    return kvDurableTimeout;
  }

  public Duration managementTimeout() {
    return managementTimeout;
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

  public Duration eventingTimeout() {
    return eventingTimeout;
  }

  @Stability.Volatile
  public Duration backupTimeout() {
    return backupTimeout;
  }

  /**
   * Returns this config as a map so it can be exported into i.e. JSON for display.
   */
  @Stability.Volatile
  Map<String, Object> exportAsMap() {
    Map<String, Object> export = new LinkedHashMap<>();

    export.put("kvMs", kvTimeout.toMillis());
    export.put("kvDurableMs", kvDurableTimeout.toMillis());
    export.put("managementMs", managementTimeout.toMillis());
    export.put("queryMs", queryTimeout.toMillis());
    export.put("viewMs", viewTimeout.toMillis());
    export.put("searchMs", searchTimeout.toMillis());
    export.put("analyticsMs", analyticsTimeout.toMillis());
    export.put("connectMs", connectTimeout.toMillis());
    export.put("disconnectMs", disconnectTimeout.toMillis());
    export.put("eventingMs", eventingTimeout.toMillis());
    export.put("backupMs", backupTimeout.toMillis());

    return export;
  }


  public static class Builder {

    private Duration kvTimeout = null;
    private Duration kvDurableTimeout = null;
    private Duration managementTimeout = null;
    private Duration queryTimeout = null;
    private Duration viewTimeout = null;
    private Duration searchTimeout = null;
    private Duration analyticsTimeout = null;
    private Duration connectTimeout = null;
    private Duration disconnectTimeout = null;
    private Duration eventingTimeout = null;
    private Duration backupTimeout = null;

    public TimeoutConfig build() {
      return new TimeoutConfig(this);
    }

    /**
     * Sets the timeout to use for key-value operations.
     *
     * <p>The default is 2.5 seconds.</p>
     *
     * @return this, for chaining
     */
    public Builder kvTimeout(Duration kvTimeout) {
      this.kvTimeout = kvTimeout;
      return this;
    }

    /**
     * Sets the timeout to use for key-value operations if {@link DurabilityLevel} is set.
     *
     * <p>The default is 10 seconds.</p>
     *
     * @return this, for chaining
     */
    public Builder kvDurableTimeout(Duration kvDurableTimeout) {
      this.kvDurableTimeout = kvDurableTimeout;
      return this;
    }

    /**
     * Sets the timeout to use for manager operations.
     *
     * <p>The default is 75 seconds.</p>
     *
     * @return this, for chaining
     */
    public Builder managementTimeout(Duration managementTimeout) {
      this.managementTimeout = managementTimeout;
      return this;
    }

    /**
     * Sets the timeout to use for query operations.
     *
     * <p>The default is 75 seconds.</p>
     *
     * @return this, for chaining
     */
    public Builder queryTimeout(Duration queryTimeout) {
      this.queryTimeout = queryTimeout;
      return this;
    }

    /**
     * Sets the timeout to use for view operations.
     *
     * <p>The default is 75 seconds.</p>
     *
     * @return this, for chaining
     */
    public Builder viewTimeout(Duration viewTimeout) {
      this.viewTimeout = viewTimeout;
      return this;
    }

    /**
     * Sets the timeout to use for search operations.
     *
     * <p>The default is 75 seconds.</p>
     *
     * @return this, for chaining
     */
    public Builder searchTimeout(Duration searchTimeout) {
      this.searchTimeout = searchTimeout;
      return this;
    }

    /**
     * Sets the timeout to use for analytics operations.
     *
     * <p>The default is 75 seconds.</p>
     *
     * @return this, for chaining
     */
    public Builder analyticsTimeout(Duration analyticsTimeout) {
      this.analyticsTimeout = analyticsTimeout;
      return this;
    }

    /**
     * Sets the timeout to use for connecting and socket connects.
     *
     * <p>The default is 10 seconds.</p>
     *
     * @return this, for chaining
     */
    public Builder connectTimeout(Duration connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    /**
     * Sets the timeout to use for disconnection operations.
     *
     * <p>The default is 10 seconds.</p>
     *
     * @return this, for chaining
     */
    public Builder disconnectTimeout(Duration disconnectTimeout) {
      this.disconnectTimeout = disconnectTimeout;
      return this;
    }

    /**
     * Sets the timeout to use for eventing operations.
     *
     * <p>The default is 75 seconds.</p>
     *
     * @return this, for chaining
     */
    public Builder eventingTimeout(Duration eventingTimeout) {
      this.eventingTimeout = eventingTimeout;
      return this;
    }

    /**
     * Sets the timeout to use for backup operations.
     *
     * <p>The default is 75 seconds.</p>
     *
     * @return this, for chaining
     */
    @Stability.Volatile
    public Builder backupTimeout(Duration backupTimeout) {
      this.backupTimeout = backupTimeout;
      return this;
    }

  }
}
