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
import com.couchbase.client.core.endpoint.CircuitBreaker;
import com.couchbase.client.core.endpoint.CircuitBreakerConfig;
import com.couchbase.client.core.service.AbstractPooledEndpointServiceConfig;
import com.couchbase.client.core.service.ServiceType;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class IoConfig {

  public static final boolean DEFAULT_MUTATION_TOKENS_ENABLED = true;
  public static final boolean DEFAULT_DNS_SRV_ENABLED = true;
  public static final boolean DEFAULT_TCP_KEEPALIVE_ENABLED = true;
  public static final Duration DEFAULT_TCP_KEEPALIVE_TIME = Duration.ofSeconds(60);
  public static final Duration DEFAULT_CONFIG_POLL_INTERVAL = Duration.ofMillis(2500);
  public static final NetworkResolution DEFAULT_NETWORK_RESOLUTION = NetworkResolution.AUTO;
  public static final int DEFAULT_NUM_KV_CONNECTIONS = 1;
  public static final int DEFAULT_MAX_HTTP_CONNECTIONS = AbstractPooledEndpointServiceConfig.DEFAULT_MAX_ENDPOINTS;
  public static final Duration DEFAULT_IDLE_HTTP_CONNECTION_TIMEOUT = AbstractPooledEndpointServiceConfig.DEFAULT_IDLE_TIME;

  private final boolean mutationTokensEnabled;
  private final Duration configPollInterval;
  private final CircuitBreakerConfig kvCircuitBreakerConfig;
  private final CircuitBreakerConfig queryCircuitBreakerConfig;
  private final CircuitBreakerConfig viewCircuitBreakerConfig;
  private final CircuitBreakerConfig searchCircuitBreakerConfig;
  private final CircuitBreakerConfig analyticsCircuitBreakerConfig;
  private final CircuitBreakerConfig managerCircuitBreakerConfig;
  private final Set<ServiceType> captureTraffic;
  private final NetworkResolution networkResolution;
  private final boolean dnsSrvEnabled;
  private final boolean tcpKeepAlivesEnabled;
  private final Duration tcpKeepAliveTime;
  private final int numKvConnections;
  private final int maxHttpConnections;
  private final Duration idleHttpConnectionTimeout;

  private IoConfig(Builder builder) {
    mutationTokensEnabled = builder.mutationTokensEnabled;
    dnsSrvEnabled = builder.dnsSrvEnabled;
    configPollInterval = Optional
      .ofNullable(builder.configPollInterval)
      .orElse(DEFAULT_CONFIG_POLL_INTERVAL);
    kvCircuitBreakerConfig = builder.kvCircuitBreakerConfig.build();
    queryCircuitBreakerConfig = builder.queryCircuitBreakerConfig.build();
    viewCircuitBreakerConfig = builder.viewCircuitBreakerConfig.build();
    searchCircuitBreakerConfig = builder.searchCircuitBreakerConfig.build();
    analyticsCircuitBreakerConfig = builder.analyticsCircuitBreakerConfig.build();
    managerCircuitBreakerConfig = builder.managerCircuitBreakerConfig.build();
    captureTraffic = Optional
      .ofNullable(builder.captureTraffic)
      .orElse(Collections.emptySet());
    networkResolution = builder.networkResolution;
    tcpKeepAlivesEnabled = builder.tcpKeepAlivesEnabled;
    tcpKeepAliveTime = builder.tcpKeepAliveTime;
    numKvConnections = builder.numKvConnections;
    maxHttpConnections = builder.maxHttpConnections;
    idleHttpConnectionTimeout = builder.idleHttpConnectionTimeout;
  }

  public static IoConfig create() {
    return builder().build();
  }

  public static Builder builder() {
    return new IoConfig.Builder();
  }

  public static Builder enableMutationTokens(boolean mutationTokensEnabled) {
    return builder().enableMutationTokens(mutationTokensEnabled);
  }

  public static Builder enableDnsSrv(boolean dnsSrvEnabled) {
    return builder().enableDnsSrv(dnsSrvEnabled);
  }

  public static Builder configPollInterval(Duration configPollInterval) {
    return builder().configPollInterval(configPollInterval);
  }

  public static Builder kvCircuitBreakerConfig(CircuitBreakerConfig.Builder kvCircuitBreakerConfig) {
    return builder().kvCircuitBreakerConfig(kvCircuitBreakerConfig);
  }

  public static Builder queryCircuitBreakerConfig(CircuitBreakerConfig.Builder queryCircuitBreakerConfig) {
    return builder().queryCircuitBreakerConfig(queryCircuitBreakerConfig);
  }

  public static Builder viewCircuitBreakerConfig(CircuitBreakerConfig.Builder viewCircuitBreakerConfig) {
    return builder().viewCircuitBreakerConfig(viewCircuitBreakerConfig);
  }

  public static Builder searchCircuitBreakerConfig(CircuitBreakerConfig.Builder searchCircuitBreakerConfig) {
    return builder().searchCircuitBreakerConfig(searchCircuitBreakerConfig);
  }

  public static Builder analyticsCircuitBreakerConfig(CircuitBreakerConfig.Builder analyticsCircuitBreakerConfig) {
    return builder().analyticsCircuitBreakerConfig(analyticsCircuitBreakerConfig);
  }

  public static Builder managerCircuitBreakerConfig(CircuitBreakerConfig.Builder managerCircuitBreakerConfig) {
    return builder().managerCircuitBreakerConfig(managerCircuitBreakerConfig);
  }

  public static Builder captureTraffic(final ServiceType... serviceTypes) {
    return builder().captureTraffic(serviceTypes);
  }

  public static Builder networkResolution(final NetworkResolution networkResolution) {
    return builder().networkResolution(networkResolution);
  }

  public static Builder enableTcpKeepAlives(final boolean tcpKeepAliveEnabled) {
    return builder().enableTcpKeepAlives(tcpKeepAliveEnabled);
  }

  public static Builder tcpKeepAliveTime(final Duration tcpKeepAliveTime) {
    return builder().tcpKeepAliveTime(tcpKeepAliveTime);
  }

  public static Builder numKvConnections(int numKvConnections) {
    return builder().numKvConnections(numKvConnections);
  }

  public static Builder maxHttpConnections(int maxHttpConnections) {
    return builder().maxHttpConnections(maxHttpConnections);
  }

  public static Builder idleHttpConnectionTimeout(Duration idleHttpConnectionTimeout) {
    return builder().idleHttpConnectionTimeout(idleHttpConnectionTimeout);
  }

  public CircuitBreakerConfig kvCircuitBreakerConfig() {
    return kvCircuitBreakerConfig;
  }

  public CircuitBreakerConfig queryCircuitBreakerConfig() {
    return queryCircuitBreakerConfig;
  }

  public CircuitBreakerConfig viewCircuitBreakerConfig() {
    return viewCircuitBreakerConfig;
  }

  public CircuitBreakerConfig searchCircuitBreakerConfig() {
    return searchCircuitBreakerConfig;
  }

  public CircuitBreakerConfig analyticsCircuitBreakerConfig() {
    return analyticsCircuitBreakerConfig;
  }

  public CircuitBreakerConfig managerCircuitBreakerConfig() {
    return managerCircuitBreakerConfig;
  }

  public boolean mutationTokensEnabled() {
    return mutationTokensEnabled;
  }

  public boolean dnsSrvEnabled() {
    return dnsSrvEnabled;
  }

  public Duration configPollInterval() {
    return configPollInterval;
  }

  public Set<ServiceType> captureTraffic() {
    return captureTraffic;
  }

  public NetworkResolution networkResolution() {
    return networkResolution;
  }

  public boolean tcpKeepAlivesEnabled() {
    return tcpKeepAlivesEnabled;
  }

  public Duration tcpKeepAliveTime() {
    return tcpKeepAliveTime;
  }

  public int numKvConnections() {
    return numKvConnections;
  }

  public int maxHttpConnections() {
    return maxHttpConnections;
  }

  public Duration idleHttpConnectionTimeout() {
    return idleHttpConnectionTimeout;
  }

  /**
   * Returns this config as a map so it can be exported into i.e. JSON for display.
   */
  @Stability.Volatile
  Map<String, Object> exportAsMap() {
    Map<String, Object> export = new LinkedHashMap<>();
    export.put("captureTraffic", captureTraffic);
    export.put("mutationTokensEnabled", mutationTokensEnabled);
    export.put("networkResolution", networkResolution.name());
    export.put("dnsSrvEnabled", dnsSrvEnabled);
    export.put("tcpKeepAlivesEnabled", tcpKeepAlivesEnabled);
    export.put("tcpKeepAliveTime", tcpKeepAliveTime);
    export.put("configPollIntervalMillis", configPollInterval.toMillis());
    export.put("kvCircuitBreakerConfig", kvCircuitBreakerConfig.enabled() ? kvCircuitBreakerConfig.exportAsMap() : "disabled");
    export.put("queryCircuitBreakerConfig", queryCircuitBreakerConfig.enabled() ? queryCircuitBreakerConfig.exportAsMap() : "disabled");
    export.put("viewCircuitBreakerConfig", viewCircuitBreakerConfig.enabled() ? viewCircuitBreakerConfig.exportAsMap() : "disabled");
    export.put("searchCircuitBreakerConfig", searchCircuitBreakerConfig.enabled() ? searchCircuitBreakerConfig.exportAsMap() : "disabled");
    export.put("analyticsCircuitBreakerConfig", analyticsCircuitBreakerConfig.enabled() ? analyticsCircuitBreakerConfig.exportAsMap() : "disabled");
    export.put("managerCircuitBreakerConfig", managerCircuitBreakerConfig.enabled() ? managerCircuitBreakerConfig.exportAsMap() : "disabled");
    export.put("numKvConnections", numKvConnections);
    export.put("maxHttpConnections", maxHttpConnections);
    export.put("idleHttpConnectionTimeout", idleHttpConnectionTimeout);
    return export;
  }

  public static class Builder {

    private boolean mutationTokensEnabled = DEFAULT_MUTATION_TOKENS_ENABLED;
    private Duration configPollInterval;
    private CircuitBreakerConfig.Builder kvCircuitBreakerConfig = CircuitBreakerConfig.builder().enabled(false);
    private CircuitBreakerConfig.Builder queryCircuitBreakerConfig = CircuitBreakerConfig.builder().enabled(false);
    private CircuitBreakerConfig.Builder viewCircuitBreakerConfig = CircuitBreakerConfig.builder().enabled(false);
    private CircuitBreakerConfig.Builder searchCircuitBreakerConfig = CircuitBreakerConfig.builder().enabled(false);
    private CircuitBreakerConfig.Builder analyticsCircuitBreakerConfig = CircuitBreakerConfig.builder().enabled(false);
    private CircuitBreakerConfig.Builder managerCircuitBreakerConfig = CircuitBreakerConfig.builder().enabled(false);
    private Set<ServiceType> captureTraffic;
    private NetworkResolution networkResolution = DEFAULT_NETWORK_RESOLUTION;
    private boolean dnsSrvEnabled = DEFAULT_DNS_SRV_ENABLED;
    private boolean tcpKeepAlivesEnabled = DEFAULT_TCP_KEEPALIVE_ENABLED;
    private Duration tcpKeepAliveTime = DEFAULT_TCP_KEEPALIVE_TIME;
    private int numKvConnections = DEFAULT_NUM_KV_CONNECTIONS;
    private int maxHttpConnections = DEFAULT_MAX_HTTP_CONNECTIONS;
    private Duration idleHttpConnectionTimeout = DEFAULT_IDLE_HTTP_CONNECTION_TIMEOUT;

    public IoConfig build() {
      return new IoConfig(this);
    }

    public Builder configPollInterval(Duration configPollInterval) {
      this.configPollInterval = configPollInterval;
      return this;
    }

    /**
     * Configures whether mutation tokens will be returned from the server for all mutation operations.
     *
     * @return this, for chaining
     */
    public Builder enableMutationTokens(boolean mutationTokensEnabled) {
      this.mutationTokensEnabled = mutationTokensEnabled;
      return this;
    }

    public Builder enableDnsSrv(boolean dnsSrvEnabled) {
      this.dnsSrvEnabled = dnsSrvEnabled;
      return this;
    }

    public Builder enableTcpKeepAlives(boolean tcpKeepAlivesEnabled) {
      this.tcpKeepAlivesEnabled = tcpKeepAlivesEnabled;
      return this;
    }

    /**
     * Allows to customize the idle time after which a tcp keepalive gets fired.
     * <p>
     * Please note that this setting only propagates to the OS on linux when the epoll transport is used. On all
     * other platforms, the OS-configured time is used (and you need to tune it there if you want to customize
     * the default behavior).
     *
     * @param tcpKeepAliveTime the custom keepalive time.
     * @return this builder for chaining purposes.
     */
    public Builder tcpKeepAliveTime(final Duration tcpKeepAliveTime) {
      this.tcpKeepAliveTime = tcpKeepAliveTime;
      return this;
    }

    /**
     * Configures a {@link CircuitBreaker} to use for key-value operations.
     *
     * @return this, for chaining
     */
    public Builder kvCircuitBreakerConfig(CircuitBreakerConfig.Builder kvCircuitBreakerConfig) {
      this.kvCircuitBreakerConfig = kvCircuitBreakerConfig;
      return this;
    }

    public CircuitBreakerConfig.Builder kvCircuitBreakerConfig() {
      return kvCircuitBreakerConfig;
    }

    /**
     * Configures a {@link CircuitBreaker} to use for query operations.
     *
     * @return this, for chaining
     */
    public Builder queryCircuitBreakerConfig(CircuitBreakerConfig.Builder queryCircuitBreakerConfig) {
      this.queryCircuitBreakerConfig = queryCircuitBreakerConfig;
      return this;
    }

    public CircuitBreakerConfig.Builder queryCircuitBreakerConfig() {
      return queryCircuitBreakerConfig;
    }

    /**
     * Configures a {@link CircuitBreaker} to use for view operations.
     *
     * @return this, for chaining
     */
    public Builder viewCircuitBreakerConfig(CircuitBreakerConfig.Builder viewCircuitBreakerConfig) {
      this.viewCircuitBreakerConfig = viewCircuitBreakerConfig;
      return this;
    }

    public CircuitBreakerConfig.Builder viewCircuitBreakerConfig() {
      return viewCircuitBreakerConfig;
    }

    /**
     * Configures a {@link CircuitBreaker} to use for search operations.
     *
     * @return this, for chaining
     */
    public Builder searchCircuitBreakerConfig(CircuitBreakerConfig.Builder searchCircuitBreakerConfig) {
      this.searchCircuitBreakerConfig = searchCircuitBreakerConfig;
      return this;
    }

    public CircuitBreakerConfig.Builder searchCircuitBreakerConfig() {
      return searchCircuitBreakerConfig;
    }

    /**
     * Configures a {@link CircuitBreaker} to use for analytics operations.
     *
     * @return this, for chaining
     */
    public Builder analyticsCircuitBreakerConfig(CircuitBreakerConfig.Builder analyticsCircuitBreakerConfig) {
      this.analyticsCircuitBreakerConfig = analyticsCircuitBreakerConfig;
      return this;
    }

    public CircuitBreakerConfig.Builder analyticsCircuitBreakerConfig() {
      return analyticsCircuitBreakerConfig;
    }

    /**
     * Configures a {@link CircuitBreaker} to use for manager operations.
     *
     * @return this, for chaining
     */
    public Builder managerCircuitBreakerConfig(CircuitBreakerConfig.Builder managerCircuitBreakerConfig) {
      this.managerCircuitBreakerConfig = managerCircuitBreakerConfig;
      return this;
    }

    public CircuitBreakerConfig.Builder managerCircuitBreakerConfig() {
      return managerCircuitBreakerConfig;
    }

    public Builder captureTraffic(final ServiceType... serviceTypes) {
      this.captureTraffic = serviceTypes.length == 0
        ? EnumSet.allOf(ServiceType.class)
        : EnumSet.copyOf(Arrays.asList(serviceTypes));
      return this;
    }

    public Builder networkResolution(final NetworkResolution networkResolution) {
      this.networkResolution = networkResolution;
      return this;
    }

    public Builder numKvConnections(int numKvConnections) {
      this.numKvConnections = numKvConnections;
      return this;
    }

    public Builder maxHttpConnections(int maxHttpConnections) {
      this.maxHttpConnections = maxHttpConnections;
      return this;
    }

    public Builder idleHttpConnectionTimeout(Duration idleHttpConnectionTimeout) {
      this.idleHttpConnectionTimeout = idleHttpConnectionTimeout;
      return this;
    }
  }
}
