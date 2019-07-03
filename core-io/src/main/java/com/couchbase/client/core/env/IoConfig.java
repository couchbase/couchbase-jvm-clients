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

  public static final boolean DEFAULT_MUTATION_TOKENS_ENABLED = false;
  public static final Duration DEFAULT_CONFIG_POLL_INTERVAL = Duration.ofMillis(2500);

  private final Set<SaslMechanism> allowedSaslMechanisms;
  private final boolean mutationTokensEnabled;
  private final Duration configPollInterval;
  private final CircuitBreakerConfig kvCircuitBreakerConfig;
  private final CircuitBreakerConfig queryCircuitBreakerConfig;
  private final CircuitBreakerConfig viewCircuitBreakerConfig;
  private final CircuitBreakerConfig searchCircuitBreakerConfig;
  private final CircuitBreakerConfig analyticsCircuitBreakerConfig;
  private final CircuitBreakerConfig managerCircuitBreakerConfig;
  private final Set<ServiceType> captureTraffic;

  private IoConfig(Builder builder) {
    mutationTokensEnabled = builder.mutationTokensEnabled;
    configPollInterval = Optional
      .ofNullable(builder.configPollInterval)
      .orElse(DEFAULT_CONFIG_POLL_INTERVAL);
    allowedSaslMechanisms = Optional
      .ofNullable(builder.allowedSaslMechanisms)
      .orElse(EnumSet.allOf(SaslMechanism.class));
    kvCircuitBreakerConfig = builder.kvCircuitBreakerConfig.build();
    queryCircuitBreakerConfig = builder.queryCircuitBreakerConfig.build();
    viewCircuitBreakerConfig = builder.viewCircuitBreakerConfig.build();
    searchCircuitBreakerConfig = builder.searchCircuitBreakerConfig.build();
    analyticsCircuitBreakerConfig = builder.analyticsCircuitBreakerConfig.build();
    managerCircuitBreakerConfig = builder.managerCircuitBreakerConfig.build();
    captureTraffic = Optional
      .ofNullable(builder.captureTraffic)
      .orElse(Collections.emptySet());
  }

  public static IoConfig create() {
    return builder().build();
  }

  public static Builder builder() {
    return new IoConfig.Builder();
  }

  public static Builder mutationTokensEnabled(boolean mutationTokensEnabled) {
    return builder().mutationTokensEnabled(mutationTokensEnabled);
  }

  public static Builder allowedSaslMechanisms(Set<SaslMechanism> allowedSaslMechanisms) {
    return builder().allowedSaslMechanisms(allowedSaslMechanisms);
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

  public Set<SaslMechanism> allowedSaslMechanisms() {
    return allowedSaslMechanisms;
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

  public Duration configPollInterval() {
    return configPollInterval;
  }

  public Set<ServiceType> captureTraffic() {
    return captureTraffic;
  }

  /**
   * Returns this config as a map so it can be exported into i.e. JSON for display.
   */
  @Stability.Volatile
  Map<String, Object> exportAsMap() {
    Map<String, Object> export = new LinkedHashMap<>();
    export.put("allowedSaslMechs", allowedSaslMechanisms);
    export.put("captureTraffic", captureTraffic);
    export.put("mutationTokensEnabled", mutationTokensEnabled);
    export.put("configPollIntervalMillis", configPollInterval.toMillis());
    export.put("kvCircuitBreakerConfig", kvCircuitBreakerConfig.enabled() ? kvCircuitBreakerConfig.exportAsMap() : "disabled");
    export.put("queryCircuitBreakerConfig", queryCircuitBreakerConfig.enabled() ? queryCircuitBreakerConfig.exportAsMap() : "disabled");
    export.put("viewCircuitBreakerConfig", viewCircuitBreakerConfig.enabled() ? viewCircuitBreakerConfig.exportAsMap() : "disabled");
    export.put("searchCircuitBreakerConfig", searchCircuitBreakerConfig.enabled() ? searchCircuitBreakerConfig.exportAsMap() : "disabled");
    export.put("analyticsCircuitBreakerConfig", analyticsCircuitBreakerConfig.enabled() ? analyticsCircuitBreakerConfig.exportAsMap() : "disabled");
    export.put("managerCircuitBreakerConfig", managerCircuitBreakerConfig.enabled() ? managerCircuitBreakerConfig.exportAsMap() : "disabled");
    return export;
  }

  public static class Builder {

    private Set<SaslMechanism> allowedSaslMechanisms;
    private boolean mutationTokensEnabled = DEFAULT_MUTATION_TOKENS_ENABLED;
    private Duration configPollInterval;
    private CircuitBreakerConfig.Builder kvCircuitBreakerConfig = CircuitBreakerConfig.builder().enabled(false);
    private CircuitBreakerConfig.Builder queryCircuitBreakerConfig = CircuitBreakerConfig.builder().enabled(false);
    private CircuitBreakerConfig.Builder viewCircuitBreakerConfig = CircuitBreakerConfig.builder().enabled(false);
    private CircuitBreakerConfig.Builder searchCircuitBreakerConfig = CircuitBreakerConfig.builder().enabled(false);
    private CircuitBreakerConfig.Builder analyticsCircuitBreakerConfig = CircuitBreakerConfig.builder().enabled(false);
    private CircuitBreakerConfig.Builder managerCircuitBreakerConfig = CircuitBreakerConfig.builder().enabled(false);
    private Set<ServiceType> captureTraffic;

    public IoConfig build() {
      return new IoConfig(this);
    }

    public Builder allowedSaslMechanisms(Set<SaslMechanism> allowedSaslMechanisms) {
      this.allowedSaslMechanisms = allowedSaslMechanisms;
      return this;
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
    public Builder mutationTokensEnabled(boolean mutationTokensEnabled) {
      this.mutationTokensEnabled = mutationTokensEnabled;
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
  }

}
