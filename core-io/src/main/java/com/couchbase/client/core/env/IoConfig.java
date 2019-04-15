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

import com.couchbase.client.core.endpoint.CircuitBreaker;
import com.couchbase.client.core.endpoint.CircuitBreakerConfig;
import com.couchbase.client.core.service.ServiceType;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
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
    kvCircuitBreakerConfig = Optional
      .ofNullable(builder.kvCircuitBreakerConfig)
      .orElse(CircuitBreakerConfig.disabled());
    queryCircuitBreakerConfig = Optional
      .ofNullable(builder.queryCircuitBreakerConfig)
      .orElse(CircuitBreakerConfig.disabled());
    viewCircuitBreakerConfig = Optional
      .ofNullable(builder.viewCircuitBreakerConfig)
      .orElse(CircuitBreakerConfig.disabled());
    searchCircuitBreakerConfig = Optional
      .ofNullable(builder.searchCircuitBreakerConfig)
      .orElse(CircuitBreakerConfig.disabled());
    analyticsCircuitBreakerConfig = Optional
      .ofNullable(builder.analyticsCircuitBreakerConfig)
      .orElse(CircuitBreakerConfig.disabled());
    managerCircuitBreakerConfig = Optional
      .ofNullable(builder.managerCircuitBreakerConfig)
      .orElse(CircuitBreakerConfig.disabled());
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

  public static Builder kvCircuitBreakerConfig(CircuitBreakerConfig kvCircuitBreakerConfig) {
    return builder().kvCircuitBreakerConfig(kvCircuitBreakerConfig);
  }

  public static Builder queryCircuitBreakerConfig(CircuitBreakerConfig queryCircuitBreakerConfig) {
    return builder().queryCircuitBreakerConfig(queryCircuitBreakerConfig);
  }

  public static Builder viewCircuitBreakerConfig(CircuitBreakerConfig viewCircuitBreakerConfig) {
    return builder().viewCircuitBreakerConfig(viewCircuitBreakerConfig);
  }

  public static Builder searchCircuitBreakerConfig(CircuitBreakerConfig searchCircuitBreakerConfig) {
    return builder().searchCircuitBreakerConfig(searchCircuitBreakerConfig);
  }

  public static Builder analyticsCircuitBreakerConfig(CircuitBreakerConfig analyticsCircuitBreakerConfig) {
    return builder().analyticsCircuitBreakerConfig(analyticsCircuitBreakerConfig);
  }

  public static Builder managerCircuitBreakerConfig(CircuitBreakerConfig managerCircuitBreakerConfig) {
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

  public static class Builder {

    private Set<SaslMechanism> allowedSaslMechanisms;
    private boolean mutationTokensEnabled = DEFAULT_MUTATION_TOKENS_ENABLED;
    private Duration configPollInterval;
    private CircuitBreakerConfig kvCircuitBreakerConfig;
    private CircuitBreakerConfig queryCircuitBreakerConfig;
    private CircuitBreakerConfig viewCircuitBreakerConfig;
    private CircuitBreakerConfig searchCircuitBreakerConfig;
    private CircuitBreakerConfig analyticsCircuitBreakerConfig;
    private CircuitBreakerConfig managerCircuitBreakerConfig;
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
    public Builder kvCircuitBreakerConfig(CircuitBreakerConfig kvCircuitBreakerConfig) {
      this.kvCircuitBreakerConfig = kvCircuitBreakerConfig;
      return this;
    }

    /**
     * Configures a {@link CircuitBreaker} to use for query operations.
     *
     * @return this, for chaining
     */
    public Builder queryCircuitBreakerConfig(CircuitBreakerConfig queryCircuitBreakerConfig) {
      this.queryCircuitBreakerConfig = queryCircuitBreakerConfig;
      return this;
    }

    /**
     * Configures a {@link CircuitBreaker} to use for view operations.
     *
     * @return this, for chaining
     */
    public Builder viewCircuitBreakerConfig(CircuitBreakerConfig viewCircuitBreakerConfig) {
      this.viewCircuitBreakerConfig = viewCircuitBreakerConfig;
      return this;
    }

    /**
     * Configures a {@link CircuitBreaker} to use for search operations.
     *
     * @return this, for chaining
     */
    public Builder searchCircuitBreakerConfig(CircuitBreakerConfig searchCircuitBreakerConfig) {
      this.searchCircuitBreakerConfig = searchCircuitBreakerConfig;
      return this;
    }

    /**
     * Configures a {@link CircuitBreaker} to use for analytics operations.
     *
     * @return this, for chaining
     */
    public Builder analyticsCircuitBreakerConfig(CircuitBreakerConfig analyticsCircuitBreakerConfig) {
      this.analyticsCircuitBreakerConfig = analyticsCircuitBreakerConfig;
      return this;
    }

    /**
     * Configures a {@link CircuitBreaker} to use for manager operations.
     *
     * @return this, for chaining
     */
    public Builder managerCircuitBreakerConfig(CircuitBreakerConfig managerCircuitBreakerConfig) {
      this.managerCircuitBreakerConfig = managerCircuitBreakerConfig;
      return this;
    }

    public Builder captureTraffic(final ServiceType... serviceTypes) {
      this.captureTraffic = new HashSet<>(Arrays.asList(
        serviceTypes.length == 0 ? ServiceType.values() : serviceTypes
      ));
      return this;
    }
  }

}
