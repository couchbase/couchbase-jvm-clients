/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.java.diagnostics;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;

import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * Allows to customize a cluster or bucket level ping operation.
 */
public class PingOptions {

  /**
   * Holds a custom report id.
   */
  private Optional<String> reportId = Optional.empty();

  /**
   * The service types to limit this diagnostics request to.
   */
  private Set<ServiceType> serviceTypes;

  /**
   * The timeout for the operation, if set.
   */
  private Optional<Duration> timeout = Optional.empty();

  /**
   * The custom retry strategy, if set.
   */
  private Optional<RetryStrategy> retryStrategy = Optional.empty();

  /**
   * Creates a new set of {@link PingOptions}.
   *
   * @return options to customize.
   */
  public static PingOptions pingOptions() {
    return new PingOptions();
  }

  private PingOptions() {}

  /**
   * Sets a custom report ID that will be used in the report.
   * <p>
   * If no report ID is provided, the client will generate a unique one.
   *
   * @param reportId the report ID that should be used for this report.
   * @return the {@link PingOptions} to allow method chaining.
   */
  public PingOptions reportId(final String reportId) {
    this.reportId = Optional.of(reportId);
    return this;
  }

  /**
   * Allows to customize the set of services to ping
   * <p>
   * If no set is provided, all possible services are pinged
   *
   * @param serviceTypes the service types that should be pinged.
   * @return the {@link PingOptions} to allow method chaining.
   */
  public PingOptions serviceTypes(final Set<ServiceType> serviceTypes) {
    notNullOrEmpty(serviceTypes, "Service Types");
    this.serviceTypes = serviceTypes;
    return this;
  }

  /**
   * Specifies a custom per-operation timeout.
   *
   * <p>Note: if a custom timeout is provided through this builder, it will override the default set
   * on the environment.</p>
   *
   * @param timeout the timeout to use for this operation.
   * @return this options builder for chaining purposes.
   */
  public PingOptions timeout(final Duration timeout) {
    this.timeout = Optional.ofNullable(timeout);
    return this;
  }

  /**
   * Specifies a custom {@link RetryStrategy} for this operation.
   *
   * <p>Note: if a custom strategy is provided through this builder, it will override the default set
   * on the environment.</p>
   *
   * @param retryStrategy the retry strategy to use for this operation.
   * @return this options builder for chaining purposes.
   */
  public PingOptions retryStrategy(final RetryStrategy retryStrategy) {
    this.retryStrategy = Optional.ofNullable(retryStrategy);
    return this;
  }


  @Stability.Internal
  public Built build() {
    return new Built();
  }

  @Stability.Internal
  public class Built {

    Built() { }

    public Optional<String> reportId() {
      return reportId;
    }

    /**
     * Returns the custom timeout if provided.
     */
    public Optional<Duration> timeout() {
      return timeout;
    }

    public Set<ServiceType> serviceTypes() {
      return serviceTypes;
    }

    /**
     * Returns the custom retry strategy if provided.
     */
    public Optional<RetryStrategy> retryStrategy() {
      return retryStrategy;
    }
  }


}
