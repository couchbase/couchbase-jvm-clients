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
import com.couchbase.client.core.diag.ClusterState;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CommonOptions;

import java.util.Optional;
import java.util.Set;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * Allows to customize the diagnostics report.
 */
public class WaitUntilReadyOptions {

  /**
   * The service types to limit this diagnostics request to.
   */
  private Set<ServiceType> serviceTypes;

  /**
   * The desired cluster state to reach.
   */
  private ClusterState desiredState = ClusterState.ONLINE;

  /**
   * Creates a new set of {@link WaitUntilReadyOptions}.
   *
   * @return options to customize.
   */
  public static WaitUntilReadyOptions waitUntilReadyOptions() {
    return new WaitUntilReadyOptions();
  }

  private WaitUntilReadyOptions() {}

  /**
   * Allows to customize the set of services to wait for.
   * <p>
   * If no set is provided, all possible services are waited for.
   *
   * @param serviceTypes the service types that should be waited for.
   * @return the {@link WaitUntilReadyOptions} to allow method chaining.
   */
  public WaitUntilReadyOptions serviceTypes(final Set<ServiceType> serviceTypes) {
    notNullOrEmpty(serviceTypes, "Service Types");
    this.serviceTypes = serviceTypes;
    return this;
  }

  /**
   * Allows to customize the desired state to wait for.
   *
   * @param desiredState the state the sdk should wait for.
   * @return the {@link WaitUntilReadyOptions} to allow method chaining.
   */
  public WaitUntilReadyOptions desiredState(final ClusterState desiredState) {
    notNull(desiredState, "Desired State");
    if (desiredState == ClusterState.OFFLINE) {
      throw new IllegalArgumentException("Offline cannot be passed in as a state to wait for");
    }

    this.desiredState = desiredState;
    return this;
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  @Stability.Internal
  public class Built {

    public Set<ServiceType> serviceTypes() {
      return serviceTypes;
    }

    public ClusterState desiredState() {
      return desiredState;
    }
  }

}
