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

package com.couchbase.client.core.service;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.KeyValueEndpoint;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.service.strategy.PartitionSelectionStrategy;

import java.util.Optional;
/**
 *
 */
public class KeyValueService extends PooledService {

  private static final EndpointSelectionStrategy STRATEGY = new PartitionSelectionStrategy();

  private final String hostname;
  private final int port;
  private final Optional<String> bucketName;
  private final Authenticator authenticator;

  public KeyValueService(final ServiceConfig serviceConfig, final CoreContext coreContext,
                         final String hostname, final int port, final Optional<String> bucketName,
                         final Authenticator authenticator) {
    super(
      inspectServiceConfig(serviceConfig, bucketName.isPresent()),
      new ServiceContext(coreContext, hostname, port, ServiceType.KV, bucketName)
    );

    this.hostname = hostname;
    this.port = port;
    this.bucketName = bucketName;
    this.authenticator = authenticator;
  }

  /**
   * Inspects the service config and makes sure that for GCCCP, we only ever open one endpoint.
   * <p>
   * Without this check, if a user opens 15 kv connections per node for throughput reasons we would also open 15
   * additional ones for GCCCP which is not needed at all just to fetch configs.
   *
   * @param original the input service config from the user.
   * @param bucketPresent indicating if it's a regular bucket connection or for gcccp.
   * @return either the original config or the one for gcccp forcing it to 1 endpoint.
   */
  private static ServiceConfig inspectServiceConfig(final ServiceConfig original, final boolean bucketPresent) {
    return bucketPresent ? original : KeyValueServiceConfig.endpoints(1).build();
  }

  @Override
  protected Endpoint createEndpoint() {
    return new KeyValueEndpoint(serviceContext(), hostname, port, bucketName, authenticator);
  }

  @Override
  protected EndpointSelectionStrategy selectionStrategy() {
    return STRATEGY;
  }

  @Override
  public ServiceType type() {
    return ServiceType.KV;
  }
}
