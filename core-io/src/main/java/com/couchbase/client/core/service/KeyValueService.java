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
import com.couchbase.client.core.env.ServiceConfig;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.service.strategy.PartitionSelectionStrategy;

/**
 *
 */
public class KeyValueService extends PooledService {

  private static final EndpointSelectionStrategy STRATEGY = new PartitionSelectionStrategy();

  private final CoreContext coreContext;
  private final NetworkAddress hostname;
  private final int port;
  private final String username;
  private final String password;
  private final String bucketname;

  /**
   *
   * @param serviceConfig
   * @param coreContext
   * @param hostname
   * @param port
   * @param username
   * @param bucketname
   * @param password
   */
  public KeyValueService(final ServiceConfig serviceConfig, final CoreContext coreContext,
                         final NetworkAddress hostname, final int port, final String username,
                         final String bucketname, final String password) {
    super(serviceConfig, new ServiceContext(coreContext, hostname, port));
    this.coreContext = coreContext;
    this.hostname = hostname;
    this.port = port;
    this.bucketname = bucketname;
    this.username = username;
    this.password = password;
  }

  @Override
  protected Endpoint createEndpoint() {
    return new KeyValueEndpoint(coreContext, hostname, port, username, bucketname, password);
  }

  @Override
  protected EndpointSelectionStrategy selectionStrategy() {
    return STRATEGY;
  }
}
