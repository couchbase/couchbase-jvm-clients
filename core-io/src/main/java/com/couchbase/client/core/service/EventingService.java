/*
 * Copyright (c) 2021 Couchbase, Inc.
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
import com.couchbase.client.core.endpoint.EventingEndpoint;
import com.couchbase.client.core.service.strategy.RoundRobinSelectionStrategy;

import java.time.Duration;
import java.util.Optional;

public class EventingService extends PooledService {

  private final String hostname;
  private final int port;

  public EventingService(CoreContext coreContext, final String hostname, final int port) {
    super(new EventingServiceConfig(), new ServiceContext(coreContext, hostname, port, ServiceType.EVENTING, Optional.empty()));
    this.hostname = hostname;
    this.port = port;
  }

  @Override
  protected Endpoint createEndpoint() {
    return new EventingEndpoint(serviceContext(), hostname, port);
  }

  @Override
  protected EndpointSelectionStrategy selectionStrategy() {
    return new RoundRobinSelectionStrategy();
  }

  static class EventingServiceConfig implements ServiceConfig {
    @Override
    public int minEndpoints() {
      return 0;
    }

    @Override
    public int maxEndpoints() {
      return 16;
    }

    @Override
    public Duration idleTime() {
      return Duration.ofSeconds(60);
    }

    @Override
    public boolean pipelined() {
      return false;
    }
  }

  @Override
  public ServiceType type() {
    return ServiceType.EVENTING;
  }
}
