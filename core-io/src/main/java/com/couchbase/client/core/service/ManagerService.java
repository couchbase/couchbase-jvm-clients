package com.couchbase.client.core.service;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.ManagerEndpoint;
import com.couchbase.client.core.env.ServiceConfig;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.service.strategy.RoundRobinSelectionStrategy;

import java.time.Duration;
import java.util.Optional;

public class ManagerService extends PooledService {

  private final NetworkAddress hostname;
  private final int port;

  public ManagerService(CoreContext coreContext, final NetworkAddress hostname, final int port) {
    super(new ManagerServiceConfig(), new ServiceContext(coreContext, hostname, port, ServiceType.MANAGER, Optional.empty()));
    this.hostname = hostname;
    this.port = port;
  }

  @Override
  protected Endpoint createEndpoint() {
    return new ManagerEndpoint(serviceContext(), hostname, port);
  }

  @Override
  protected EndpointSelectionStrategy selectionStrategy() {
    return new RoundRobinSelectionStrategy();
  }

  static class ManagerServiceConfig implements ServiceConfig {
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
    return ServiceType.MANAGER;
  }
}
