package com.couchbase.client.core.service;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.ManagerEndpoint;
import com.couchbase.client.core.env.ServiceConfig;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.service.strategy.RoundRobinSelectionStrategy;

import java.time.Duration;

public class ManagerService extends PooledService {

  private final CoreContext coreContext;
  private final NetworkAddress hostname;
  private final int port;

  public ManagerService(CoreContext coreContext, final NetworkAddress hostname, final int port) {
    super(new ManagerServiceConfig(), new ServiceContext(coreContext, hostname, port));
    this.coreContext = coreContext;
    this.hostname = hostname;
    this.port = port;
  }

  @Override
  protected Endpoint createEndpoint() {
    return new ManagerEndpoint(coreContext, hostname, port);
  }

  @Override
  protected EndpointSelectionStrategy selectionStrategy() {
    return new RoundRobinSelectionStrategy();
  }

  static class ManagerServiceConfig implements ServiceConfig {
    @Override
    public int minEndpoints() {
      // TODO: set this to 0 once the actual dynamic pool is back implemented
      return 1;
    }

    @Override
    public int maxEndpoints() {
      return 8;
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
}
