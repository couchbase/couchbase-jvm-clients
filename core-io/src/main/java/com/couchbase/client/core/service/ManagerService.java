package com.couchbase.client.core.service;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.ManagerEndpoint;
import com.couchbase.client.core.env.ServiceConfig;
import com.couchbase.client.core.io.NetworkAddress;

public class ManagerService extends PooledService {

  private final CoreContext coreContext;
  private final NetworkAddress hostname;
  private final int port;

  public ManagerService(ServiceConfig serviceConfig, CoreContext coreContext, final NetworkAddress hostname, final int port) {
    super(serviceConfig, new ServiceContext(coreContext, hostname, port));
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
    return null;
  }
}
