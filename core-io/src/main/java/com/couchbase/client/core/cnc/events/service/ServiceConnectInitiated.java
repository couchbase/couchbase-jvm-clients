package com.couchbase.client.core.cnc.events.service;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.service.ServiceContext;

import java.time.Duration;

public class ServiceConnectInitiated extends AbstractEvent {

  private final int minimumEndpoints;

  public ServiceConnectInitiated(final ServiceContext context, final int minimumEndpoints) {
    super(Severity.DEBUG, Category.SERVICE, Duration.ZERO, context);
    this.minimumEndpoints = minimumEndpoints;
  }

  public int minimumEndpoints() {
    return minimumEndpoints;
  }

  @Override
  public String description() {
    return "Starting to connect service with " + minimumEndpoints + " minimum endpoints";
  }
}
