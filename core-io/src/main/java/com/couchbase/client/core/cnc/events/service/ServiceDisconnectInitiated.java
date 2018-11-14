package com.couchbase.client.core.cnc.events.service;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.service.ServiceContext;

import java.time.Duration;

public class ServiceDisconnectInitiated extends AbstractEvent {

  private final int disconnectingEndpoints;

  public ServiceDisconnectInitiated(final ServiceContext context, final int disconnectingEndpoints) {
    super(Severity.DEBUG, Category.SERVICE, Duration.ZERO, context);
    this.disconnectingEndpoints = disconnectingEndpoints;
  }

  public int disconnectingEndpoints() {
    return disconnectingEndpoints;
  }

  @Override
  public String description() {
    return "Starting to disconnect service with " + disconnectingEndpoints
      + " underlying endpoints";
  }
}
