package com.couchbase.client.core.service.strategy;

import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.service.EndpointSelectionStrategy;

import java.util.List;

public class RoundRobinSelectionStrategy implements EndpointSelectionStrategy {

  @Override
  public <R extends Request<? extends Response>> Endpoint select(R request, List<Endpoint> endpoints) {
    // TODO: FIXME
    return endpoints.get(0);
  }
}
