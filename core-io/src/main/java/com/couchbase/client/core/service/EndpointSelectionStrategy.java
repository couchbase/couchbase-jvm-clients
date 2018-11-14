package com.couchbase.client.core.service;

import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;

import java.util.List;

public interface EndpointSelectionStrategy {

  /**
   * Selects an {@link Endpoint} for the given {@link Request}.
   *
   * <p>If null is returned, it means that no endpoint could be selected and it is up to the
   * calling party to decide what to do next.</p>
   *
   * @param request the input request.
   * @param endpoints all the available endpoints.
   * @return the selected endpoint.
   */
  <R extends Request<? extends Response>> Endpoint select(R request, List<Endpoint> endpoints);

}
