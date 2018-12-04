package com.couchbase.client.core.node;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;

import java.util.List;

public class KeyValueLocator implements Locator {

  @Override
  public void dispatch(final Request<? extends Response> request, final List<Node> nodes,
                       final ClusterConfig config, final CoreContext ctx) {
    nodes.get(0).send(request);
    System.err.println("dispatch!" + nodes);
  }

}
