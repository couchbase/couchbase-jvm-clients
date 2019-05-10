package com.couchbase.client.core.node;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.TargetedRequest;

import java.util.List;

public class ManagerLocator implements Locator {

  @Override
  public void dispatch(final Request<? extends Response> request,
                       final List<Node> nodes, final ClusterConfig config, final CoreContext ctx) {
    if (request instanceof TargetedRequest) {
      for (Node n : nodes) {
        if (n.identifier().equals(((TargetedRequest) request).target())) {
          n.send(request);
        }
      }
      // toDO: not found (also check for connected?) .. retry?
    } else {
      throw new UnsupportedOperationException("not yet implemented");
    }
  }
}
