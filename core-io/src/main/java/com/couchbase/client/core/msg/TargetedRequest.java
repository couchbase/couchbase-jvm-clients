package com.couchbase.client.core.msg;

import com.couchbase.client.core.node.NodeIdentifier;

public interface TargetedRequest {

  NodeIdentifier target();

}
