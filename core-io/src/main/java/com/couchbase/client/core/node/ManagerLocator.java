/*
 * Copyright (c) 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
