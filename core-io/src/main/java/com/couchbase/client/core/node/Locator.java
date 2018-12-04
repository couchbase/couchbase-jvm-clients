/*
 * Copyright (c) 2016 Couchbase, Inc.
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

import java.util.List;

public interface Locator {

    /**
     * Given the environment and node information, the implementation locates the right set of
     * nodes and dispatches the request into them.
     *
     * @param request the request to dispatch.
     * @param nodes the current list of active nodes.
     * @param config the current cluster configuration.
     * @param ctx the core context.
     */
    void dispatch(Request<? extends Response> request, List<Node> nodes, ClusterConfig config, CoreContext ctx);
}
