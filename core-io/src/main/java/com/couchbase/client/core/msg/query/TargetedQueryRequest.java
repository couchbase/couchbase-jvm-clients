/*
 * Copyright (c) 2020 Couchbase, Inc.
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
package com.couchbase.client.core.msg.query;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.msg.TargetedRequest;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;

@Stability.Volatile
public class TargetedQueryRequest extends QueryRequest implements TargetedRequest {
    private NodeIdentifier target;

    public TargetedQueryRequest(Duration timeout, CoreContext ctx, RetryStrategy retryStrategy, Authenticator authenticator,
                                String statement, byte[] query, boolean idempotent, String contextId, final RequestSpan parentSpan,
                                final String queryContext, NodeIdentifier target) {
        super(timeout, ctx, retryStrategy, authenticator, statement, query, idempotent, contextId, parentSpan, queryContext);
        this.target = target;
    }

    @Override
    public NodeIdentifier target() {
        return target;
    }
}
