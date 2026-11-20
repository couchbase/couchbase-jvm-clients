/*
 * Copyright (c) 2026 Couchbase, Inc.
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
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.topology.NodeIdentifier;

import java.time.Duration;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;

@Stability.Internal
public class ConversationalQueryRequest extends QueryRequest {

  public ConversationalQueryRequest(Duration timeout, CoreContext ctx, RetryStrategy retryStrategy,
                                    Authenticator authenticator, String statement, byte[] query,
                                    boolean idempotent, String contextId, RequestSpan span, NodeIdentifier target) {
    super(timeout, ctx, retryStrategy, authenticator, statement, query, idempotent, contextId, span, null, null, target, false);
  }

  @Override
  public String toString() {
    // The request body is deliberately excluded, as it may contain model credentials (natural_cred, natural_config).
    return "ConversationalQueryRequest{" +
        "statement='" + redactUser(statement()) + '\'' +
        ", contextId='" + operationId() + '\'' +
        '}';
  }
}
