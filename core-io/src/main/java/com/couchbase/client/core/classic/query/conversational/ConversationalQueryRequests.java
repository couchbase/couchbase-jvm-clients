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

package com.couchbase.client.core.classic.query.conversational;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.msg.query.ConversationalQueryRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.topology.NodeIdentifier;
import reactor.util.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;

import static com.couchbase.client.core.util.Golang.encodeDurationToMs;

/**
 * Turns an encoded request body (see {@link ConversationalQueryEncoding}) into a request.
 */
@Stability.Internal
class ConversationalQueryRequests {
  private ConversationalQueryRequests() {
  }

  static Duration timeout(Core core, CoreCommonOptions common) {
    return common.timeout().orElse(core.context().environment().timeoutConfig().queryTimeout());
  }

  static ConversationalQueryRequest request(Core core,
                                            ObjectNode body,
                                            Duration timeout,
                                            CoreCommonOptions common,
                                            String spanName,
                                            boolean idempotent,
                                            @Nullable String chatId,
                                            @Nullable NodeIdentifier target) {
    RetryStrategy retryStrategy = common.retryStrategy().orElse(core.context().environment().retryStrategy());

    body.put("timeout", encodeDurationToMs(timeout));
    if (chatId != null) {
      body.put("natural_chatid", chatId);
    }
    if (!body.hasNonNull("client_context_id")) {
      body.put("client_context_id", UUID.randomUUID().toString());
    }
    String statement = body.get("statement").asText();

    RequestSpan span = core.context().coreResources()
        .requestTracer()
        .requestSpan(spanName, common.parentSpan().orElse(null));

    ConversationalQueryRequest request = new ConversationalQueryRequest(timeout, core.context(), retryStrategy,
        core.context().authenticator(), statement, body.toString().getBytes(StandardCharsets.UTF_8),
        idempotent, body.get("client_context_id").asText(), span, target);
    request.context().clientContext(common.clientContext());
    return request;
  }
}
