/*
 * Copyright 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.io.netty.analytics;

import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.endpoint.BaseEndpoint;
import com.couchbase.client.core.io.netty.NonChunkedHttpMessageHandler;
import com.couchbase.client.core.msg.NonChunkedHttpRequest;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.service.ServiceType;

import static java.nio.charset.StandardCharsets.UTF_8;

class NonChunkedAnalyticsMessageHandler extends NonChunkedHttpMessageHandler {
  NonChunkedAnalyticsMessageHandler(BaseEndpoint endpoint) {
    super(endpoint, ServiceType.ANALYTICS);
  }

  @Override
  protected Exception failRequestWith(HttpResponseStatus status, String content, NonChunkedHttpRequest<Response> request) {
   return AnalyticsChunkResponseParser.errorsToThrowable(content.getBytes(UTF_8), request.context(), status);
  }
}
