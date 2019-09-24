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

package com.couchbase.client.core.io.netty.query;

import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.endpoint.BaseEndpoint;
import com.couchbase.client.core.error.HttpStatusCodeException;
import com.couchbase.client.core.error.QueryException;
import com.couchbase.client.core.io.netty.NonChunkedHttpMessageHandler;
import com.couchbase.client.core.service.ServiceType;

import static java.nio.charset.StandardCharsets.UTF_8;

class NonChunkedQueryMessageHandler extends NonChunkedHttpMessageHandler {
  NonChunkedQueryMessageHandler(BaseEndpoint endpoint) {
    super(endpoint, ServiceType.QUERY);
  }

  @Override
  protected Exception failRequestWith(final HttpResponseStatus status, final String content) {
    return new QueryException(content.getBytes(UTF_8));
  }
}
