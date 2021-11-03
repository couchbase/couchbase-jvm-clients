/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core.io.netty.search;

import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.endpoint.BaseEndpoint;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.IndexExistsException;
import com.couchbase.client.core.error.IndexNotFoundException;
import com.couchbase.client.core.error.context.SearchErrorContext;
import com.couchbase.client.core.io.netty.HttpProtocol;
import com.couchbase.client.core.io.netty.NonChunkedHttpMessageHandler;
import com.couchbase.client.core.msg.NonChunkedHttpRequest;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.service.ServiceType;

class NonChunkedSearchMessageHandler extends NonChunkedHttpMessageHandler {

  NonChunkedSearchMessageHandler(BaseEndpoint endpoint) {
    super(endpoint, ServiceType.SEARCH);
  }

  @Override
  protected Exception failRequestWith(HttpResponseStatus status, String content, NonChunkedHttpRequest<Response> request) {
    SearchErrorContext errorContext = new SearchErrorContext(
      HttpProtocol.decodeStatus(status),
      request.context(),
      status.code(),
      content
    );

    if (status == HttpResponseStatus.BAD_REQUEST && content.contains("index missing for update")) {
      return IndexNotFoundException.withMessageAndErrorContext("The index has not been found during an update on upsert. " +
        "If you did not intend to perform an index update, remove (or null out) the UUID property on the index.", errorContext);
    } else if (status == HttpResponseStatus.BAD_REQUEST && content.contains("index not found")) {
      return IndexNotFoundException.withMessageAndErrorContext("Index not found", errorContext);
    } else if (status == HttpResponseStatus.BAD_REQUEST && content.contains("index with the same name already exists")) {
      return new IndexExistsException("The index already exists. If you meant to replace/update it, make sure " +
        "that the UUID property is set.", errorContext);
    } else if (content.contains("Page not found")) {

    }

    return new CouchbaseException("Unknown search error: " + content, errorContext);
  }

}
