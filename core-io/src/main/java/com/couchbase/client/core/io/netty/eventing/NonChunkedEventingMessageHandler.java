/*
 * Copyright (c) 2021 Couchbase, Inc.
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

package com.couchbase.client.core.io.netty.eventing;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.endpoint.BaseEndpoint;
import com.couchbase.client.core.error.*;
import com.couchbase.client.core.error.context.EventingErrorContext;
import com.couchbase.client.core.io.netty.HttpProtocol;
import com.couchbase.client.core.io.netty.NonChunkedHttpMessageHandler;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.NonChunkedHttpRequest;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.service.ServiceType;

import java.util.Map;

public class NonChunkedEventingMessageHandler extends NonChunkedHttpMessageHandler {

  public NonChunkedEventingMessageHandler(final BaseEndpoint endpoint) {
    super(endpoint, ServiceType.EVENTING);
  }

  @Override
  protected Exception failRequestWith(final HttpResponseStatus status, final String content,
                                      final NonChunkedHttpRequest<Response> request) {
    final Map<String, Object> properties = extractProperties(content);

    EventingErrorContext errorContext = new EventingErrorContext(
      HttpProtocol.decodeStatus(status),
      request.context(),
      status.code(),
      properties
    );


    if (content != null) {
      if (content.contains("ERR_APP_NOT_FOUND_TS")) {
        return new EventingFunctionNotFoundException("Eventing function not found", errorContext);
      } else if (content.contains("ERR_APP_NOT_DEPLOYED")) {
        return new EventingFunctionNotDeployedException("Eventing function not deployed", errorContext);
      } else if (content.contains("ERR_HANDLER_COMPILATION")) {
        return new EventingFunctionCompilationFailureException("Eventing function failed to compile", errorContext);
      } else if (content.contains("ERR_COLLECTION_MISSING")) {
        // The name can be derived from the error context, it's hard to extract it from here.
        return new CollectionNotFoundException("", errorContext);
      } else if (content.contains("ERR_SRC_MB_SAME")) {
        return new EventingFunctionIdenticalKeyspaceException("The source keyspace and the metadata keyspace are the same", errorContext);
      } else if (content.contains("ERR_APP_NOT_BOOTSTRAPPED")) {
        return new EventingFunctionNotBootstrappedException("Eventing function is not bootstrapped yet", errorContext);
      } else if (content.contains("ERR_APP_NOT_UNDEPLOYED")) {
        return new EventingFunctionDeployedException("Eventing function deployed", errorContext);
      } else if (content.contains("ERR_BUCKET_MISSING")) {
        return new BucketNotFoundException("", errorContext);
      }
    }

    return new CouchbaseException("Unknown eventing error: " + content, errorContext);
  }

  /**
   * If eventing returns the content as JSON, we can extract the properties and include in the error context.
   *
   * @param content the raw content, might be JSON.
   * @return the extracted properties, or null if not extractable.
   */
  private static Map<String, Object> extractProperties(final String content) {
    try {
      return Mapper.decodeInto(content, new TypeReference<Map<String, Object>>() {
      });
    } catch (Exception ex) {
      // Ignored on purpose since this is just best effort.
      return null;
    }
  }

}