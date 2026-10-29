/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.service;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.endpoint.http.CoreHttpRequest;
import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.EventingFunctionCompilationFailureException;
import com.couchbase.client.core.error.EventingFunctionDeployedException;
import com.couchbase.client.core.error.EventingFunctionIdenticalKeyspaceException;
import com.couchbase.client.core.error.EventingFunctionNotBootstrappedException;
import com.couchbase.client.core.error.EventingFunctionNotDeployedException;
import com.couchbase.client.core.error.EventingFunctionNotFoundException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.ScopeNotFoundException;
import com.couchbase.client.core.error.context.EventingErrorContext;
import com.couchbase.client.core.io.netty.HttpProtocol;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.util.HostAndPort;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.Locale;
import java.util.Map;

/**
 * The eventing service on one node, using OkHttp.
 * <p>
 * Handles only {@link CoreHttpRequest}s (for example, eventing function
 * management), which {@link AbstractOkHttpService} takes care of. All this class adds is how
 * the service's error responses become exceptions.
 */
@NullMarked
public class OkHttpEventingService extends AbstractOkHttpService {

  /**
   * The most requests in flight (so connections) per node.
   */
  private static final int MAX_IN_FLIGHT = 16;

  public OkHttpEventingService(CoreContext context, HostAndPort address) {
    super(
      ServiceType.EVENTING,
      context,
      address,
      MAX_IN_FLIGHT,
      context.environment().ioConfig().eventingCircuitBreakerConfig()
    );
  }

  @Override
  protected boolean supports(Request<?> request) {
    return false; // only CoreHttpRequests, which every service supports
  }

  @Override
  protected void dispatchServiceRequest(Request<?> request, DispatchAttempt attempt) {
    // Not called, since supports() is always false.
    throw new IllegalStateException("Unsupported request type: " + request);
  }

  @Override
  protected Exception translateCoreHttpError(int httpStatus, String responseBody, CoreHttpRequest request) {
    return errorToThrowable(HttpResponseStatus.valueOf(httpStatus), responseBody, request);
  }

  /**
   * Returns the exception for an error response to an HTTP request to the eventing service.
   */
  static Exception errorToThrowable(HttpResponseStatus status, String content, Request<?> request) {
    final Map<String, Object> properties = extractProperties(content);

    EventingErrorContext errorContext = new EventingErrorContext(
      HttpProtocol.decodeStatus(status),
      request.context(),
      status.code(),
      properties
    );


    if (content != null) {
      if (content.contains("ERR_APP_NOT_FOUND_TS") || content.contains("ERR_APP_NOT_FOUND")) { // could search for just the one without the _TS suffix, but let's mention both in case we change how we match in the future.
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
        if (content.toLowerCase(Locale.ROOT).contains("scope not defined")) {
          return new ScopeNotFoundException("", errorContext);
        }
        return new BucketNotFoundException("", errorContext);
      }
    }

    if (status.equals(HttpResponseStatus.BAD_REQUEST)) {
      return new InvalidArgumentException("Eventing service reported an invalid argument: " + content, null, errorContext);
    }

    return new CouchbaseException("Unknown eventing error: " + content, errorContext);
  }

  /**
   * If eventing returns the content as JSON, we can extract the properties and include in the error context.
   *
   * @param content the raw content, might be JSON.
   * @return the extracted properties, or null if not extractable.
   */
  private static @Nullable Map<String, Object> extractProperties(String content) {
    try {
      return Mapper.decodeInto(content, new TypeReference<Map<String, Object>>() {
      });
    } catch (Exception ex) {
      // Ignored on purpose since this is just best effort.
      return null;
    }
  }
}
