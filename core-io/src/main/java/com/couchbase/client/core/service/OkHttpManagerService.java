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
import com.couchbase.client.core.cnc.events.io.IdleStreamingEndpointClosedEvent;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.endpoint.http.CoreHttpRequest;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.io.netty.manager.NonChunkedManagerMessageHandler;
import com.couchbase.client.core.msg.BaseHttpRequest;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.manager.BucketConfigStreamingRequest;
import com.couchbase.client.core.msg.manager.BucketConfigStreamingResponse;
import com.couchbase.client.core.msg.manager.ManagerRequest;
import com.couchbase.client.core.util.HostAndPort;
import okhttp3.ResponseBody;
import okio.BufferedSource;
import okio.ByteString;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.Optional;


/**
 * The cluster manager service on one node, using OkHttp.
 * <p>
 * Handles {@link ManagerRequest}s here, including the long-lived bucket config stream;
 * everything else (admission, dispatch, failures, {@link CoreHttpRequest}s, diagnostics)
 * is in {@link AbstractOkHttpService}.
 * <p>
 * Like the Netty implementation, manager requests always complete with a response that has
 * the HTTP status; it's up to the caller to check it. Only {@link CoreHttpRequest}s are failed
 * for error statuses.
 */
@NullMarked
public class OkHttpManagerService extends AbstractOkHttpService {

  /**
   * Like the Netty implementation, which has at most this many endpoints (connections) per node.
   */
  private static final int MAX_IN_FLIGHT = 16;

  /**
   * Separates configs in the bucket config stream.
   */
  private static final ByteString CONFIG_SEPARATOR = ByteString.encodeUtf8("\n\n\n\n");

  public OkHttpManagerService(CoreContext context, HostAndPort address) {
    super(
      ServiceType.MANAGER,
      context,
      address,
      MAX_IN_FLIGHT,
      context.environment().ioConfig().managerCircuitBreakerConfig(),
      // The bucket config stream is closed if it's idle this long, so the refresher redials (maybe a different node).
      context.environment().ioConfig().configIdleRedialTimeout()
    );
  }

  @Override
  protected boolean supports(Request<?> request) {
    return request instanceof ManagerRequest;
  }

  @Override
  protected void dispatchServiceRequest(Request<?> request, DispatchAttempt attempt) {
    ManagerRequest<?> managerRequest = (ManagerRequest<?>) request;

    // Manager requests are GET requests with no body.
    okhttp3.Request.Builder requestBuilder = new okhttp3.Request.Builder()
      .url(url(managerRequest.path()));

    if (request instanceof BucketConfigStreamingRequest) {
      BucketConfigStreamingRequest streamingRequest = (BucketConfigStreamingRequest) request;
      execute(streamingRequest, requestBuilder, attempt, response -> handleConfigStream(streamingRequest, response, attempt));
    } else {
      sendManagerRequest((BaseHttpRequest<?>) request, requestBuilder, attempt);
    }
  }

  @Override
  protected Exception translateCoreHttpError(int httpStatus, String responseBody, CoreHttpRequest request) {
    return NonChunkedManagerMessageHandler.errorToThrowable(HttpResponseStatus.valueOf(httpStatus), responseBody, request);
  }

  private <R extends Response> void sendManagerRequest(
    BaseHttpRequest<R> request,
    okhttp3.Request.Builder requestBuilder,
    DispatchAttempt attempt
  ) {
    @SuppressWarnings("unchecked")
    ManagerRequest<R> managerRequest = (ManagerRequest<R>) request;

    execute(request, requestBuilder, attempt, response -> {
      try (ResponseBody responseBody = response.body()) {
        // Like the Netty implementation: whatever the status, the caller gets a response.
        R decoded = managerRequest.decode(response.code(), responseBody.bytes());
        attempt.recordOutcome(decoded, null);
        request.succeed(decoded);
      } catch (Throwable t) {
        attempt.recordFailure(t);
        request.fail(new DecodingFailureException("failed to process HTTP response", t));
      }
    });
  }

  /**
   * Completes the request as soon as the response headers arrive, then pushes each config
   * to the response as it arrives, until the server closes the stream, the connection fails,
   * or the stream is idle for too long. Then completes the stream (like the Netty implementation,
   * which completes the stream when the connection closes), so the refresher can redial.
   * <p>
   * Runs on the thread that received the response, for as long as the stream lasts.
   */
  private void handleConfigStream(BucketConfigStreamingRequest request, okhttp3.Response httpResponse, DispatchAttempt attempt) {
    BucketConfigStreamingResponse streamingResponse;
    try {
      streamingResponse = request.decode(httpResponse.code(), null);
    } catch (Throwable t) {
      httpResponse.close();
      attempt.recordFailure(t);
      request.fail(new DecodingFailureException("failed to process HTTP response", t));
      return;
    }

    attempt.recordOutcome(streamingResponse, null);
    request.succeed(streamingResponse);

    try (ResponseBody responseBody = httpResponse.body()) {
      // Like the Netty implementation: once the stream is open, close it if nothing arrives for a while.
      CouchbaseOkHttpClient.setSocketReadTimeout(httpResponse.request(), streamingReadTimeout());
      BufferedSource source = responseBody.source();

      while (true) {
        long separatorIndex = source.indexOf(CONFIG_SEPARATOR);
        if (separatorIndex == -1) {
          break; // end of stream (any partial config is discarded, like Netty)
        }
        String config = source.readUtf8(separatorIndex);
        source.skip(CONFIG_SEPARATOR.size());
        streamingResponse.pushConfig(config.trim());
      }

    } catch (SocketTimeoutException e) {
      context().environment().eventBus().publish(new IdleStreamingEndpointClosedEvent(ioContext(request.context())));

    } catch (IOException | RuntimeException e) {
      // For example, the connection failed or was closed. The refresher will redial.
      log.debug("Bucket config stream ended: {}", e.toString());

    } finally {
      streamingResponse.completeStream();
    }
  }

  private IoContext ioContext(RequestContext requestContext) {
    return new IoContext(
      context(),
      socketAddress(requestContext.lastDispatchedFrom()),
      socketAddress(requestContext.lastDispatchedTo()),
      Optional.empty()
    );
  }

  private static @Nullable InetSocketAddress socketAddress(@Nullable HostAndPort address) {
    return address == null ? null : InetSocketAddress.createUnresolved(address.host(), address.port());
  }
}
