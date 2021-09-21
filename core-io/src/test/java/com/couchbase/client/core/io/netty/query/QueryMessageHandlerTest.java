/*
 * Copyright (c) 2019 Couchbase, Inc.
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
package com.couchbase.client.core.io.netty.query;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.channel.embedded.EmbeddedChannel;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultHttpContent;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultHttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultLastHttpContent;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpContent;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.LastHttpContent;
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.deps.io.netty.util.ResourceLeakDetector;
import com.couchbase.client.core.endpoint.BaseEndpoint;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.endpoint.NoopCircuitBreaker;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.HostAndPort;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.couchbase.client.test.Util.readResource;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Verifies the functionality of the {@link QueryMessageHandler}.
 */
class QueryMessageHandlerTest {

  static {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  private static CoreEnvironment ENV;
  private static CoreContext CORE_CTX;
  private static EndpointContext ENDPOINT_CTX;

  @BeforeAll
  static void setup() {
    ENV = CoreEnvironment.create();
    CORE_CTX = new CoreContext(mock(Core.class), 1, ENV, PasswordAuthenticator.create("user", "pass"));
    ENDPOINT_CTX = new EndpointContext(CORE_CTX, new HostAndPort("127.0.0.1", 1234),
      NoopCircuitBreaker.INSTANCE, ServiceType.QUERY, Optional.empty(), Optional.empty(), Optional.empty());
  }

  @AfterAll
  static void teardown() {
    ENV.shutdown();
  }

  /**
   * To avoid accidentally pipelining requests, we need to make sure that the handler only reports completion
   * once the streaming is fully completed, and not just the response completable future initially completed.
   *
   * <p>We send a request, make sure it is encoded correctly and then stream back the chunks, making sure the
   * final completion is only signaled once the last chunk arrived.</p>
   */
  @Test
  void reportsCompletionOnlyOnceStreamingEnds() {
    BaseEndpoint endpoint = mock(BaseEndpoint.class);
    EmbeddedChannel channel = new EmbeddedChannel(new QueryMessageHandler(endpoint, ENDPOINT_CTX));

    byte[] query = "doesn'tmatter".getBytes(CharsetUtil.UTF_8);
    QueryRequest request = new QueryRequest(
      ENV.timeoutConfig().queryTimeout(), CORE_CTX, ENV.retryStrategy(), CORE_CTX.authenticator(), "statement", query, false, null, null, null,
    null, null);
    channel.writeAndFlush(request);

    FullHttpRequest encodedRequest = channel.readOutbound();
    assertEquals("doesn'tmatter", encodedRequest.content().toString(CharsetUtil.UTF_8));
    ReferenceCountUtil.release(encodedRequest);

    verify(endpoint, never()).markRequestCompletion();
    assertFalse(request.response().isDone());


    ByteBuf fullResponse = Unpooled.copiedBuffer(
      readResource("success_response.json", QueryMessageHandlerTest.class),
      CharsetUtil.UTF_8
    );

    // send the header back first
    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

    channel.writeInbound(response);
    assertFalse(request.response().isDone());

    // send first chunk, should complete the future but not mark the endpoint completion
    HttpContent firstChunk = new DefaultHttpContent(fullResponse.readBytes(500));
    channel.writeInbound(firstChunk);

    assertTrue(request.response().isDone());
    verify(endpoint, never()).markRequestCompletion();

    // send the second chunk, no change
    HttpContent secondChunk = new DefaultHttpContent(fullResponse.readBytes(500));
    channel.writeInbound(secondChunk);

    verify(endpoint, never()).markRequestCompletion();

    // send the last chunk, mark for completion finally
    LastHttpContent lastChunk = new DefaultLastHttpContent(fullResponse.readBytes(fullResponse.readableBytes()));
    channel.writeInbound(lastChunk);

    verify(endpoint, times(1)).markRequestCompletion();

    channel.finishAndReleaseAll();
  }

  @Test
  void handlesConcurrentInFlightRequest() {
    BaseEndpoint endpoint = mock(BaseEndpoint.class);
    EmbeddedChannel channel = new EmbeddedChannel(new QueryMessageHandler(endpoint, ENDPOINT_CTX));

    byte[] query = "doesn'tmatter".getBytes(CharsetUtil.UTF_8);
    QueryRequest request1 = new QueryRequest(
      ENV.timeoutConfig().queryTimeout(), CORE_CTX, FailFastRetryStrategy.INSTANCE, CORE_CTX.authenticator(), "statement", query,
      true, null, null, null, null, null
    );
    QueryRequest request2 = new QueryRequest(
      ENV.timeoutConfig().queryTimeout(), CORE_CTX, FailFastRetryStrategy.INSTANCE, CORE_CTX.authenticator(), "statement", query,
      true, null, null, null, null, null
    );
    channel.writeAndFlush(request1);
    channel.writeAndFlush(request2);

    waitUntilCondition(() -> request2.context().retryAttempts() > 0);
    verify(endpoint, times(1)).decrementOutstandingRequests();
    channel.finishAndReleaseAll();
  }

}
