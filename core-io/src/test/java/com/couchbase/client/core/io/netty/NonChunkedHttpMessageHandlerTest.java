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

package com.couchbase.client.core.io.netty;

import com.couchbase.client.core.deps.io.netty.channel.embedded.EmbeddedChannel;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultFullHttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpObjectAggregator;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.endpoint.BaseEndpoint;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.view.GenericViewRequest;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link NonChunkedHttpMessageHandler}.
 */
class NonChunkedHttpMessageHandlerTest {

  private static CoreEnvironment env;

  private EmbeddedChannel channel;
  private BaseEndpoint endpoint;

  @BeforeAll
  static void beforeAll() {
    env = CoreEnvironment.create("user", "pass");
  }

  @AfterAll
  static void afterAll() {
    env.shutdown();
  }

  @BeforeEach
  void setup() {
    endpoint = mock(BaseEndpoint.class);
    EndpointContext endpointContext = mock(EndpointContext.class);
    when(endpointContext.environment()).thenReturn(env);
    when(endpoint.endpointContext()).thenReturn(endpointContext);
    channel = new EmbeddedChannel();
  }

  @AfterEach
  void teardown() {
    channel.finishAndReleaseAll();
  }

  @Test
  void addsAggregatorWhenAdded() {
    assertNull(channel.pipeline().get(TestNonChunkedHttpMessageHandler.class));
    assertNull(channel.pipeline().get(HttpObjectAggregator.class));

    channel.pipeline().addFirst(
      NonChunkedHttpMessageHandler.IDENTIFIER,
      new TestNonChunkedHttpMessageHandler(endpoint)
    );

    assertNotNull(channel.pipeline().get(TestNonChunkedHttpMessageHandler.class));
    assertNotNull(channel.pipeline().get(HttpObjectAggregator.class));
  }

  @Test
  void removesAggregatorWhenRemoved() {
    channel.pipeline().addFirst(
      NonChunkedHttpMessageHandler.IDENTIFIER,
      new TestNonChunkedHttpMessageHandler(endpoint)
    );

    assertNotNull(channel.pipeline().get(TestNonChunkedHttpMessageHandler.class));
    assertNotNull(channel.pipeline().get(HttpObjectAggregator.class));

    channel.pipeline().remove(NonChunkedHttpMessageHandler.IDENTIFIER);

    assertNull(channel.pipeline().get(TestNonChunkedHttpMessageHandler.class));
    assertNull(channel.pipeline().get(HttpObjectAggregator.class));
  }

  @Test
  void callsMarkRequestCompletedOnceFinished() throws Exception {
    channel.pipeline().addFirst(
      NonChunkedHttpMessageHandler.IDENTIFIER,
      new TestNonChunkedHttpMessageHandler(endpoint)
    );
    channel.pipeline().fireChannelActive();

    GenericViewRequest request = new GenericViewRequest(
      Duration.ofSeconds(1),
      endpoint.endpointContext(),
      BestEffortRetryStrategy.INSTANCE,
      () -> new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
    );
    channel.writeAndFlush(request);

    FullHttpRequest written = channel.readOutbound();
    assertEquals("/", written.uri());

    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    channel.writeInbound(response);

    assertEquals(ResponseStatus.SUCCESS, request.response().get().status());
    verify(endpoint, times(1)).markRequestCompletion();
  }

  static class TestNonChunkedHttpMessageHandler extends NonChunkedHttpMessageHandler {

    TestNonChunkedHttpMessageHandler(BaseEndpoint endpoint) {
      super(endpoint, ServiceType.VIEWS);
    }

    @Override
    protected Exception failRequestWith(HttpResponseStatus status, String content) {
      return new Exception(content);
    }

  }

}
