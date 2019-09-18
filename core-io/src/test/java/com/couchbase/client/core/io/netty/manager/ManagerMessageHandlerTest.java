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

package com.couchbase.client.core.io.netty.manager;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.channel.embedded.EmbeddedChannel;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultHttpContent;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultHttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultLastHttpContent;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.deps.io.netty.util.ResourceLeakDetector;
import com.couchbase.client.core.endpoint.BaseEndpoint;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.msg.manager.BucketConfigStreamingRequest;
import com.couchbase.client.core.msg.manager.BucketConfigStreamingResponse;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.couchbase.client.test.Util.readResource;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ManagerMessageHandlerTest {

  static {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  private static final String USER = "Admin";
  private static final String PASS = "pass";

  private static CoreEnvironment ENV;

  @BeforeAll
  static void setup() {
    ENV = CoreEnvironment.create();
  }

  @AfterAll
  static void teardown() {
    ENV.shutdown();
  }

  /**
   * Configs can come in all shapes and sizes chunked, but only after the 4 newlines are pushed by the cluster
   * the config should be propagated into the flux.
   */
  @Test
  void returnsNewConfigsWhenChunked() throws Exception {
    CoreContext ctx = new CoreContext(mock(Core.class), 1, ENV, PasswordAuthenticator.create(USER, PASS));
    BaseEndpoint endpoint = mock(BaseEndpoint.class);
    EndpointContext endpointContext = mock(EndpointContext.class);
    when(endpointContext.environment()).thenReturn(ENV);
    when(endpoint.endpointContext()).thenReturn(endpointContext);
    EmbeddedChannel channel = new EmbeddedChannel(new ManagerMessageHandler(endpoint, ctx));

    BucketConfigStreamingRequest request = new BucketConfigStreamingRequest(Duration.ofSeconds(1), ctx,
      BestEffortRetryStrategy.INSTANCE, "bucket", ctx.authenticator());
    channel.write(request);

    HttpRequest outboundHeader = channel.readOutbound();
    assertEquals(HttpMethod.GET, outboundHeader.method());
    assertEquals("/pools/default/bs/bucket", outboundHeader.uri());
    assertEquals(HttpVersion.HTTP_1_1, outboundHeader.protocolVersion());

    CompletableFuture<BucketConfigStreamingResponse> response = request.response();

    assertFalse(response.isDone());

    HttpResponse inboundResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    channel.writeInbound(inboundResponse);

    BucketConfigStreamingResponse completedResponse = request.response().get();

    final List<String> configsPushed = Collections.synchronizedList(new ArrayList<>());
    final AtomicBoolean terminated = new AtomicBoolean(false);
    Thread listener = new Thread(() -> completedResponse.configs().subscribe(
      configsPushed::add,
      (e) -> {},
      () -> terminated.set(true))
    );
    listener.setDaemon(true);
    listener.start();

    ByteBuf fullContent = Unpooled.copiedBuffer(
      readResource("terse_stream_two_configs.json", ManagerMessageHandlerTest.class),
      StandardCharsets.UTF_8
    );

    while (fullContent.readableBytes() > 0) {
      int len = new Random().nextInt(fullContent.readableBytes() + 1);
      if (len == 0) {
        continue;
      }
      channel.writeInbound(new DefaultHttpContent(fullContent.readBytes(len)));
    }

    waitUntilCondition(() -> configsPushed.size() == 2);
    for (String config : configsPushed) {
      assertTrue(config.startsWith("{"));
      assertTrue(config.endsWith("}"));
    }

    assertFalse(terminated.get());
    channel.writeInbound(new DefaultLastHttpContent());
    waitUntilCondition(terminated::get);

    ReferenceCountUtil.release(fullContent);
    channel.close().awaitUninterruptibly();
  }

  /**
   * When a config stream is being processed and the underlying channel closes, the stream should also be closed
   * so the upper level can re-establish a new one.
   */
  @Test
  void closesStreamIfChannelClosed() throws Exception {
    CoreContext ctx = new CoreContext(mock(Core.class), 1, ENV,  PasswordAuthenticator.create(USER, PASS));
    BaseEndpoint endpoint = mock(BaseEndpoint.class);
    EndpointContext endpointContext = mock(EndpointContext.class);
    when(endpointContext.environment()).thenReturn(ENV);
    when(endpoint.endpointContext()).thenReturn(endpointContext);
    EmbeddedChannel channel = new EmbeddedChannel(new ManagerMessageHandler(endpoint, ctx));

    BucketConfigStreamingRequest request = new BucketConfigStreamingRequest(Duration.ofSeconds(1), ctx,
      BestEffortRetryStrategy.INSTANCE, "bucket", ctx.authenticator());
    channel.write(request);

    HttpRequest outboundHeader = channel.readOutbound();
    assertEquals(HttpMethod.GET, outboundHeader.method());
    assertEquals("/pools/default/bs/bucket", outboundHeader.uri());
    assertEquals(HttpVersion.HTTP_1_1, outboundHeader.protocolVersion());

    CompletableFuture<BucketConfigStreamingResponse> response = request.response();

    assertFalse(response.isDone());

    HttpResponse inboundResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    channel.writeInbound(inboundResponse);

    BucketConfigStreamingResponse completedResponse = request.response().get();

    final AtomicBoolean terminated = new AtomicBoolean(false);
    Thread listener = new Thread(() -> completedResponse.configs().subscribe(
      (v) -> {},
      (e) -> {},
      () -> terminated.set(true))
    );
    listener.setDaemon(true);
    listener.start();

    assertFalse(terminated.get());
    channel.close().awaitUninterruptibly();
    waitUntilCondition(terminated::get);
  }

}