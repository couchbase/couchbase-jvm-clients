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

package com.couchbase.client.core.io.netty.query;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.Timer;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.RoleBasedCredentials;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.env.UserAgent;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceContext;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * These tests make sure that explicit backpressure is handled from the {@link QueryMessageHandler},
 * allowing consumers to ask for a specific number of rows to be emitted.
 */
class QueryMessageHandlerBackpressureTest {

  private ChunkServer chunkServer;
  private EventLoopGroup eventLoopGroup;

  @BeforeEach
  void beforeEach() {
    eventLoopGroup = new DefaultEventLoopGroup();
    chunkServer = new ChunkServer(eventLoopGroup);
  }

  @AfterEach
  void afterEach() {
    chunkServer.shutdown();
    eventLoopGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
  }

  /**
   * This test makes sure that even if the server returns a good bunch of data, each individual
   * chunk is requested by the caller explicitly.
   */
  @Test
  void requestRecordsExplicitly() throws Exception {
    ServiceContext serviceCtx = mock(ServiceContext.class);
    Bootstrap client = new Bootstrap()
      .channel(LocalChannel.class)
      .group(new DefaultEventLoopGroup())
      .remoteAddress(new LocalAddress("s1"))
      .handler(new ChannelInitializer<LocalChannel>() {
        @Override
        protected void initChannel(LocalChannel ch) {
          ch.pipeline().addLast(new HttpClientCodec(), new QueryMessageHandler(serviceCtx));
        }
      });

    Channel channel = client.connect().awaitUninterruptibly().channel();
    CoreContext ctx = mock(CoreContext.class);
    CoreEnvironment env = mock(CoreEnvironment.class);
    UserAgent agent = new UserAgent("name", "0.0.0", Optional.empty(), Optional.empty());
    when(serviceCtx.environment()).thenReturn(env);
    when(ctx.environment()).thenReturn(env);
    when(env.userAgent()).thenReturn(agent);
    when(env.timer()).thenReturn(Timer.create());
    when(env.timeoutConfig()).thenReturn(TimeoutConfig.create());

    final List<byte[]> rows = Collections.synchronizedList(new ArrayList<>());
    QueryRequest request = new QueryRequest(Duration.ofSeconds(1), ctx, mock(RetryStrategy.class),
            new RoleBasedCredentials("admin", "password"), "myquery".getBytes(CharsetUtil.UTF_8));
    channel.writeAndFlush(request);

    QueryResponse response = request.response().get();

    assertEquals(0, rows.size());
    StepVerifier.create(response.rows().map(String::new))
            .thenRequest(1)
            .expectNext("{\"foo\"}")
            .thenRequest(1)
            .expectNext("{\"bar\"}")
            .thenRequest(2)
            .expectNext("{\"faz\"}","{\"baz\"}")
            .thenRequest(4)
            .expectNext("{\"fazz\"}","{\"bazz\"}","{\"fizz\"}","{\"bizz\"}")
            .expectComplete()
            .verify();
  }


  static class ChunkServer {

    private final Channel channel;
    ChunkServer(EventLoopGroup eventLoopGroup) {
      ServerBootstrap server = new ServerBootstrap()
        .channel(LocalServerChannel.class)
        .group(eventLoopGroup)
        .localAddress(new LocalAddress("s1"))
        .childHandler(new ChannelInitializer<LocalChannel>() {
          @Override
          protected void initChannel(LocalChannel ch) {
            ch.pipeline().addLast(new HttpServerCodec(), new ChannelInboundHandlerAdapter() {

              @Override
              public void channelRead(ChannelHandlerContext ctx, Object msg) {
                ReferenceCountUtil.release(msg);
              }

              @Override
              public void channelReadComplete(ChannelHandlerContext ctx) {
                HttpResponse response = new DefaultHttpResponse(
                  HttpVersion.HTTP_1_1,
                  HttpResponseStatus.OK
                );
                response.headers().set(HttpHeaderNames.CONTENT_LENGTH, ("{\"results\": [{\"foo\"},{\"bar\"}," +
                        "{\"faz\"},{\"baz\"},{\"fazz\"},{\"bazz\"},{\"fizz\"},{\"bizz\"}]}").length());
                ctx.write(response);
                ctx.write(new DefaultHttpContent(
                  Unpooled.copiedBuffer("{\"results\": [{\"foo\"},", CharsetUtil.UTF_8)
                ));
                ctx.writeAndFlush(new DefaultHttpContent(
                        Unpooled.copiedBuffer("{\"bar\"},", CharsetUtil.UTF_8)
                ));
                ctx.writeAndFlush(new DefaultHttpContent(
                        Unpooled.copiedBuffer("{\"faz\"},{\"baz\"},", CharsetUtil.UTF_8)
                ));
                ctx.writeAndFlush(new DefaultLastHttpContent(
                        Unpooled.copiedBuffer("{\"fazz\"},{\"bazz\"},{\"fizz\"},{\"bizz\"}]}", CharsetUtil.UTF_8)
                ));
              }
            });
          }
        });

      this.channel = server.bind().awaitUninterruptibly().channel();
    }

    void shutdown() {
      channel.disconnect().awaitUninterruptibly();
    }

  }
}