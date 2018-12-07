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
import com.couchbase.client.core.env.Credentials;
import com.couchbase.client.core.env.RoleBasedCredentials;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.retry.RetryStrategy;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.util.Utils.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

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
    Bootstrap client = new Bootstrap()
      .channel(LocalChannel.class)
      .group(new DefaultEventLoopGroup())
      .remoteAddress(new LocalAddress("s1"))
      .handler(new ChannelInitializer<LocalChannel>() {
        @Override
        protected void initChannel(LocalChannel ch) {
          ch.pipeline().addLast(new HttpClientCodec(), new QueryMessageHandler());
        }
      });
    Channel channel = client.connect().awaitUninterruptibly().channel();

    final List<QueryResponse.QueryEvent> rows = Collections.synchronizedList(new ArrayList<>());
    QueryRequest request = new QueryRequest(
      Duration.ofSeconds(1),
      mock(CoreContext.class),
      mock(RetryStrategy.class),
      new RoleBasedCredentials("admin", "password"),
      "myquery".getBytes(CharsetUtil.UTF_8),
      new QueryResponse.QueryEventSubscriber() {
        @Override
        public void onNext(QueryResponse.QueryEvent row) {
          rows.add(row);
        }

        @Override
        public void onComplete() {

        }
      }
    );
    channel.writeAndFlush(request);

    QueryResponse response = request.response().get();

    assertEquals(0, rows.size());
    Thread.sleep(100);
    response.request(1);
    waitUntilCondition(() -> rows.size() == 1);
    response.request(1);
    waitUntilCondition(() -> rows.size() == 2);

    assertEquals(2, rows.size());
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
                response.headers().set(HttpHeaderNames.CONTENT_LENGTH, "{\"results\": [{},{}]}".length());
                ctx.write(response);
                ctx.write(new DefaultHttpContent(
                  Unpooled.copiedBuffer("{\"results\": [{},", CharsetUtil.UTF_8)
                ));
                ctx.writeAndFlush(new DefaultLastHttpContent(
                  Unpooled.copiedBuffer("{}]}", CharsetUtil.UTF_8)
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