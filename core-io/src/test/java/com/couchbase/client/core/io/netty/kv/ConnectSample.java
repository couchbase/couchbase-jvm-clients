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

package com.couchbase.client.core.io.netty.kv;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.DefaultEventBus;
import com.couchbase.client.core.cnc.LoggingEventConsumer;
import com.couchbase.client.core.env.CoreEnvironment;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This will go away, it is just to demo the kv connect steps in a
 * holistic picture until it is complete.
 */
public class ConnectSample {

  public static void main(String... args) throws Exception {
    DefaultEventBus eventBus = DefaultEventBus.create();
    CoreEnvironment coreConfig = mock(CoreEnvironment.class);
    when(coreConfig.eventBus()).thenReturn(eventBus);
    when(coreConfig.userAgent()).thenReturn("core-io");

    eventBus.subscribe(LoggingEventConsumer.builder()
      .disableSlf4J(true)
      .build());
    eventBus.start();

    final CoreContext ctx = new CoreContext(1234, coreConfig);
    final Duration timeout = Duration.ofSeconds(1);

    final Set<ServerFeature> features = new HashSet<>(Collections.singletonList(
      ServerFeature.SELECT_BUCKET
    ));

    ChannelFuture connect = new Bootstrap()
      .group(new NioEventLoopGroup())
      .channel(NioSocketChannel.class)
      .remoteAddress("127.0.0.1", 11210)
      .handler(new ChannelInitializer<Channel>() {
        @Override
        protected void initChannel(Channel ch)  {
          ch.pipeline()
            .addLast(new MemcacheProtocolDecoder())
            .addLast(new LoggingHandler(LogLevel.TRACE))
            .addLast(new FeatureNegotiatingHandler(ctx, features))
            .addLast(new ErrorMapLoadingHandler(ctx));
        }
      })
      .connect();

    connect.awaitUninterruptibly();
    Thread.sleep(100000);
  }
}
