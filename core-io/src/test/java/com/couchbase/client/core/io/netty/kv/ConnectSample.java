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
import com.couchbase.client.core.env.CompressionConfig;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.IoEnvironment;
import com.couchbase.client.core.env.SaslMechanism;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.msg.kv.GetRequest;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.CharsetUtil;

import java.time.Duration;
import java.util.EnumSet;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This will go away, it is just to demo the kv connect steps in a
 * holistic picture until it is complete.
 */
public class ConnectSample {

  public static void main(String... args) throws Exception {
    DefaultEventBus eventBus = DefaultEventBus.create();
    IoEnvironment ioEnv = mock(IoEnvironment.class);
    when(ioEnv.connectTimeout()).thenReturn(Duration.ofSeconds(1));
    CoreEnvironment coreConfig = mock(CoreEnvironment.class);
    when(coreConfig.eventBus()).thenReturn(eventBus);
    when(coreConfig.userAgent()).thenReturn("core-io");
    when(coreConfig.ioEnvironment()).thenReturn(ioEnv);
    CompressionConfig cc = mock(CompressionConfig.class);
    SecurityConfig sc = mock(SecurityConfig.class);
    when(cc.enabled()).thenReturn(true);
    when(sc.certAuthEnabled()).thenReturn(false);
    when(ioEnv.compressionConfig()).thenReturn(cc);
    when(ioEnv.securityConfig()).thenReturn(sc);
    when(ioEnv.allowedSaslMechanisms()).thenReturn(EnumSet.of(SaslMechanism.PLAIN));

    eventBus.subscribe(LoggingEventConsumer.builder()
      .disableSlf4J(true)
      .fallbackToConsole(true)
      .build());
    eventBus.start();

    final CoreContext ctx = new CoreContext(1234, coreConfig);
    final Duration timeout = Duration.ofSeconds(1);


    ChannelFuture connect = new Bootstrap()
      .group(new NioEventLoopGroup())
      .channel(NioSocketChannel.class)
      .remoteAddress("127.0.0.1", 11210)
      .handler(new KeyValueChannelInitializer(ctx, "travel-sample", "Administrator", "password"))
      .connect();

    connect.awaitUninterruptibly().addListeners(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        System.err.println(ConnectTimings.export(future.channel()));
        if (future.isSuccess()) {
          GetRequest request = new GetRequest("airline_10".getBytes(CharsetUtil.UTF_8), timeout, null);
          future.channel().writeAndFlush(request);
          request.response().whenComplete((getResponse, throwable) -> System.err.println(getResponse));
        } else {
          System.err.println(future.cause());
        }
      }
    });


    Thread.sleep(100000);
  }
}
