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
import com.couchbase.client.core.cnc.LoggingEventConsumer;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.msg.kv.GetRequest;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.DefaultSelectStrategyFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SelectStrategyFactory;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.CharsetUtil;
import io.netty.util.ResourceLeakDetector;

import java.time.Duration;


/**
 * This will go away, it is just to demo the kv connect steps in a
 * holistic picture until it is complete.
 */
public class ConnectSample {

  static {
   ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  public static void main(String... args) throws Exception {
    CoreEnvironment environment = CoreEnvironment.create();

    environment.eventBus().subscribe(LoggingEventConsumer.builder()
      .disableSlf4J(true)
      .fallbackToConsole(true)
      .build());

    final CoreContext ctx = new CoreContext(1234, environment);
    final Duration timeout = Duration.ofSeconds(1);


    SelectStrategyFactory factory = DefaultSelectStrategyFactory.INSTANCE;

    SelectStrategyFactory f2 = () -> (supplier, hasTasks) -> supplier.get();


    EventLoopGroup group = environment.ioEnvironment().kvEventLoopGroup().get();
    Class<? extends Channel> c;
    if (group instanceof EpollEventLoopGroup) {
      c = EpollSocketChannel.class;
    } else if (group instanceof KQueueEventLoopGroup) {
      c = KQueueSocketChannel.class;
    } else {
      c = NioSocketChannel.class;
    }

    ChannelFuture connect = new Bootstrap()
      .group(group)
      .channel(c)
      .remoteAddress("127.0.0.1", 11210)
      .handler(new KeyValueChannelInitializer(ctx, "travel-sample", "Administrator", "password"))
      .connect();

    Channel channel = connect.awaitUninterruptibly().channel();

    for (int i = 0; i < Integer.MAX_VALUE; i++) {
      GetRequest request = new GetRequest("airline_10".getBytes(CharsetUtil.UTF_8), timeout, null);
      request.partition((short) 41);
      channel.writeAndFlush(request);
      System.err.println(new String(request.response().get().content(), CharsetUtil.UTF_8));
      break;
    }

    Thread.sleep(100000);
  }
}
