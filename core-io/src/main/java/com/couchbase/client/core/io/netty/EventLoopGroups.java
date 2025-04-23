/*
 * Copyright 2025 Couchbase, Inc.
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

package com.couchbase.client.core.io.netty;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.io.netty.channel.Channel;
import com.couchbase.client.core.deps.io.netty.channel.DefaultEventLoopGroup;
import com.couchbase.client.core.deps.io.netty.channel.EventLoopGroup;
import com.couchbase.client.core.deps.io.netty.channel.IoHandler;
import com.couchbase.client.core.deps.io.netty.channel.IoHandlerFactory;
import com.couchbase.client.core.deps.io.netty.channel.MultiThreadIoEventLoopGroup;
import com.couchbase.client.core.deps.io.netty.channel.epoll.Epoll;
import com.couchbase.client.core.deps.io.netty.channel.epoll.EpollIoHandler;
import com.couchbase.client.core.deps.io.netty.channel.epoll.EpollSocketChannel;
import com.couchbase.client.core.deps.io.netty.channel.kqueue.KQueue;
import com.couchbase.client.core.deps.io.netty.channel.kqueue.KQueueIoHandler;
import com.couchbase.client.core.deps.io.netty.channel.kqueue.KQueueSocketChannel;
import com.couchbase.client.core.deps.io.netty.channel.local.LocalChannel;
import com.couchbase.client.core.deps.io.netty.channel.local.LocalIoHandler;
import com.couchbase.client.core.deps.io.netty.channel.nio.NioIoHandler;
import com.couchbase.client.core.deps.io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Utility methods for working with EventLoopGroups.
 */
@Stability.Internal
public class EventLoopGroups {
  private EventLoopGroups() {
  }

  public static IoHandlerFactory newIoHandlerFactory(boolean nativeIoEnabled) {
    if (nativeIoEnabled && Epoll.isAvailable()) {
      return EpollIoHandler.newFactory();
    }

    if (nativeIoEnabled && KQueue.isAvailable()) {
      return KQueueIoHandler.newFactory();
    }

    return NioIoHandler.newFactory();
  }

  /**
   * Returns the correct channel class for the given event loop group.
   */
  public static Class<? extends Channel> channelType(final EventLoopGroup eventLoopGroup) {
    if (isEpoll(eventLoopGroup)) {
      return EpollSocketChannel.class;
    }

    if (isNio(eventLoopGroup)) {
      return NioSocketChannel.class;
    }

    if (isKQueue(eventLoopGroup)) {
      return KQueueSocketChannel.class;
    }

    if (isLocal(eventLoopGroup)) {
      // Used for testing!
      return LocalChannel.class;
    }

    throw new IllegalArgumentException("Unknown EventLoopGroup Type: "
      + eventLoopGroup.getClass().getSimpleName());
  }

  public static boolean isKQueue(EventLoopGroup group) {
    return isIoType(group, KQueueIoHandler.class);
  }

  public static boolean isEpoll(EventLoopGroup group) {
    return isIoType(group, EpollIoHandler.class);
  }

  public static boolean isNio(EventLoopGroup group) {
    return isIoType(group, NioIoHandler.class);
  }

  public static boolean isLocal(EventLoopGroup group) {
    return isIoType(group, LocalIoHandler.class) || group instanceof DefaultEventLoopGroup;
  }

  private static boolean isIoType(
    EventLoopGroup group,
    Class<? extends IoHandler> handlerType
  ) {
    return group instanceof MultiThreadIoEventLoopGroup && ((MultiThreadIoEventLoopGroup) group).isIoType(handlerType);
  }
}
