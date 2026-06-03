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

package com.couchbase.client.core.io.netty;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.io.netty.bootstrap.Bootstrap;
import com.couchbase.client.core.deps.io.netty.channel.Channel;
import com.couchbase.client.core.deps.io.netty.channel.DefaultEventLoopGroup;
import com.couchbase.client.core.deps.io.netty.channel.IoEventLoop;
import com.couchbase.client.core.deps.io.netty.channel.IoEventLoopGroup;
import com.couchbase.client.core.deps.io.netty.channel.IoHandler;
import com.couchbase.client.core.deps.io.netty.channel.epoll.EpollIoHandler;
import com.couchbase.client.core.deps.io.netty.channel.epoll.EpollSocketChannel;
import com.couchbase.client.core.deps.io.netty.channel.kqueue.KQueueIoHandler;
import com.couchbase.client.core.deps.io.netty.channel.kqueue.KQueueSocketChannel;
import com.couchbase.client.core.deps.io.netty.channel.local.LocalChannel;
import com.couchbase.client.core.deps.io.netty.channel.local.LocalIoHandler;
import com.couchbase.client.core.deps.io.netty.channel.nio.NioIoHandler;
import com.couchbase.client.core.deps.io.netty.channel.socket.nio.NioSocketChannel;
import org.jspecify.annotations.NullMarked;

import static java.util.Objects.requireNonNull;

/**
 * Inspecting the event loop group type on every use can cause uneven distribution
 * of channels to event loops, because {@link IoEventLoopGroup#isIoType(Class)}
 * increments the counter that points to the "next" loop (see JVMCBC-1740).
 * To avoid that trap, this class inspects the group just once and caches the type.
 */
@Stability.Internal
@NullMarked
public class EventLoopGroupAndType {
  private final IoEventLoopGroup group;
  private final Type type;

  private enum Type {
    EPOLL(
      EpollIoHandler.class,
      EpollSocketChannel.class
    ),
    KQUEUE(
      KQueueIoHandler.class,
      KQueueSocketChannel.class
    ),
    NIO(
      NioIoHandler.class,
      NioSocketChannel.class
    ),
    LOCAL(
      LocalIoHandler.class,
      LocalChannel.class
    ),
    ;

    private final Class<? extends IoHandler> handlerType;
    private final Class<? extends Channel> channelType;

    Type(Class<? extends IoHandler> handlerType, Class<? extends Channel> channelType) {
      this.handlerType = handlerType;
      this.channelType = requireNonNull(channelType);
    }
  }

  private EventLoopGroupAndType(
    IoEventLoopGroup group,
    Type type
  ) {
    this.group = requireNonNull(group);
    this.type = requireNonNull(type);
  }

  public static EventLoopGroupAndType from(IoEventLoopGroup group) {
    return new EventLoopGroupAndType(group, sniffType(group));
  }

  public IoEventLoopGroup group() {
    return group;
  }

  public Bootstrap newBootstrap() {
    return new Bootstrap()
      .group(group)
      .channel(type.channelType);
  }

  public boolean isLocal() {
    return type == Type.LOCAL;
  }

  public boolean isEpoll() {
    return type == Type.EPOLL;
  }

  public boolean isKqueue() {
    return type == Type.KQUEUE;
  }

  public boolean isNio() {
    return type == Type.NIO;
  }

  private static Type sniffType(final IoEventLoopGroup eventLoopGroup) {
    IoEventLoop loop = eventLoopGroup.next();
    for (Type type : Type.values()) {
      if (loop.isIoType(type.handlerType)) {
        return type;
      }
    }

    if (eventLoopGroup instanceof DefaultEventLoopGroup) {
      return Type.LOCAL;
    }

    throw new IllegalArgumentException("Unknown IO type for event loop group "
      + eventLoopGroup.getClass().getSimpleName());
  }

  @Override
  public String toString() {
    return "EventLoopGroupAndType{" +
      "group=" + group +
      ", type=" + type +
      '}';
  }
}
