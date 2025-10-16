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

package com.couchbase.client.core.io.netty.kv;

import com.couchbase.client.core.deps.io.netty.channel.ChannelDuplexHandler;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPromise;

import java.net.SocketAddress;

import static java.util.Objects.requireNonNull;

/**
 * Waits for the pipelined handshake sequence to complete before propagating
 * the `channelActive` signal.
 * <p>
 * Necessary because {@link KeyValueMessageHandler#channelActive}
 * depends on channel attributes set by pipelined handshake handlers.
 * <p>
 * @implNote Some handshake handlers are implicit barriers
 * (specifically {@link SelectBucketHandler} and {@link SaslAuthenticationHandler}
 * with a multistep mechanism), but they are not always present.
 */
public class HandshakeBarrier extends ChannelDuplexHandler {

  private ChannelPromise interceptedConnectPromise;

  @Override
  public void connect(
    ChannelHandlerContext ctx,
    SocketAddress remoteAddress,
    SocketAddress localAddress,
    ChannelPromise promise
  ) {
    interceptedConnectPromise = requireNonNull(promise);

    ChannelPromise downstream = ctx.newPromise();
    downstream.addListener(f -> {
      if (f.isSuccess()) {
        interceptedConnectPromise.trySuccess();
        ctx.pipeline().remove(this);
        ctx.fireChannelActive();

      } else {
        interceptedConnectPromise.tryFailure(f.cause());
      }
    });

    ctx.connect(remoteAddress, localAddress, downstream);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    interceptedConnectPromise.tryFailure(cause);
    ctx.fireExceptionCaught(cause);
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    // Prevent this signal from reaching non-pipelined handlers,
    // because the pipelined handshake handlers might still be waiting for responses.
  }
}
