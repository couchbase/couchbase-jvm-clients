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

package com.couchbase.client.core.io.netty.config;

import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.manager.ManagerRequest;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;

/**
 * This handler dispatches requests and responses against the cluster manager service.
 *
 * <p>Note that since one of the messages is a long streaming connection to get continuous updates on configs,
 * the channel might be occupied for a long time. As a result, the upper layers (service pooling) need to be
 * responsible for opening another handler if all the current ones are occupied.</p>
 *
 * @since 1.0.0
 */
public class ManagerMessageHandler extends ChannelDuplexHandler {

  private ManagerRequest currentRequest;
  private HttpResponseStatus lastStatus;
  private ByteBuf currentContent;

  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    currentContent = ctx.alloc().buffer();
    ctx.fireChannelActive();
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
    if (msg instanceof ManagerRequest) {
      currentRequest = (ManagerRequest) msg;
      ctx.writeAndFlush(((ManagerRequest) msg).encode());
      currentContent.clear();
    } else {
      // todo: terminate this channel and raise an event, this is not supposed to happen
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (msg instanceof HttpResponse) {
      lastStatus = ((HttpResponse) msg).status();
    } else if (msg instanceof HttpContent) {
      currentContent.writeBytes(((HttpContent) msg).content());
      if (msg instanceof LastHttpContent) {
        byte[] copy = new byte[currentContent.readableBytes()];
        currentContent.readBytes(copy);
        Response response = currentRequest.decode(copy);
        currentRequest.succeed(response);
      }
    } else {
      // todo: error since a type returned that was not expected
    }

    ReferenceCountUtil.release(msg);
  }

}
