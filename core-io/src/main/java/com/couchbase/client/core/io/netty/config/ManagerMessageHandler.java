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

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.events.io.ChannelClosedProactivelyEvent;
import com.couchbase.client.core.cnc.events.io.InvalidRequestDetectedEvent;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.channel.ChannelDuplexHandler;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPromise;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpContent;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.LastHttpContent;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.endpoint.BaseEndpoint;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.manager.ManagerRequest;
import com.couchbase.client.core.retry.RetryOrchestrator;
import com.couchbase.client.core.service.ServiceType;

import java.util.Optional;

/**
 * This handler dispatches requests and responses against the cluster manager service.
 *
 * <p>Note that since one of the messages is a long streaming connection to get continuous
 * updates on configs, the channel might be occupied for a long time. As a result, the upper
 * layers (service pooling) need to be responsible for opening another handler if all the
 * current ones are occupied.</p>
 *
 * @since 1.0.0
 */
public class ManagerMessageHandler extends ChannelDuplexHandler {

  private final CoreContext coreContext;
  private IoContext ioContext;
  private ManagerRequest<Response> currentRequest;
  private ByteBuf currentContent;
  private final EventBus eventBus;
  private final BaseEndpoint endpoint;

  public ManagerMessageHandler(BaseEndpoint endpoint, CoreContext coreContext) {
    this.endpoint = endpoint;
    this.coreContext = coreContext;
    this.eventBus = coreContext.environment().eventBus();
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    ioContext = new IoContext(
      coreContext,
      ctx.channel().localAddress(),
      ctx.channel().remoteAddress(),
      Optional.empty()
    );

    currentContent = ctx.alloc().buffer();
    ctx.fireChannelActive();
  }

  @Override
  @SuppressWarnings({ "unchecked" })
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
    if (msg instanceof ManagerRequest) {
      if (currentRequest != null) {
        RetryOrchestrator.maybeRetry(coreContext, (ManagerRequest<Response>) msg);
        return;
      }

      currentRequest = (ManagerRequest<Response>) msg;
      ctx.writeAndFlush(((ManagerRequest) msg).encode());
      currentContent.clear();
    } else {
      eventBus.publish(new InvalidRequestDetectedEvent(ioContext, ServiceType.MANAGER, msg));
      ctx.channel().close().addListener(f -> eventBus.publish(new ChannelClosedProactivelyEvent(
        ioContext,
        ChannelClosedProactivelyEvent.Reason.INVALID_REQUEST_DETECTED)
      ));
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (msg instanceof HttpResponse) {
      // lastStatus = ((HttpResponse) msg).status();
    } else if (msg instanceof HttpContent) {
      currentContent.writeBytes(((HttpContent) msg).content());
      if (msg instanceof LastHttpContent) {
        byte[] copy = new byte[currentContent.readableBytes()];
        currentContent.readBytes(copy);
        Response response = currentRequest.decode(copy);
        currentRequest.succeed(response);
        currentRequest = null;
        if (endpoint != null) {
          endpoint.markRequestCompletion();
        }
      }
    } else {
      // todo: error since a type returned that was not expected
    }

    ReferenceCountUtil.release(msg);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    ReferenceCountUtil.release(currentContent);
    ctx.fireChannelInactive();
  }
}
