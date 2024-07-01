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

package com.couchbase.client.core.io.netty;

import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.events.io.GenericFailureDetectedEvent;
import com.couchbase.client.core.cnc.events.io.SecureConnectionFailedEvent;
import com.couchbase.client.core.deps.io.netty.handler.ssl.SslHandshakeCompletionEvent;
import com.couchbase.client.core.diagnostics.AuthenticationStatus;
import com.couchbase.client.core.endpoint.BaseEndpoint;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelInboundHandlerAdapter;
import com.couchbase.client.core.deps.io.netty.handler.codec.DecoderException;

import javax.net.ssl.SSLException;

public class PipelineErrorHandler extends ChannelInboundHandlerAdapter {

  private final BaseEndpoint endpoint;
  private final EventBus eventBus;
  private final EndpointContext endpointContext;
  private IoContext ioContext;

  public PipelineErrorHandler(final BaseEndpoint endpoint) {
    this.endpoint = endpoint;
    this.endpointContext = endpoint.context();
    this.eventBus = endpointContext.environment().eventBus();
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx)  {
    assembleIoContext(ctx);
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
    if (evt instanceof SslHandshakeCompletionEvent) {
      SslHandshakeCompletionEvent sslEvent = (SslHandshakeCompletionEvent) evt;
      // Cause is null if handshake was successful. Set it unconditionally,
      // so the endpoint can recover from a transient on-path attack
      // (where the "server" temporarily presents an untrusted certificate).
      endpointContext.tlsHandshakeFailure(sslEvent.cause());
    }

    ctx.fireUserEventTriggered(evt);
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause)  {
    // if an exception happened during connect, the ioContext is not yet ready,
    // so try to construct one on the fly!
    assembleIoContext(ctx);

    if (cause instanceof DecoderException && cause.getCause() instanceof SSLException) {
      endpointContext.authenticationStatus(AuthenticationStatus.FAILED);
      eventBus.publish(new SecureConnectionFailedEvent(ioContext, (SSLException) cause.getCause()));
    } else {
      eventBus.publish(new GenericFailureDetectedEvent(ioContext, cause));
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    endpoint.notifyChannelInactive();
    ctx.fireChannelActive();
  }

  /**
   * Helper method to assemble the IO context if not present already.
   */
  private void assembleIoContext(final ChannelHandlerContext ctx) {
    if (ioContext == null) {
      ioContext = new IoContext(
        endpointContext,
        ctx.channel().localAddress(),
        ctx.channel().remoteAddress(),
        endpointContext.bucket()
      );
    }
  }

}
