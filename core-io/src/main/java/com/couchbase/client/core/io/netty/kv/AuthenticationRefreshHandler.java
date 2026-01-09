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

package com.couchbase.client.core.io.netty.kv;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.buffer.UnpooledByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelInboundHandlerAdapter;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.io.netty.kv.sasl.SingleStepSaslAuthParameters;
import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.BaseKeyValueRequest;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.util.SynchronousEventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.decodeStatus;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noPartition;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.request;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.status;
import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static com.couchbase.client.core.util.CbThrowables.propagate;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class AuthenticationRefreshHandler extends ChannelInboundHandlerAdapter {
  private static final Logger log = LoggerFactory.getLogger(AuthenticationRefreshHandler.class);

  private final EndpointContext endpointContext;

  private ChannelHandlerContext ctx;
  private SynchronousEventBus.Subscription subscription;

  private boolean handshakeComplete;
  private boolean reauthenticateLaterWhenHandshakeCompletes;

  public AuthenticationRefreshHandler(EndpointContext endpointContext) {
    this.endpointContext = requireNonNull(endpointContext);
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) {
    this.ctx = requireNonNull(ctx);

    subscription = endpointContext.core()
      .authenticationRefreshTriggers.subscribe(signal ->
        // Switch to the channel's event loop to avoid race condition with channelActive(),
        // and for safe access of all non-final handler fields.
        ctx.executor().submit(this::handleSignal)
      );
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) {
    subscription.unsubscribe();
  }

  private void requireInEventLoop() {
    if (!ctx.executor().inEventLoop()) {
      throw new IllegalStateException("This method must only be called in the channel's event loop");
    }
  }

  private void handleSignal() {
    requireInEventLoop();

    if (handshakeComplete) {
      reauthenticate();
    } else {
      log.info("Deferring reauthentication until handshake completes. Channel: {}", redactSystem(ctx.channel()));
      reauthenticateLaterWhenHandshakeCompletes = true;
    }
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    ctx.fireChannelActive();

    handshakeComplete = true;
    if (reauthenticateLaterWhenHandshakeCompletes) {
      log.info("Immediately reauthenticating the channel because a new authenticator was provided during the handshake process. Channel: {}", redactSystem(ctx.channel()));
      reauthenticate();
    }
  }

  private void reauthenticate() {
    requireInEventLoop();

    SingleStepSaslAuthParameters params = endpointContext.authenticator().getSingleStepSaslAuthParameters();
    SingleStepAuthRequest req = new SingleStepAuthRequest(endpointContext, params);

    // Omit channel writability check. We're unlikely to be making these requests more than once a minute,
    // so there's little danger of OOM-ing due to too many of these requests piling up in the channel's outbound buffer.

    ctx.writeAndFlush(req)
      .addListener(it -> {
        if (it.isSuccess()) {
          log.debug("Successfully wrote authentication refresh request to channel {}", redactSystem(ctx.channel()));
          return;
        }

        if (!ctx.channel().isActive()) {
          log.debug("Failed to write authentication refresh request, but that's probably okay because channel is inactive. Channel: {}", redactSystem(ctx.channel()), it.cause());
          return;
        }

        log.warn("Failed to write authentication refresh request to channel. Closing channel: {}", redactSystem(ctx.channel()), it.cause());
        ctx.channel().close();
      });

    req.response()
      .thenAccept(response -> {
        if (!response.status().success()) {
          log.warn("Authentication refresh request failed with status {}; closing channel {}", response.status(), redactSystem(ctx.channel()));
          ctx.channel().close();
        } else {
          log.info("Authentication refresh succeeded for channel {}", redactSystem(ctx.channel()));
        }
      })
      .exceptionally(t -> {
        log.error("Authentication refresh threw an exception; closing channel {}", redactSystem(ctx.channel()), t);
        ctx.channel().close();
        throw propagate(t);
      });
  }

  private static class SingleStepAuthResponse extends BaseResponse {
    protected SingleStepAuthResponse(ResponseStatus status) {
      super(status);
    }
  }

  private static class SingleStepAuthRequest extends BaseKeyValueRequest<SingleStepAuthResponse> {
    private final SingleStepSaslAuthParameters params;

    protected SingleStepAuthRequest(CoreContext ctx, SingleStepSaslAuthParameters params) {
      // We're sending this request at a low layer that bypasses timeout and retry handling,
      // so those arguments don't really matter -- but superclass requires us to pass _something_.
      super(Duration.ZERO, ctx, FailFastRetryStrategy.INSTANCE, null, null);

      this.params = requireNonNull(params);
    }

    @Override
    public ByteBuf encode(ByteBufAllocator alloc, int opaque, KeyValueChannelContext ctx) {
      ByteBuf key = Unpooled.copiedBuffer(params.mechanism().mech(), UTF_8);
      ByteBuf body = Unpooled.copiedBuffer(params.payload(), UTF_8);

      return request(
        UnpooledByteBufAllocator.DEFAULT,
        MemcacheProtocol.Opcode.SASL_AUTH,
        noDatatype(),
        noPartition(),
        opaque,
        noCas(),
        noExtras(),
        key,
        body
      );
    }

    @Override
    public SingleStepAuthResponse decode(
      ByteBuf response,
      KeyValueChannelContext ctx
    ) {
      short statusCode = status(response);
      ResponseStatus status = statusCode == MemcacheProtocol.Status.AUTH_ERROR.status()
        ? ResponseStatus.NO_ACCESS // abusing this to mean "not authenticated"
        : decodeStatus(statusCode);

      return new SingleStepAuthResponse(status);
    }
  }
}
