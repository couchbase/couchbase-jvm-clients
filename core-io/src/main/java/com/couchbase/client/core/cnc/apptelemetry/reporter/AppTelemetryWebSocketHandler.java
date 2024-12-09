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

package com.couchbase.client.core.cnc.apptelemetry.reporter;

import com.couchbase.client.core.cnc.apptelemetry.collector.AppTelemetryCollector;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.channel.Channel;
import com.couchbase.client.core.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPromise;
import com.couchbase.client.core.deps.io.netty.channel.SimpleChannelInboundHandler;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.websocketx.WebSocketFrame;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.websocketx.WebSocketHandshakeException;
import com.couchbase.client.core.deps.io.netty.handler.timeout.IdleState;
import com.couchbase.client.core.deps.io.netty.handler.timeout.IdleStateEvent;
import com.couchbase.client.core.deps.io.netty.handler.timeout.IdleStateHandler;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

class AppTelemetryWebSocketHandler extends SimpleChannelInboundHandler<Object> {

  private static final Logger log = LoggerFactory.getLogger(AppTelemetryWebSocketHandler.class);

  private final WebSocketClientHandshaker handshaker;
  private final AppTelemetryCollector collector;
  private @Nullable ChannelPromise handshakePromise;

  private static final int REQ_OPCODE_GET_TELEMETRY = 0;

  private static final int RES_OPCODE_SUCCESS = 0;
  private static final int RES_OPCODE_UNRECOGNIZED = 1;

  public AppTelemetryWebSocketHandler(
    WebSocketClientHandshaker handshaker,
    AppTelemetryCollector collector
  ) {
    this.handshaker = requireNonNull(handshaker);
    this.collector = requireNonNull(collector);
  }

  public ChannelFuture handshakeFuture() {
    return handshakePromise();
  }

  private ChannelPromise handshakePromise() {
    if (handshakePromise == null) {
      throw new IllegalStateException("Can't get handshake future until handler is added to pipeline.");
    }
    return handshakePromise;
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) {
    handshakePromise = ctx.newPromise();
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    handshaker.handshake(ctx.channel());
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    log.debug("App telemetry WebSocket channel closed.");
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
    if (evt instanceof IdleStateEvent) {
      IdleStateEvent e = (IdleStateEvent) evt;
      if (e.state() == IdleState.READER_IDLE) {
        Duration idleTimeout = Duration.ofMillis(ctx.pipeline().get(IdleStateHandler.class).getReaderIdleTimeInMillis());
        log.info("App telemetry WebSocket connection idle for more than {}; closing channel.", idleTimeout);
        ctx.close();
      } else if (e.state() == IdleState.WRITER_IDLE) {
        log.info("App telemetry WebSocket connection is idle; pinging server.");
        ByteBuf payload = Unpooled.buffer(8).writeLong(System.nanoTime());
        ctx.writeAndFlush(new PingWebSocketFrame(payload), ctx.voidPromise());
      }
      return;
    }

    ctx.fireUserEventTriggered(evt);
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, Object msg) {
    Channel ch = ctx.channel();
    if (!handshaker.isHandshakeComplete()) {
      try {
        handshaker.finishHandshake(ch, (FullHttpResponse) msg);
        log.debug("App telemetry WebSocket handshake successful");
        handshakePromise().setSuccess();

      } catch (WebSocketHandshakeException e) {
        log.warn("App telemetry WebSocket handshake failed", e);
        handshakePromise().setFailure(e);
      }

      return;
    }

    if (msg instanceof FullHttpResponse) {
      FullHttpResponse response = (FullHttpResponse) msg;
      throw new RuntimeException(
        "Unexpected FullHttpResponse (getStatus=" + response.status() +
          ", content=" + response.content().toString(UTF_8) + ')');
    }

    WebSocketFrame frame = (WebSocketFrame) msg;
    if (frame instanceof BinaryWebSocketFrame) {
      BinaryWebSocketFrame binaryFrame = (BinaryWebSocketFrame) frame;
      int opcode = binaryFrame.content().getUnsignedByte(0);

      switch (opcode) {
        case REQ_OPCODE_GET_TELEMETRY: {
          ByteBuf response = Unpooled.buffer().writeByte(RES_OPCODE_SUCCESS);
          collector.reportTo(s -> response.writeCharSequence(s, UTF_8));

          if (log.isDebugEnabled()) {
            log.debug(
              "App telemetry WebSocket client responding to GET_TELEMETRY with:\n{}",
              response.slice()
                .skipBytes(1) // skip response opcode
                .toString(UTF_8)
                .trim() // without trailing newline
            );
          }

          ctx.writeAndFlush(new BinaryWebSocketFrame(response), ch.voidPromise());
          break;
        }

        default: {
          log.debug("App telemetry WebSocket client responding to unrecognized opcode {}", opcode);
          ByteBuf response = Unpooled.buffer(1).writeByte(RES_OPCODE_UNRECOGNIZED);
          ctx.writeAndFlush(new BinaryWebSocketFrame(response), ch.voidPromise());
        }
      }

    } else if (frame instanceof PingWebSocketFrame) {
      log.debug("App telemetry WebSocket client received ping");
      ctx.writeAndFlush(new PongWebSocketFrame(frame.content().retain()), ch.voidPromise());

    } else if (frame instanceof PongWebSocketFrame) {
      if (log.isDebugEnabled()) {
        ByteBuf payload = frame.content();
        if (!payload.isReadable(8)) {
          log.debug("App telemetry WebSocket client received pong; latency unknown (no payload)");
        } else {
          long startNanos = payload.readLong();
          long elapsedNanos = System.nanoTime() - startNanos;
          log.debug("App telemetry WebSocket client received pong; latency={}", Duration.ofNanos(elapsedNanos));
        }
      }

    } else if (frame instanceof CloseWebSocketFrame) {
      CloseWebSocketFrame closeFrame = (CloseWebSocketFrame) frame;
      log.debug("App telemetry WebSocket client received close frame; code={} reason={}", closeFrame.statusCode(), closeFrame.reasonText());

      // Don't ping during closing handshake.
      // Handshaker force-closes the channel if necessary.
      ctx.pipeline().remove(IdleStateHandler.class);

      handshaker.close(ctx, (CloseWebSocketFrame) frame.retain())
        .addListener(f -> {
          log.debug("App telemetry WebSocket closing handshake complete; closing channel");
          ch.close();
        });

    } else {
      log.error("App telemetry WebSocket client received unexpected frame type: {}", frame.getClass());
      ch.close();
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    log.warn("Closing app telemetry WebSocket channel due to unexpected exception.", cause);
    handshakePromise().tryFailure(cause);
    ctx.close();
  }
}
