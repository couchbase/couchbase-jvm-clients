/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.io.netty.query;

import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.service.ServiceContext;
import com.couchbase.client.core.util.ResponseStatusConverter;
import com.couchbase.client.core.util.yasjl.ByteBufJsonParser;
import com.couchbase.client.core.util.yasjl.Callbacks.JsonPointerCB1;
import com.couchbase.client.core.util.yasjl.JsonPointer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import java.io.EOFException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * This handler is responsible for writing Query requests and completing their associated responses
 * once they arrive.
 *
 * @since 2.0.0
 */
public class QueryMessageHandler extends ChannelDuplexHandler {

  /**
   * The current query request that is being handled by the query message handler
   */
  private QueryRequest currentRequest;

  /**
   * The query response that will be created as received for the current request
   */
  private QueryResponse currentResponse;

  /**
   * The response content that is being received as chunks
   */
  private ByteBuf responseContent;

  /**
   * The query service context
   */
  private final ServiceContext serviceContext;

  /**
   * A Streaming json parser {@link ByteBufJsonParser}
   */
  private ByteBufJsonParser parser;

  /**
   * The initialized state of the parser
   */
  private boolean isParserInitialized = false;

  /**
   * The character set used for decoding the streaming response received
   */
  private static final Charset CHARSET = CharsetUtil.UTF_8;

  public QueryMessageHandler(ServiceContext serviceContext) {
    this.serviceContext = serviceContext;
    this.parser = new ByteBufJsonParser(new JsonPointer[]{
      new JsonPointer("/results/-", new JsonPointerCB1() {
        @Override
        public void call(final ByteBuf value) {
          byte[] data = new byte[value.readableBytes()];
            value.readBytes(data);
            value.release();
            if (!currentResponse.isCompleted()) {
              if (currentResponse.rowRequestSize() != 0 || currentResponse.rows().getPending() == 0) {
                currentResponse.rows().onNext(data);
                currentResponse.rowRequestCompleted();
              } else {
                currentResponse.rows().onError(new RequestCanceledException(currentResponse.rowRequestSize() == 0 ? "No row requests" :
                        "Current row responses are not consumed", currentRequest.context()));
                currentResponse.complete();
              }
            }
        }
      }), new JsonPointer("/requestID/-", new JsonPointerCB1() {
      @Override
      public void call(final ByteBuf value) {
        String requestID = value.toString(CHARSET);
        requestID = requestID.substring(1, requestID.length() - 1);
        value.release();
        currentResponse.requestId().onNext(requestID);
      }
    }), new JsonPointer("/errors/-", new JsonPointerCB1() {
      @Override
      public void call(final ByteBuf value) {
        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();
        currentResponse.errors().onNext(data);
      }
    }), new JsonPointer("/warnings/-", new JsonPointerCB1() {
      @Override
      public void call(final ByteBuf value) {
        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();
        currentResponse.warnings().onNext(data);
      }
    }), new JsonPointer("/clientContextID", new JsonPointerCB1() {
      @Override
      public void call(final ByteBuf value) {
        String clientContextID = value.toString(CHARSET);
        clientContextID = clientContextID.substring(1, clientContextID.length() - 1);
        value.release();
        currentResponse.clientContextId().onNext(clientContextID);
      }
    }), new JsonPointer("/metrics", new JsonPointerCB1() {
      @Override
      public void call(final ByteBuf value) {
        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();
        currentResponse.metrics().onNext(data);
      }
    }), new JsonPointer("/status", new JsonPointerCB1() {
      @Override
      public void call(final ByteBuf value) {
        String statusStr = value.toString(CHARSET);
        statusStr = statusStr.substring(1, statusStr.length() - 1);
        value.release();
        currentResponse.queryStatus().onNext(statusStr);
      }
    }), new JsonPointer("/signature", new JsonPointerCB1() {
      @Override
      public void call(final ByteBuf value) {
        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();
        currentResponse.signature().onNext(data);
      }
    }), new JsonPointer("/profile", new JsonPointerCB1() {
      @Override
      public void call(final ByteBuf value) {
        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();
        currentResponse.profile().onNext(data);
      }
    })
    });
  }

  @Override
  public void channelInactive(final ChannelHandlerContext ctx) {
    if (currentResponse != null) {
      currentResponse.complete();
    } else if (currentRequest != null) {
      currentRequest.fail(new RequestCanceledException("Closed channel" + getChannelIdentifier(ctx),
              currentRequest.context()));
    }
    ctx.fireChannelInactive();
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    ctx.fireChannelActive();
  }

  @Override
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
    if (msg instanceof QueryRequest) {
      currentRequest = (QueryRequest) msg;
      FullHttpRequest encoded = ((QueryRequest) msg).encode();
      encoded.headers().set(HttpHeaderNames.HOST, "127.0.0.1");
      ctx.write(encoded);
      ctx.channel().config().setAutoRead(true);
    } else {
      this.currentRequest.fail(new RequestCanceledException("Unknown request in channel" + getChannelIdentifier(ctx),
              currentRequest.context()));
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    this.currentRequest.fail(new RequestCanceledException("Exception caught in channel" + getChannelIdentifier(ctx) +
            Arrays.stream(cause.getStackTrace()).map(StackTraceElement::toString).collect(Collectors.joining("\n")),
            currentRequest.context()));
    ctx.close();
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    try {
      if (msg instanceof HttpResponse) {
        this.responseContent = ctx.alloc().buffer();
        this.currentResponse = new QueryResponse(ResponseStatusConverter.fromHttp(((HttpResponse) msg).status().code()),
                ctx.channel(), this.serviceContext.environment());
        this.currentRequest.succeed(this.currentResponse);
        this.isParserInitialized = false;
      }
      if (msg instanceof HttpContent) {
        if (!this.currentResponse.isCompleted()) {
          boolean last = msg instanceof LastHttpContent;
          this.responseContent.writeBytes(((HttpContent) msg).content());
          try {
            if (!this.isParserInitialized) {
              this.parser.initialize(this.responseContent);
              this.isParserInitialized = true;
            }
            this.parser.parse();
            //discard only if EOF is not thrown
            this.responseContent.discardReadBytes();
          } catch (EOFException e) {
            // ignore, we are waiting for more data.
          }
          if (last) {
            this.responseContent.clear();
            this.currentResponse.complete();
          }
        }
      }
    } finally {
      ReferenceCountUtil.release(msg);
    }
  }

  /**
   * The IP address and port info for the channel's local and remote socket endpoints used as channel identifier
   *
   * @param ctx {@link ChannelHandlerContext}
   * @return socket address details as string
   */
  private String getChannelIdentifier(final ChannelHandlerContext ctx) {
    return "["+
            "remote:" + getAddressAsString(ctx.channel().remoteAddress()) + "," +
            "local:" + getAddressAsString(ctx.channel().localAddress()) + "]";
  }

  /**
   * Get the socket address as a string
   *
   * @param address {@link SocketAddress}
   * @return socket address as string
   */
  private String getAddressAsString(final SocketAddress address) {
    String ret = address.toString();
    if (address instanceof InetSocketAddress) {
      ret = ((InetSocketAddress)address).getAddress().getHostAddress() + ((InetSocketAddress)address).getPort();
    }
    return ret;
  }
}