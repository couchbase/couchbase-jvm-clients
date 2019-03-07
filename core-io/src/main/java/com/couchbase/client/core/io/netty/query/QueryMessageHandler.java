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

import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.error.QueryStreamException;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.util.ResponseStatusConverter;
import com.couchbase.client.core.util.yasjl.ByteBufJsonParser;
import com.couchbase.client.core.util.yasjl.Callbacks.JsonPointerCB1;
import com.couchbase.client.core.util.yasjl.JsonPointer;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.channel.ChannelDuplexHandler;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPromise;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpContent;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaderNames;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.LastHttpContent;
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import java.io.EOFException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;

/**
 * This handler is responsible for writing Query requests and completing their associated responses
 * once they arrive.
 *
 * @since 2.0.0
 */
public class QueryMessageHandler extends ChannelDuplexHandler {

  /**
   * The query endpoint context.
   */
  private final EndpointContext endpointContext;

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
   * A Streaming json parser {@link ByteBufJsonParser}
   */
  private ByteBufJsonParser parser;

  /**
   * Holds the remote host for caching purposes.
   */
  private String remoteHost;

  /**
   * The IO context once connected.
   */
  private IoContext ioContext;

  /**
   * The initialized state of the parser
   */
  private boolean isParserInitialized = false;

  /**
   * The character set used for decoding the streaming response received
   */
  private static final Charset CHARSET = CharsetUtil.UTF_8;

  public QueryMessageHandler(final EndpointContext endpointContext) {
    this.endpointContext = endpointContext;
    this.parser = new ByteBufJsonParser(new JsonPointer[]{
      new JsonPointer("/results/-", (JsonPointerCB1) value -> {
        byte[] data = new byte[value.readableBytes()];
          value.readBytes(data);
          value.release();
          if (!currentResponse.isCompleted()) {
            if (currentResponse.rowRequestSize() != 0 && currentResponse.rows().getPending() == 0) {
              currentResponse.rows().onNext(data);
              currentResponse.rowRequestCompleted();
            } else {
              currentResponse.completeExceptionally(
                new QueryStreamException(currentResponse.rowRequestSize() == 0 ? "No row requests"
                  : "Current row responses are not consumed"));
            }
          }
      }),
      new JsonPointer("/requestID/-", (JsonPointerCB1) value -> {
        String requestID = value.toString(CHARSET);
        requestID = requestID.substring(1, requestID.length() - 1);
        value.release();
//        currentResponse.requestId().onNext(requestID);
      }),
      new JsonPointer("/errors/-", (JsonPointerCB1) value -> {
        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();
        currentResponse.rows().onNext(data);
      }),
      new JsonPointer("/warnings/-", (JsonPointerCB1) value -> {
        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();
//        currentResponse.warnings().onNext(data);
      }),
      new JsonPointer("/clientContextID", (JsonPointerCB1) value -> {
        String clientContextID = value.toString(CHARSET);
        clientContextID = clientContextID.substring(1, clientContextID.length() - 1);
        value.release();
//        currentResponse.clientContextId().onNext(clientContextID);
      }),
      new JsonPointer("/metrics", (JsonPointerCB1) value -> {
        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();
//        currentResponse.metrics().onNext(data);
      }),
      new JsonPointer("/status", (JsonPointerCB1) value -> {
        String statusStr = value.toString(CHARSET);
        statusStr = statusStr.substring(1, statusStr.length() - 1);
        value.release();
//        currentResponse.queryStatus().onNext(statusStr);
      }),
      new JsonPointer("/signature", (JsonPointerCB1) value -> {
        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();
//        currentResponse.signature().onNext(data);
      }),
      new JsonPointer("/profile", (JsonPointerCB1) value -> {
        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();
//        currentResponse.profile().onNext(data);
      })
    });
  }

  @Override
  public void channelInactive(final ChannelHandlerContext ctx) {
    if (currentResponse != null) {
      currentResponse.complete();
    } else if (currentRequest != null) {
      currentRequest.cancel(CancellationReason.IO_CLOSED_WHILE_IN_FLIGHT);
    }
    ReferenceCountUtil.release(responseContent);
    ctx.fireChannelInactive();
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    remoteHost = remoteHttpHost(ctx);
    ioContext = new IoContext(
      endpointContext,
      ctx.channel().localAddress(),
      ctx.channel().remoteAddress(),
      endpointContext.bucket()
    );
    ctx.fireChannelActive();
  }

  @Override
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
    if (msg instanceof QueryRequest) {
      currentRequest = (QueryRequest) msg;
      FullHttpRequest encoded = ((QueryRequest) msg).encode();
      encoded.headers().set(HttpHeaderNames.HOST, remoteHost);
      ctx.write(encoded);
      ctx.channel().config().setAutoRead(true);
    } else {
      this.currentRequest.cancel(CancellationReason.OTHER);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    // todo: log exception caught
    ctx.close();
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    try {
      if (msg instanceof HttpResponse) {
        this.responseContent = ctx.alloc().buffer();
        this.currentResponse = new QueryResponse(ResponseStatusConverter.fromHttp(((HttpResponse) msg).status().code()),
                ctx.channel(), endpointContext.environment());
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
            this.currentResponse.completeSuccessfully();
          }
        }
      }
    } catch(Exception ex) {
      if (this.currentResponse != null) {
        this.currentResponse.completeExceptionally(ex);
      } else {
        currentRequest.cancel(CancellationReason.IO_CLOSED_WHILE_IN_FLIGHT);
      }
    } finally {
      ReferenceCountUtil.release(msg);
    }
  }

  /**
   * Calculates the remote host for caching so that it is set on each query request.
   *
   * @param ctx the channel handler context.
   * @return the converted remote http host.
   */
  private String remoteHttpHost(final ChannelHandlerContext ctx) {
    final String remoteHost;
    final SocketAddress addr = ctx.channel().remoteAddress();
    if (addr instanceof InetSocketAddress) {
      InetSocketAddress inetAddr = (InetSocketAddress) addr;
      remoteHost = inetAddr.getAddress().getHostAddress() + ":" + inetAddr.getPort();
    } else {
      remoteHost = addr.toString();
    }
    return remoteHost;
  }
}