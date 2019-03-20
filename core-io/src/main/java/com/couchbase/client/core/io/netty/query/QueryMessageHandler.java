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

import com.couchbase.client.core.deps.io.netty.handler.codec.http.*;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.error.QueryServiceException;
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
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import java.io.EOFException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;

import static java.nio.charset.StandardCharsets.UTF_8;

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

  public QueryMessageHandler(final EndpointContext endpointContext) {
    this.endpointContext = endpointContext;

    /*
     * The N1QL spec (https://docs.google.com/document/d/1Uyv4t06DNGq7TxJjGI_T_MbbEYf8Er-imC7yzTY0uZw/edit#) defines
     * this ordering for the response body:
     *
     * requestID
     * clientContextID
     * signature
     * results
     * errors
     * warnings
     * success
     * metrics
     * And optionally profile.  It's unclear exactly where this goes but we can assume after results.
     *
     * The code below takes advantage of this guaranteed order to only complete the initial request once all fields
     * up to but not including results is available.  We don't assume the presence of any field, so complete
     * `currentRequest` in all fields after and including signature.
     */
    this.parser = new ByteBufJsonParser(new JsonPointer[]{
      new JsonPointer("/requestID", (JsonPointerCB1) value -> {
        String requestID = value.toString(UTF_8);
        requestID = requestID.substring(1, requestID.length() - 1);
        value.release();
        currentResponse.requestId(requestID);
      }),
      new JsonPointer("/results/-", (JsonPointerCB1) value -> {
        if (!currentRequest.completed()) {
          this.currentRequest.succeed(this.currentResponse);
        }
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
      new JsonPointer("/errors/-", (JsonPointerCB1) value -> {
        if (!currentRequest.completed()) {
          this.currentRequest.succeed(this.currentResponse);
        }
        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();

        currentResponse.rows().onError(new QueryServiceException(data));
      }),
      new JsonPointer("/warnings/-", (JsonPointerCB1) value -> {
        if (!currentRequest.completed()) {
          this.currentRequest.succeed(this.currentResponse);
        }
        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();
        currentResponse.addWarning(data);
      }),
      new JsonPointer("/clientContextID", (JsonPointerCB1) value -> {
        // It's not actually stated in the N1QL spec, but turns out this is optional
        String clientContextID = value.toString(UTF_8);
        clientContextID = clientContextID.substring(1, clientContextID.length() - 1);
        value.release();
        currentResponse.clientContextId(clientContextID);
      }),
      new JsonPointer("/metrics", (JsonPointerCB1) value -> {
        if (!currentRequest.completed()) {
          this.currentRequest.succeed(this.currentResponse);
        }
        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();
        currentResponse.metrics(data);
      }),
      new JsonPointer("/status", (JsonPointerCB1) value -> {
        if (!currentRequest.completed()) {
          this.currentRequest.succeed(this.currentResponse);
        }
        String statusStr = value.toString(UTF_8);
        statusStr = statusStr.substring(1, statusStr.length() - 1);
        value.release();
        currentResponse.queryStatus(statusStr);
      }),
      new JsonPointer("/signature", (JsonPointerCB1) value -> {
        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();
        currentResponse.signature(data);
        if (!currentRequest.completed()) {
          this.currentRequest.succeed(this.currentResponse);
        }
      }),
      new JsonPointer("/profile", (JsonPointerCB1) value -> {
        if (!currentRequest.completed()) {
          this.currentRequest.succeed(this.currentResponse);
        }
        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();
        currentResponse.profile(data);
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

        // Reset state for this request
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
