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

package com.couchbase.client.core.io.netty.chunk;

import com.couchbase.client.core.deps.io.netty.channel.ChannelDuplexHandler;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandler;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPromise;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpContent;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaderNames;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.LastHttpContent;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.endpoint.BaseEndpoint;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.msg.HttpRequest;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.chunk.ChunkHeader;
import com.couchbase.client.core.msg.chunk.ChunkRow;
import com.couchbase.client.core.msg.chunk.ChunkTrailer;
import com.couchbase.client.core.msg.chunk.ChunkedResponse;
import com.couchbase.client.core.util.ResponseStatusConverter;

import static com.couchbase.client.core.io.netty.HttpProtocol.remoteHttpHost;

/**
 * Implements the chunk stream handling for all generic http stream based services.
 */
@ChannelHandler.Sharable
public abstract class ChunkedMessageHandler
  <H extends ChunkHeader,
    ROW extends ChunkRow,
    T extends ChunkTrailer,
    R extends ChunkedResponse<H, ROW, T>,
    REQ extends HttpRequest<H, ROW, T, R>> extends ChannelDuplexHandler {

  /**
   * The query endpoint context.
   */
  private final EndpointContext endpointContext;

  /**
   * Holds the response parser implementation for this service.
   */
  private final ChunkResponseParser<H, ROW, T> chunkResponseParser;

  /**
   * Holds the surrounding endpoint.
   */
  private final BaseEndpoint endpoint;

  /**
   * The IO context once connected.
   */
  private IoContext ioContext;

  /**
   * Holds the remote host for caching purposes.
   */
  private String remoteHost;

  /**
   * Holds the current outstanding request sent to the server.
   */
  private REQ currentRequest;

  /**
   * Holds the current response.
   */
  private R currentResponse;

  /**
   * The last received response status from the server.
   */
  private HttpResponse currentResponseStatus;

  /**
   * Holds the converted response status.
   */
  private ResponseStatus convertedResponseStatus;

  /**
   * Creates a new {@link ChunkedMessageHandler}.
   *
   * @param endpoint holds the surrounding endpoint.
   * @param endpointContext the related endpoint context.
   * @param chunkResponseParser the chunk response parser to use for this handler.
   */
  protected ChunkedMessageHandler(final BaseEndpoint endpoint,
                                  final EndpointContext endpointContext,
                                  final ChunkResponseParser<H, ROW, T> chunkResponseParser) {
    this.endpoint = endpoint;
    this.endpointContext = endpointContext;
    this.chunkResponseParser = chunkResponseParser;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void write(final ChannelHandlerContext ctx, final Object msg,
                    final ChannelPromise promise) {
    try {
      currentRequest = (REQ) msg;
      FullHttpRequest encoded = currentRequest.encode();
      encoded.headers().set(HttpHeaderNames.HOST, remoteHost);
      encoded.headers().set(HttpHeaderNames.USER_AGENT, endpointContext.environment().userAgent().formattedLong());
      ctx.write(encoded, promise);
    } catch (Throwable t) {
      // TODO: handle encoding/write failures
    }
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    remoteHost = remoteHttpHost(ctx.channel().remoteAddress());
    ioContext = new IoContext(
      endpointContext,
      ctx.channel().localAddress(),
      ctx.channel().remoteAddress(),
      endpointContext.bucket()
    );
    ctx.fireChannelActive();
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    try {
      if (msg instanceof HttpResponse) {
        handleHttpResponse(ctx, (HttpResponse) msg);
      } else if (msg instanceof HttpContent) {
        ((HttpContent) msg).retain(); // Parser takes ownership; counteract the release in 'finally' block.
        handleHttpContent((HttpContent) msg);
        if (msg instanceof LastHttpContent) {
          chunkResponseParser.endOfInput();
          if (!isSuccess()) {
            completeResponseWithFailure();
          }
          cleanupState();
          if (endpoint != null) {
            endpoint.markRequestCompletion();
          }
        }
      } else {
        // todo: error -> unknown response type
      }
    } finally {
      ReferenceCountUtil.release(msg);
    }
  }

  @Override
  public void handlerRemoved(final ChannelHandlerContext ctx) {
    cleanupState();
    ctx.fireChannelInactive();
  }

  private void handleHttpResponse(final ChannelHandlerContext ctx, final HttpResponse msg) {
    currentResponseStatus = msg;
    convertedResponseStatus = ResponseStatusConverter.fromHttp(msg.status().code());
    chunkResponseParser.initialize(ctx.channel().config());
  }

  private void handleHttpContent(final HttpContent msg) {
    chunkResponseParser.feed(msg.content());

    if (currentResponse == null && isSuccess() && chunkResponseParser.header().isPresent()) {
      completeInitialResponse(chunkResponseParser.header().get());
    }
  }

  private boolean isSuccess() {
    return convertedResponseStatus.success() && !chunkResponseParser.decodingFailure().isPresent();
  }

  private void completeInitialResponse(final H header) {
    currentResponse = currentRequest.decode(
      convertedResponseStatus, header, chunkResponseParser.rows(), chunkResponseParser.trailer()
    );
    currentRequest.succeed(currentResponse);
  }

  private void completeResponseWithFailure() {
    final Throwable cause = chunkResponseParser.decodingFailure().orElseGet(
      () -> chunkResponseParser.error().orElseGet(
        () -> new CouchbaseException("Request failed, but no more information available")));
    currentRequest.fail(cause);
  }

  private void cleanupState() {
    chunkResponseParser.cleanup();
    currentResponse = null;
    currentRequest = null;
    currentResponseStatus = null;
  }

}
