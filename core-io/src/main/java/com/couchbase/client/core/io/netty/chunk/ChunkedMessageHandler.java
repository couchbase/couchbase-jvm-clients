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

import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.cnc.events.io.ChannelClosedProactivelyEvent;
import com.couchbase.client.core.cnc.events.io.UnsupportedResponseTypeReceivedEvent;
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
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.io.netty.HttpProtocol;
import com.couchbase.client.core.io.netty.kv.ChannelAttributes;
import com.couchbase.client.core.msg.HttpRequest;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.chunk.ChunkHeader;
import com.couchbase.client.core.msg.chunk.ChunkRow;
import com.couchbase.client.core.msg.chunk.ChunkTrailer;
import com.couchbase.client.core.msg.chunk.ChunkedResponse;
import com.couchbase.client.core.retry.RetryOrchestrator;
import com.couchbase.client.core.retry.RetryReason;

import java.util.Optional;

import static com.couchbase.client.core.io.netty.HandlerUtils.closeChannelWithReason;
import static com.couchbase.client.core.io.netty.TracingUtils.setCommonDispatchSpanAttributes;

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
   * If this message handler supports pipelining.
   */
  private final boolean pipelined;

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
   * Holds the current dispatch span.
   */
  private RequestSpan currentDispatchSpan;

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
   * Holds the start dispatch time for the current request.
   */
  private long dispatchTimingStart;

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
    this.pipelined = endpoint.pipelined();

    if (pipelined) {
      throw new CouchbaseException("The ChunkedMessageHandler does not support pipelining, this is a bug!");
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
    // We still have a request in-flight so we need to reschedule it in order to not let it trip on each others
    // toes.
    if (!pipelined && currentRequest != null) {
      RetryOrchestrator.maybeRetry(endpointContext, (REQ) msg, RetryReason.NOT_PIPELINED_REQUEST_IN_FLIGHT);
      if (endpoint != null) {
        endpoint.decrementOutstandingRequests();
      }
      return;
    }

    try {
      currentRequest = (REQ) msg;
      FullHttpRequest encoded = currentRequest.encode();
      encoded.headers().set(HttpHeaderNames.HOST, remoteHost);
      encoded.headers().set(HttpHeaderNames.USER_AGENT, endpointContext.environment().userAgent().formattedLong());
      chunkResponseParser.updateRequestContext(currentRequest.context());
      dispatchTimingStart = System.nanoTime();
      if (currentRequest.requestSpan() != null) {
        RequestTracer tracer = endpointContext.coreResources().requestTracer();
        currentDispatchSpan = tracer.requestSpan(TracingIdentifiers.SPAN_DISPATCH, currentRequest.requestSpan());

        if (!CbTracing.isInternalTracer(tracer)) {
          setCommonDispatchSpanAttributes(
            currentDispatchSpan,
            ctx.channel().attr(ChannelAttributes.CHANNEL_ID_KEY).get(),
            ioContext.localHostname(),
            ioContext.localPort(),
            endpoint.remoteHostname(),
            endpoint.remotePort(),
            currentRequest.operationId()
          );
        }
      }
      ctx.write(encoded, promise);
    } catch (Throwable t) {
      currentRequest.response().completeExceptionally(t);
      if (endpoint != null) {
        endpoint.decrementOutstandingRequests();
      }
    }
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    remoteHost = endpoint.remoteHostname() + ":" + endpoint.remotePort();
    ioContext = new IoContext(
      endpointContext,
      ctx.channel().localAddress(),
      ctx.channel().remoteAddress(),
      endpointContext.bucket()
    );
    ctx.fireChannelActive();
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    try {
      if (msg instanceof HttpResponse) {
        handleHttpResponse(ctx, (HttpResponse) msg);
      } else if (msg instanceof HttpContent) {
        ((HttpContent) msg).retain(); // Parser takes ownership; counteract the release in 'finally' block.
        handleHttpContent((HttpContent) msg);
        if (msg instanceof LastHttpContent) {
          chunkResponseParser.endOfInput();
          if (!isSuccess()) {
            maybeCompleteResponseWithFailure();
          }
          cleanupState();
          if (endpoint != null) {
            endpoint.markRequestCompletion();
          }
        }
      } else {
        ioContext.environment().eventBus().publish(
          new UnsupportedResponseTypeReceivedEvent(ioContext, msg)
        );
        closeChannelWithReason(ioContext, ctx, ChannelClosedProactivelyEvent.Reason.INVALID_RESPONSE_FORMAT_DETECTED);
      }
    } finally {
      ReferenceCountUtil.release(msg);
    }
  }

  @Override
  public void handlerRemoved(final ChannelHandlerContext ctx) {
    cleanupState();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    if (currentRequest != null) {
      RetryOrchestrator.maybeRetry(ioContext, currentRequest, RetryReason.CHANNEL_CLOSED_WHILE_IN_FLIGHT);
    }
    ctx.fireChannelInactive();
  }

  private void handleHttpResponse(final ChannelHandlerContext ctx, final HttpResponse msg) {
    currentRequest.context().dispatchLatency(System.nanoTime() - dispatchTimingStart);
    if (currentDispatchSpan != null) {
      currentDispatchSpan.end();
    }
    currentResponseStatus = msg;
    chunkResponseParser.updateResponseHeader(msg);
    convertedResponseStatus = HttpProtocol.decodeStatus(msg.status());
    chunkResponseParser.initialize(ctx.channel().config());
  }

  private void handleHttpContent(final HttpContent msg) {
    chunkResponseParser.feed(msg.content());

    boolean isLastChunk = msg instanceof LastHttpContent;
    if (currentResponse == null && isSuccess() && chunkResponseParser.header(isLastChunk).isPresent()) {
      completeInitialResponse(chunkResponseParser.header(isLastChunk).get());
    }
  }

  private boolean isSuccess() {
    return convertedResponseStatus.success() && !chunkResponseParser.decodingFailure().isPresent();
  }

  private void completeInitialResponse(final H header) {
    currentResponse = currentRequest.decode(
      convertedResponseStatus, header, chunkResponseParser.rows(), chunkResponseParser.trailer()
    );
    if (!currentRequest.completed()) {
      currentRequest.succeed(currentResponse);
    } else {
      ioContext.environment().orphanReporter().report(currentRequest);
    }
  }

  private void maybeCompleteResponseWithFailure() {
    if (!currentRequest.completed()) {
      final CouchbaseException cause = chunkResponseParser.decodingFailure().orElseGet(
        () -> chunkResponseParser.error().orElseGet(
          () -> new CouchbaseException("Request failed, but no more information available")));

      Optional<RetryReason> qualifies = qualifiesForRetry(cause);
      if (qualifies.isPresent()) {
        RetryOrchestrator.maybeRetry(ioContext, currentRequest, qualifies.get());
      } else {
        currentRequest.fail(cause);
      }
    } else {
      ioContext.environment().orphanReporter().report(currentRequest);
    }
  }

  /**
   * Can be implemented by children to not fail a request but rather send it into retry.
   *
   * @param exception the throwable to check.
   * @return a reason if it should be retried - if empty will fail the request.
   */
  protected Optional<RetryReason> qualifiesForRetry(CouchbaseException exception) {
    return Optional.empty();
  }

  private void cleanupState() {
    chunkResponseParser.cleanup();
    currentResponse = null;
    currentRequest = null;
    currentDispatchSpan = null;
    currentResponseStatus = null;
    dispatchTimingStart = 0;
  }

}
