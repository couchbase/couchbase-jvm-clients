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

import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.cnc.events.io.ChannelClosedProactivelyEvent;
import com.couchbase.client.core.cnc.events.io.InvalidRequestDetectedEvent;
import com.couchbase.client.core.cnc.events.io.UnsupportedResponseTypeReceivedEvent;
import com.couchbase.client.core.deps.io.netty.channel.ChannelDuplexHandler;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandler;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPromise;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaderNames;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpObjectAggregator;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.endpoint.BaseEndpoint;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.error.HttpStatusCodeException;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.io.netty.chunk.ChunkedMessageHandler;
import com.couchbase.client.core.io.netty.kv.ChannelAttributes;
import com.couchbase.client.core.msg.NonChunkedHttpRequest;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryOrchestrator;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.service.ServiceType;

import java.nio.charset.StandardCharsets;

import static com.couchbase.client.core.io.netty.HandlerUtils.closeChannelWithReason;
import static com.couchbase.client.core.io.netty.TracingUtils.setCommonDispatchSpanAttributes;

/**
 * This message handler can be considered the opposite of the {@link ChunkedMessageHandler}.
 *
 * <p>This generic implementation makes sure that when it lives in the pipeline it also pulls in the http
 * aggregator and sends full http requests / receives full http responses.</p>
 *
 * <p>You usually want to add this handler for non-perf critical messages like creating indexes and similar, so
 * their encoder and decoder implementations are considerably simpler than having to deal with chunking and a
 * streaming parser.</p>
 *
 * @since 2.0.0
 */
@ChannelHandler.Sharable
public abstract class NonChunkedHttpMessageHandler extends ChannelDuplexHandler {

  public static final String IDENTIFIER = NonChunkedHttpMessageHandler.class.getSimpleName();
  private static final String AGG_IDENTIFIER = HttpObjectAggregator.class.getSimpleName();

  private final EventBus eventBus;
  private final ServiceType serviceType;

  /**
   * Holds the current request.
   */
  private NonChunkedHttpRequest<Response> currentRequest;

  /**
   * Holds the current dispatch span.
   */
  private RequestSpan currentDispatchSpan;

  /**
   * Stores the remote host for caching purposes.
   */
  private String remoteHost;

  /**
   * Stores the current IO context.
   */
  private IoContext ioContext;

  /**
   * The upper endpoint.
   */
  private final BaseEndpoint endpoint;

  /**
   * The query endpoint context.
   */
  private final EndpointContext endpointContext;

  /**
   * Holds the start dispatch time for the current request.
   */
  private long dispatchTimingStart;

  /**
   * Once active holds additional channel context for decoding.
   */
  private HttpChannelContext channelContext;

  protected NonChunkedHttpMessageHandler(final BaseEndpoint endpoint, final ServiceType serviceType) {
    this.endpoint = endpoint;
    this.endpointContext = endpoint.context();
    this.eventBus = endpointContext.environment().eventBus();
    this.serviceType = serviceType;
  }

  /**
   * To be implemented by children, should return the proper service exception type for each.
   *
   * @param content the raw full content body of the response if not successful.
   * @return the exception with which the request will be failed.
   */
  protected Exception failRequestWith(HttpResponseStatus status, String content, NonChunkedHttpRequest<Response> request) {
    return failRequestWithHttpStatusCodeException(status, content, request);
  }

  private Exception failRequestWithHttpStatusCodeException(HttpResponseStatus status, String content, NonChunkedHttpRequest<Response> request) {
    return new HttpStatusCodeException(status, content, request, null);
  }

  /**
   * Writes a given request and encodes it.
   *
   * @param ctx the channel handler context.
   * @param msg the msg to write.
   * @param promise the promise that will be passed along.
   */
  @Override
  @SuppressWarnings("unchecked")
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
    // We still have a request in-flight so we need to reschedule it in order to not let it trip on each others
    // toes.
    if (currentRequest != null) {
      RetryOrchestrator.maybeRetry(endpointContext, (Request<? extends Response>) msg, RetryReason.NOT_PIPELINED_REQUEST_IN_FLIGHT);
      if (endpoint != null) {
        endpoint.decrementOutstandingRequests();
      }
      return;
    }

    if (msg instanceof NonChunkedHttpRequest) {
      try {
        currentRequest = (NonChunkedHttpRequest<Response>) msg;
        FullHttpRequest encoded = ((NonChunkedHttpRequest<Response>) msg).encode();
        encoded.headers().set(HttpHeaderNames.HOST, remoteHost);
        encoded.headers().set(HttpHeaderNames.USER_AGENT, endpointContext.environment().userAgent().formattedLong());
        dispatchTimingStart = System.nanoTime();
        if (currentRequest.requestSpan() != null) {
          RequestTracer tracer = endpointContext.environment().requestTracer();
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
    } else {
      if (endpoint != null) {
        endpoint.decrementOutstandingRequests();
      }
      eventBus.publish(new InvalidRequestDetectedEvent(ioContext, serviceType, msg));
      ctx.channel().close().addListener(f -> eventBus.publish(new ChannelClosedProactivelyEvent(
        ioContext,
        ChannelClosedProactivelyEvent.Reason.INVALID_REQUEST_DETECTED)
      ));
    }
  }

  /**
   * When this channel is marked active it also needs to propagate that to the aggregator.
   *
   * @param ctx the channel handler context.
   */
  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    ioContext = new IoContext(
      endpointContext,
      ctx.channel().localAddress(),
      ctx.channel().remoteAddress(),
      endpointContext.bucket()
    );

    channelContext = new HttpChannelContext(ctx.channel().id());

    remoteHost = endpoint.remoteHostname() + ":" + endpoint.remotePort();
    ctx.pipeline().get(HttpObjectAggregator.class).channelActive(ctx);
    ctx.fireChannelActive();
  }

  /**
   * Parses the full http response and sends it to decode into the request.
   *
   * @param ctx the channel handler context.
   * @param msg the FullHttpResponse from the server.
   */
  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    try {
      if (msg instanceof FullHttpResponse) {
        try {
          currentRequest.context().dispatchLatency(System.nanoTime() - dispatchTimingStart);
          if (currentDispatchSpan != null) {
            currentDispatchSpan.end();
          }
            FullHttpResponse httpResponse = (FullHttpResponse) msg;
            ResponseStatus responseStatus = HttpProtocol.decodeStatus(httpResponse.status());
            if (!currentRequest.completed()) {
              if (responseStatus == ResponseStatus.SUCCESS) {
                Response response = currentRequest.decode(httpResponse, channelContext);
                currentRequest.succeed(response);
              } else {
                String body = httpResponse.content().toString(StandardCharsets.UTF_8);
                Exception error = currentRequest.bypassExceptionTranslation()
                    ? failRequestWithHttpStatusCodeException(httpResponse.status(), body, currentRequest)
                    : failRequestWith(httpResponse.status(), body, currentRequest);
                currentRequest.fail(error);
              }
            } else {
              ioContext.environment().orphanReporter().report(currentRequest);
            }
        } catch (Throwable ex) {
          currentRequest.fail(ex);
        } finally {
          currentRequest = null;
          currentDispatchSpan = null;
          endpoint.markRequestCompletion();
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

  /**
   * When the non-chunked handler is added, it also needs to add the http aggregator.
   *
   * @param ctx the channel handler context.
   */
  @Override
  public void handlerAdded(final ChannelHandlerContext ctx) {
    ctx.pipeline().addBefore(IDENTIFIER, AGG_IDENTIFIER, new HttpObjectAggregator(Integer.MAX_VALUE));
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    if (currentRequest != null) {
      RetryOrchestrator.maybeRetry(ioContext, currentRequest, RetryReason.CHANNEL_CLOSED_WHILE_IN_FLIGHT);
    }
    ctx.fireChannelInactive();
  }

  /**
   * When the non-chunked handler is removed, it also needs to remove its http aggregator.
   *
   * @param ctx the channel handler context.
   */
  @Override
  public void handlerRemoved(final ChannelHandlerContext ctx) {
    ctx.pipeline().remove(HttpObjectAggregator.class);
  }

}
