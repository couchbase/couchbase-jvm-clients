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

package com.couchbase.client.core.io.netty.kv;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.cnc.events.io.ChannelClosedProactivelyEvent;
import com.couchbase.client.core.cnc.events.io.CollectionOutdatedHandledEvent;
import com.couchbase.client.core.cnc.events.io.InvalidRequestDetectedEvent;
import com.couchbase.client.core.cnc.events.io.KeyValueErrorMapCodeHandledEvent;
import com.couchbase.client.core.cnc.events.io.NotMyVbucketReceivedEvent;
import com.couchbase.client.core.cnc.events.io.UnknownResponseReceivedEvent;
import com.couchbase.client.core.cnc.events.io.UnknownResponseStatusReceivedEvent;
import com.couchbase.client.core.cnc.events.io.UnsupportedResponseTypeReceivedEvent;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.MemcachedBucketConfig;
import com.couchbase.client.core.config.ProposedBucketConfigContext;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.deps.io.netty.channel.ChannelDuplexHandler;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPromise;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.deps.io.netty.util.collection.IntObjectHashMap;
import com.couchbase.client.core.deps.io.netty.util.collection.IntObjectMap;
import com.couchbase.client.core.endpoint.BaseEndpoint;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.env.CompressionConfig;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.RangeScanIdFailureException;
import com.couchbase.client.core.error.RangeScanPartitionFailedException;
import com.couchbase.client.core.io.CollectionMap;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.io.netty.TracingUtils;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import com.couchbase.client.core.msg.kv.RangeScanContinueRequest;
import com.couchbase.client.core.msg.kv.RangeScanContinueResponse;
import com.couchbase.client.core.msg.kv.UnlockRequest;
import com.couchbase.client.core.retry.RetryOrchestrator;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.UnsignedLEB128;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.couchbase.client.core.deps.io.netty.buffer.Unpooled.EMPTY_BUFFER;
import static com.couchbase.client.core.io.netty.HandlerUtils.closeChannelWithReason;
import static com.couchbase.client.core.io.netty.TracingUtils.setCommonDispatchSpanAttributes;
import static com.couchbase.client.core.io.netty.TracingUtils.setCommonKVSpanAttributes;
import static com.couchbase.client.core.io.netty.TracingUtils.setNumericOperationId;
import static com.couchbase.client.core.io.netty.kv.ErrorMap.ErrorAttribute.AUTH;
import static com.couchbase.client.core.io.netty.kv.ErrorMap.ErrorAttribute.CONN_STATE_INVALIDATED;
import static com.couchbase.client.core.io.netty.kv.ErrorMap.ErrorAttribute.ITEM_LOCKED;
import static com.couchbase.client.core.io.netty.kv.ErrorMap.ErrorAttribute.RETRY_LATER;
import static com.couchbase.client.core.io.netty.kv.ErrorMap.ErrorAttribute.RETRY_NOW;
import static com.couchbase.client.core.io.netty.kv.ErrorMap.ErrorAttribute.TEMP;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.body;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This handler is responsible for writing KV requests and completing their associated responses
 * once they arrive.
 *
 * @since 2.0.0
 */
public class KeyValueMessageHandler extends ChannelDuplexHandler {

  /**
   * Stores the {@link CoreContext} for use.
   */
  private final EndpointContext endpointContext;

  /**
   * Holds all outstanding requests based on their opaque.
   */
  private final IntObjectMap<KeyValueRequest<Response>> writtenRequests;

  /**
   * Holds all outstanding requests based on their opaque.
   */
  private final IntObjectMap<RequestSpan> writtenRequestDispatchSpans;

  /**
   * Holds the start timestamps for the outstanding dispatched requests.
   */
  private final IntObjectMap<Long> writtenRequestDispatchTimings;

  /**
   * The compression config used for this handler.
   */
  private final CompressionConfig compressionConfig;

  /**
   * The event bus used to signal events.
   */
  private final EventBus eventBus;

  /**
   * The name of the bucket.
   */
  private final Optional<String> bucketName;

  /**
   * The surrounding endpoint.
   */
  private final BaseEndpoint endpoint;

  /**
   * Stores the current IO context.
   */
  private IoContext ioContext;

  /**
   * Once connected/active, holds the channel context.
   */
  private KeyValueChannelContext channelContext;

  /**
   * If present, holds the error map negotiated on this connection.
   */
  private ErrorMap errorMap;

  /**
   * Knows if the tracer is an internal or external one for optimizations.
   */
  private final boolean isInternalTracer;

  /**
   * Creates a new {@link KeyValueMessageHandler}.
   *
   * @param endpointContext the parent core context.
   */
  public KeyValueMessageHandler(final BaseEndpoint endpoint, final EndpointContext endpointContext,
                                final Optional<String> bucketName) {
    this.endpoint = endpoint;
    this.endpointContext = endpointContext;
    this.writtenRequests = new IntObjectHashMap<>();
    this.writtenRequestDispatchTimings = new IntObjectHashMap<>();
    this.writtenRequestDispatchSpans = new IntObjectHashMap<>();
    this.compressionConfig = endpointContext.environment().compressionConfig();
    this.eventBus = endpointContext.environment().eventBus();
    this.bucketName = bucketName;
    this.isInternalTracer = CbTracing.isInternalTracer(endpointContext.environment().requestTracer());
  }

  /**
   * Actions to be performed when the channel becomes active.
   *
   * <p>Since the opaque is incremented in the handler below during bootstrap but now is
   * only modified in this handler, cache the reference since the attribute lookup is
   * more costly.</p>
   *
   * @param ctx the channel context.
   */
  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    ioContext = new IoContext(
      endpointContext,
      ctx.channel().localAddress(),
      ctx.channel().remoteAddress(),
      endpointContext.bucket()
    );

    errorMap = ctx.channel().attr(ChannelAttributes.ERROR_MAP_KEY).get();

    Set<ServerFeature> features = ctx.channel().attr(ChannelAttributes.SERVER_FEATURE_KEY).get();
    boolean compression = features != null && features.contains(ServerFeature.SNAPPY);
    boolean collections = features != null && features.contains(ServerFeature.COLLECTIONS);
    boolean mutationTokens = features != null && features.contains(ServerFeature.MUTATION_SEQNO);
    boolean syncReplication = features != null && features.contains(ServerFeature.SYNC_REPLICATION);
    boolean altRequest = features != null && features.contains(ServerFeature.ALT_REQUEST);
    boolean vattrEnabled = features != null && features.contains(ServerFeature.VATTR);
    boolean createAsDeleted = features != null && features.contains(ServerFeature.CREATE_AS_DELETED);
    boolean preserveTtl = features != null && features.contains(ServerFeature.PRESERVE_TTL);

    if (syncReplication && !altRequest) {
      throw new IllegalStateException("If Synchronous Replication is enabled, the server also " +
        "must negotiate Alternate Requests. This is a bug! - please report.");
    }

    channelContext = new KeyValueChannelContext(
      compression ? compressionConfig : null,
      collections,
      mutationTokens,
      bucketName,
      syncReplication,
      vattrEnabled,
      altRequest,
      ioContext.core().configurationProvider().collectionMap(),
      ctx.channel().id(),
      createAsDeleted,
      preserveTtl
    );

    ctx.fireChannelActive();
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
    if (msg instanceof KeyValueRequest) {
      KeyValueRequest<Response> request = (KeyValueRequest<Response>) msg;

      int opaque = request.opaque();
      writtenRequests.put(opaque, request);
      try {
        ctx.write(request.encode(ctx.alloc(), opaque, channelContext), promise);
        writtenRequestDispatchTimings.put(opaque, (Long) System.nanoTime());
        if (request.requestSpan() != null) {
          RequestTracer tracer = endpointContext.environment().requestTracer();
          RequestSpan dispatchSpan = tracer.requestSpan(TracingIdentifiers.SPAN_DISPATCH, request.requestSpan());

          if (!isInternalTracer) {
            setCommonDispatchSpanAttributes(
              dispatchSpan,
              ctx.channel().attr(ChannelAttributes.CHANNEL_ID_KEY).get(),
              ioContext.localHostname(),
              ioContext.localPort(),
              endpoint.remoteHostname(),
              endpoint.remotePort(),
              null
            );
            setNumericOperationId(dispatchSpan, request.opaque());
            setCommonKVSpanAttributes(dispatchSpan, request);
          }

          writtenRequestDispatchSpans.put(opaque, dispatchSpan);
        }

      } catch (Throwable err) {
        writtenRequests.remove(opaque);
        if (err instanceof CollectionNotFoundException) {
          if (channelContext.collectionsEnabled()) {
            ConfigurationProvider cp = ioContext.core().configurationProvider();
            if (cp.collectionRefreshInProgress(request.collectionIdentifier())) {
              RetryOrchestrator.maybeRetry(ioContext, request, RetryReason.COLLECTION_MAP_REFRESH_IN_PROGRESS);
            } else if (cp.config().bucketConfig(request.bucket()) instanceof MemcachedBucketConfig) {
              request.fail(FeatureNotAvailableException.collectionsForMemcached());
            } else {
              handleOutdatedCollection(request, RetryReason.COLLECTION_NOT_FOUND);
            }
            return;
          }
        }
        request.fail(err);
      }
    } else {
      eventBus.publish(new InvalidRequestDetectedEvent(ioContext, ServiceType.KV, msg));
      ctx.channel().close().addListener(f -> eventBus.publish(new ChannelClosedProactivelyEvent(
        ioContext,
        ChannelClosedProactivelyEvent.Reason.INVALID_REQUEST_DETECTED)
      ));
    }
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    try {
      if (msg instanceof ByteBuf) {
        decode(ctx, (ByteBuf) msg);
      } else {
        ioContext.environment().eventBus().publish(
          new UnsupportedResponseTypeReceivedEvent(ioContext, msg)
        );
        closeChannelWithReason(ioContext, ctx, ChannelClosedProactivelyEvent.Reason.INVALID_RESPONSE_FORMAT_DETECTED);
      }
    } finally {
      if (endpoint != null) {
        endpoint.markRequestCompletion();
      }
      ReferenceCountUtil.release(msg);
    }
  }

  @Override
  public void channelInactive(final ChannelHandlerContext ctx) {
    for (KeyValueRequest<? extends  Response> request : writtenRequests.values()) {
      RetryOrchestrator.maybeRetry(ioContext, request, RetryReason.CHANNEL_CLOSED_WHILE_IN_FLIGHT);
    }
    ctx.fireChannelInactive();
  }

  /**
   * Main method to start decoding the response.
   *
   * @param ctx the channel handler context from netty.
   * @param response the response to decode and handle.
   */
  private void decode(final ChannelHandlerContext ctx, final ByteBuf response) {
    int opaque = MemcacheProtocol.opaque(response);
    KeyValueRequest<Response> request = writtenRequests.remove(opaque);

    if (request == null) {
      handleUnknownResponseReceived(ctx, response);
      return;
    }

    long originalStart = completeRequestTimings(request, response, opaque);

    short statusCode = MemcacheProtocol.status(response);
    ResponseStatus status = MemcacheProtocol.decodeStatus(statusCode);
    ErrorMap.ErrorCode errorCode = status != ResponseStatus.SUCCESS ? decodeErrorCode(statusCode) : null;

    if (errorCode != null) {
      request.errorCode(errorCode);
    }

    boolean errorUnknown = false;
    if (status == ResponseStatus.UNKNOWN) {
      errorUnknown = true;

      if (errorCode != null) {
        ioContext.environment().eventBus().publish(new KeyValueErrorMapCodeHandledEvent(ioContext, errorCode));
        status = handleErrorCode(ctx, errorCode);
      }

      ioContext.environment().eventBus().publish(new UnknownResponseStatusReceivedEvent(ioContext, statusCode));
    }

    boolean isRangeScanContinue = (Request<?>) request instanceof RangeScanContinueRequest;

    if (status == ResponseStatus.NOT_MY_VBUCKET && !isRangeScanContinue) {
      handleNotMyVbucket(request, response);
    } else if (status == ResponseStatus.UNKNOWN_COLLECTION) {
      handleOutdatedCollection(request, RetryReason.KV_COLLECTION_OUTDATED);
    } else if (errorUnknown && errorMapIndicatesRetry(errorCode)) {
      RetryOrchestrator.maybeRetry(ioContext, request, RetryReason.KV_ERROR_MAP_INDICATED);
    } else if (statusIndicatesInvalidChannel(status)) {
      closeChannelWithReason(ioContext, ctx, ChannelClosedProactivelyEvent.Reason.KV_RESPONSE_CONTAINED_CLOSE_INDICATION);
    } else {
      retryOrComplete(request, response, status, isRangeScanContinue, originalStart);
    }
  }

  /**
   * If an operation does not need special handling, this method is most likely called and will complete the
   * operation or retry it if needed.
   *
   * @param request the original request.
   * @param response the response received for the request.
   * @param status the parsed status code.
   */
  private void retryOrComplete(final KeyValueRequest<Response> request, final ByteBuf response,
                               final ResponseStatus status, final boolean isRangeScanContinue, final long start) {
    RetryReason retryReason = statusCodeIndicatesRetry(status, request);
    if (retryReason == null) {
      if (isRangeScanContinue) {
        decodeAndCompleteRangeScanContinue(request, response, start);
        // TODO: where to orphan?
      } else {
        if (!request.completed()) {
          decodeAndComplete(request, response);
        } else {
          ioContext.environment().orphanReporter().report(request);
        }
      }
    } else {
      RetryOrchestrator.maybeRetry(ioContext, request, retryReason);
    }
  }

  /**
   * Helper method to complete request timings and dispatch spans.
   *
   * @param request the request to complete.
   * @param response the response to complete.
   * @param opaque the opaque for the request.
   */
  private long completeRequestTimings(final KeyValueRequest<Response> request, final ByteBuf response,
                                      final int opaque) {
    long serverTime = MemcacheProtocol.parseServerDurationFromResponse(response);
    request.context().serverLatency(serverTime);

    long start = writtenRequestDispatchTimings.remove(opaque);
    request.context().dispatchLatency(System.nanoTime() - start);

    RequestSpan dispatchSpan = writtenRequestDispatchSpans.remove(opaque);
    if (dispatchSpan != null) {
      if (!isInternalTracer) {
        TracingUtils.setServerDurationAttribute(dispatchSpan, serverTime);
      }
      dispatchSpan.end();
    }

    return start;
  }

  /**
   * Tries to decode the response and succeed the request.
   * <p>
   * If decoding fails, will fail the underlying request as well.
   *
   * @param request the request to decode and complete.
   * @param response the raw response to decode.
   */
  private void decodeAndComplete(final KeyValueRequest<Response> request, final ByteBuf response) {
    try {
      Response decoded = request.decode(response, channelContext);
      request.succeed(decoded);
    } catch (Throwable t) {
      request.fail(new DecodingFailureException(t));
    }
  }

  private void decodeAndCompleteRangeScanContinue(final KeyValueRequest<Response> request, final ByteBuf response,
                                                  long originalStart) {
    RangeScanContinueResponse decoded = (RangeScanContinueResponse) request.decode(response, channelContext);
    if (!request.completed()) {
      request.succeed(decoded);
    }

    ResponseStatus status = decoded.status();

    boolean expectedStatus = status == ResponseStatus.COMPLETE
      || status == ResponseStatus.CONTINUE
      || status == ResponseStatus.SUCCESS;

    if (!expectedStatus) {
      // (daschl): I don't think failing the stream here makes a big difference for i.e. NMVB responses since
      // the outer request is already completed above with the status code. I still thought it makes sense to
      // properly close the item stream just in case someone is streaming from it, and we have a chance to debug
      // this scenario.
      decoded.failFeed(new RangeScanPartitionFailedException(
        "Stream continue failed with non-successful response status",
        status
      ));
      return;
    }

    boolean hasLastItem = status == ResponseStatus.COMPLETE;
    boolean completeStream = hasLastItem || status == ResponseStatus.CONTINUE;
    decoded.feedItems(MemcacheProtocol.body(response).orElse(EMPTY_BUFFER), hasLastItem, completeStream);

    if (decoded.status() == ResponseStatus.SUCCESS) {
      writtenRequests.put(request.opaque(), request);
      writtenRequestDispatchTimings.put(request.opaque(), (Long) originalStart);
    }
  }

  /**
   * If certain status codes are returned from the server, there is a clear indication that the channel is
   * invalid and needs to be closed in order to avoid any further trouble.
   *
   * @param status the status code to check.
   * @return true if it indicates an invalid channel that needs to be closed.
   */
  private boolean statusIndicatesInvalidChannel(final ResponseStatus status) {
    return status == ResponseStatus.INTERNAL_SERVER_ERROR
      || (status == ResponseStatus.NO_BUCKET && bucketName.isPresent())
      || status == ResponseStatus.NOT_INITIALIZED;
  }

  /**
   * Helper method to perform some debug and cleanup logic if a response is received which we didn't expect.
   *
   * @param ctx the channel handler context from netty.
   * @param response the response to decode and handle.
   */
  private void handleUnknownResponseReceived(final ChannelHandlerContext ctx, final ByteBuf response) {
    byte[] packet = ByteBufUtil.getBytes(response);
    ioContext.environment().eventBus().publish(
      new UnknownResponseReceivedEvent(ioContext, packet)
    );
    // We got a response with an opaque value that we know nothing about. There is clearly something weird
    // going on so to be sure we close the connection to avoid any further weird situations.
    closeChannelWithReason(ioContext, ctx, ChannelClosedProactivelyEvent.Reason.KV_RESPONSE_CONTAINED_UNKNOWN_OPAQUE);
  }

  /**
   * If an error code has been found, this method tries to analyze it and perform the right
   * side effects.
   *
   * @param ctx the channel context.
   * @param errorCode the error code to handle
   * @return the new status code for the client to use.
   */
  private ResponseStatus handleErrorCode(final ChannelHandlerContext ctx, final ErrorMap.ErrorCode errorCode) {
    if (errorCode.attributes().contains(CONN_STATE_INVALIDATED)) {
      closeChannelWithReason(ioContext, ctx, ChannelClosedProactivelyEvent.Reason.KV_RESPONSE_CONTAINED_CLOSE_INDICATION);
      return ResponseStatus.UNKNOWN;
    }

    if (errorCode.attributes().contains(TEMP)) {
      return ResponseStatus.TEMPORARY_FAILURE;
    }

    if (errorCode.attributes().contains(AUTH)) {
      return ResponseStatus.NO_ACCESS;
    }

    if (errorCode.attributes().contains(ITEM_LOCKED)) {
      return ResponseStatus.LOCKED;
    }

    return ResponseStatus.UNKNOWN;
  }

  /**
   * Helper method to check if the consulted kv error map indicates a retry condition.
   *
   * @param errorCode the error code to handle
   * @return true if retry is indicated.
   */
  private boolean errorMapIndicatesRetry(final ErrorMap.ErrorCode errorCode) {
    return errorCode != null && (errorCode.attributes().contains(RETRY_NOW) || errorCode.attributes().contains(RETRY_LATER));
  }

  /**
   * Certain error codes can be transparently retried in the client instead of being raised to the user.
   * <p>
   * Note this special case where LOCKED returned for unlock should NOT be retried, because it does not make
   * sense (someone else unlocked the document). Usually this is turned into a CAS mismatch at the higher
   * levels.
   *
   * @param status the status code to check.
   * @return the retry reason that indicates the retry.
   */
  private RetryReason statusCodeIndicatesRetry(final ResponseStatus status, final Request<?> request) {
    switch (status) {
      case LOCKED:
        return request instanceof UnlockRequest ? null : RetryReason.KV_LOCKED;
      case TEMPORARY_FAILURE:
        return RetryReason.KV_TEMPORARY_FAILURE;
      case SYNC_WRITE_IN_PROGRESS:
        return RetryReason.KV_SYNC_WRITE_IN_PROGRESS;
      case SYNC_WRITE_RE_COMMIT_IN_PROGRESS:
        return RetryReason.KV_SYNC_WRITE_RE_COMMIT_IN_PROGRESS;
      default:
        return null;
    }
  }

  /**
   * Helper method to try to decode the status code into the error map code if possible.
   *
   * @param statusCode the status code to decode.
   * @return the error code if found, null otherwise.
   */
  private ErrorMap.ErrorCode decodeErrorCode(final short statusCode) {
    return errorMap != null ? errorMap.errors().get(statusCode) : null;
  }

  /**
   * Helper method to handle a "not my vbucket" response.
   *
   * @param request the request to retry.
   * @param response the response to extract the config from, potentially.
   */
  private void handleNotMyVbucket(final KeyValueRequest<? extends Response> request, final ByteBuf response) {
    request.indicateRejectedWithNotMyVbucket();

    eventBus.publish(new NotMyVbucketReceivedEvent(ioContext, request.partition()));

    final String origin = request.context().lastDispatchedTo() != null ? request.context().lastDispatchedTo().host() : null;
    RetryOrchestrator.maybeRetry(ioContext, request, RetryReason.KV_NOT_MY_VBUCKET);

    body(response)
      .map(b -> b.toString(UTF_8).trim())
      .filter(c -> c.startsWith("{"))
      .ifPresent(c -> ioContext.core().configurationProvider().proposeBucketConfig(
        new ProposedBucketConfigContext(request.bucket(), c, origin)
      ));
  }

  /**
   * Helper method to redispatch a request and signal that we need to refresh the collection map.
   *
   * @param request the request to retry.
   */
  private void handleOutdatedCollection(final KeyValueRequest<? extends Response> request, final RetryReason retryReason) {
    eventBus.publish(new CollectionOutdatedHandledEvent(
      request.collectionIdentifier(),
      retryReason,
      new OutdatedCollectionContext(ioContext, ioContext.core().configurationProvider().collectionMap())
    ));

    ioContext.core().configurationProvider().refreshCollectionId(request.collectionIdentifier());
    RetryOrchestrator.maybeRetry(ioContext, request, retryReason);
  }

  static class OutdatedCollectionContext extends IoContext {

    private final CollectionMap collectionMap;

    public OutdatedCollectionContext(IoContext ioContext, CollectionMap collectionMap) {
      super(ioContext, ioContext.localSocket(), ioContext.remoteSocket(), ioContext.bucket());
      this.collectionMap = collectionMap;
    }

    @Override
    public void injectExportableParams(final Map<String, Object> input) {
      super.injectExportableParams(input);

      input.put("open", collectionMap.inner().entrySet().stream().map(e -> {
        Map<String, Object> converted = new HashMap<>(e.getKey().toMap());
        converted.put("id", "0x" + Long.toHexString(UnsignedLEB128.decode(e.getValue())));
        return converted;
      }).collect(Collectors.toList()));
    }
  }

}
