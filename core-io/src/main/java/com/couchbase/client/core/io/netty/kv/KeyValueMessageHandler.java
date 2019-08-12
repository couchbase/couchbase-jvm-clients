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
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.events.config.CollectionMapRefreshFailedEvent;
import com.couchbase.client.core.cnc.events.io.ChannelClosedProactivelyEvent;
import com.couchbase.client.core.cnc.events.io.InvalidRequestDetectedEvent;
import com.couchbase.client.core.cnc.events.io.KeyValueErrorMapCodeHandledEvent;
import com.couchbase.client.core.cnc.events.io.UnknownResponseReceivedEvent;
import com.couchbase.client.core.cnc.events.io.UnknownResponseStatusReceivedEvent;
import com.couchbase.client.core.cnc.events.io.UnsupportedResponseTypeReceivedEvent;
import com.couchbase.client.core.config.ProposedBucketConfigContext;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.channel.ChannelDuplexHandler;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPromise;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.deps.io.netty.util.collection.IntObjectHashMap;
import com.couchbase.client.core.deps.io.netty.util.collection.IntObjectMap;
import com.couchbase.client.core.endpoint.BaseEndpoint;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.env.CompressionConfig;
import com.couchbase.client.core.error.DecodingFailedException;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import com.couchbase.client.core.retry.RetryOrchestrator;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.service.ServiceType;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.ErrorMap.ErrorAttribute.AUTH;
import static com.couchbase.client.core.io.netty.kv.ErrorMap.ErrorAttribute.CONN_STATE_INVALIDATED;
import static com.couchbase.client.core.io.netty.kv.ErrorMap.ErrorAttribute.ITEM_LOCKED;
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
   * Stores the current opaque value.
   */
  private int opaque;

  /**
   * Once connected/active, holds the channel context.
   */
  private ChannelContext channelContext;

  /**
   * If present, holds the error map negotiated on this connection.
   */
  private ErrorMap errorMap;

  /**
   * Creates a new {@link KeyValueMessageHandler}.
   *
   * @param endpointContext the parent core context.
   */
  public KeyValueMessageHandler(final BaseEndpoint endpoint,
                                final EndpointContext endpointContext, final Optional<String> bucketName) {
    this.endpoint = endpoint;
    this.endpointContext = endpointContext;
    this.writtenRequests = new IntObjectHashMap<>();
    this.writtenRequestDispatchTimings = new IntObjectHashMap<>();
    this.compressionConfig = endpointContext.environment().compressionConfig();
    this.eventBus = endpointContext.environment().eventBus();
    this.bucketName = bucketName;
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

    opaque = Utils.opaque(ctx.channel(), false);

    errorMap = ctx.channel().attr(ChannelAttributes.ERROR_MAP_KEY).get();

    List<ServerFeature> features = ctx.channel().attr(ChannelAttributes.SERVER_FEATURE_KEY).get();
    boolean compression = features != null && features.contains(ServerFeature.SNAPPY);
    boolean collections = features != null && features.contains(ServerFeature.COLLECTIONS);
    boolean mutationTokens = features != null && features.contains(ServerFeature.MUTATION_SEQNO);
    boolean syncReplication = features != null && features.contains(ServerFeature.SYNC_REPLICATION);
    boolean altRequest = features != null && features.contains(ServerFeature.ALT_REQUEST);

    if (syncReplication && !altRequest) {
      throw new IllegalStateException("If Synchronous Replication is enabled, the server also " +
        "must negotiate Alternate Requests. This is a bug! - please report.");
    }

    channelContext = new ChannelContext(
      compression ? compressionConfig : null,
      collections,
      mutationTokens,
      bucketName,
      syncReplication,
      altRequest,
      ioContext.core().configurationProvider().collectionMap()
    );

    ctx.fireChannelActive();
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
    if (msg instanceof KeyValueRequest) {
      KeyValueRequest<Response> request = (KeyValueRequest<Response>) msg;

      int nextOpaque;
      do {
        nextOpaque = ++opaque;
      } while (writtenRequests.containsKey(nextOpaque));

      writtenRequests.put(nextOpaque, request);
      try {
        ctx.write(request.encode(ctx.alloc(), nextOpaque, channelContext), promise);
        writtenRequestDispatchTimings.put(nextOpaque, (Long) System.nanoTime());
      }
      catch(RuntimeException err) {
        request.response().completeExceptionally(err);
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
    if (msg instanceof ByteBuf) {
      decode(ctx, (ByteBuf) msg);
    } else {
      ioContext.environment().eventBus().publish(
        new UnsupportedResponseTypeReceivedEvent(ioContext, msg)
      );
      ReferenceCountUtil.release(msg);
    }
  }

  /**
   * Main method to start dispatching the decode.
   *
   * @param response the response to decode and handle.
   */
  private void decode(final ChannelHandlerContext ctx, final ByteBuf response) {
    int opaque = MemcacheProtocol.opaque(response);
    KeyValueRequest<Response> request = writtenRequests.remove(opaque);
    long start = writtenRequestDispatchTimings.remove(opaque);

    if (request == null) {
      byte[] packet = new byte[response.readableBytes()];
      response.readBytes(packet);
      ioContext.environment().eventBus().publish(
        new UnknownResponseReceivedEvent(ioContext, packet)
      );
    } else {
      request.context().dispatchLatency(System.nanoTime() - start);

      short statusCode = MemcacheProtocol.status(response);
      ResponseStatus status = MemcacheProtocol.decodeStatus(statusCode);
      ErrorMap.ErrorCode errorCode = status == ResponseStatus.UNKNOWN ? decodeErrorCode(statusCode) : null;

      if (errorCode != null) {
        ioContext.environment().eventBus().publish(new KeyValueErrorMapCodeHandledEvent(ioContext, errorCode));
        status = handleErrorCode(ctx, errorCode);
      }

      if (status == ResponseStatus.UNKNOWN) {
        ioContext.environment().eventBus().publish(new UnknownResponseStatusReceivedEvent(ioContext, statusCode));
      }

      if (status == ResponseStatus.NOT_MY_VBUCKET) {
        handleNotMyVbucket(request, response);
      } else if (status == ResponseStatus.UNKNOWN_COLLECTION) {
        handleOutdatedCollection(request);
      } else {
        try {
          Response decoded = request.decode(response, channelContext);
          request.succeed(decoded);
        } catch (Throwable t) {
          request.fail(new DecodingFailedException(t));
        }
      }
    }

    if (endpoint != null) {
      endpoint.markRequestCompletion();
    }
    ReferenceCountUtil.release(response);
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
      ctx.channel().close();
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
  private void handleNotMyVbucket(final KeyValueRequest<Response> request, final ByteBuf response) {
    final String origin = request.context().dispatchedTo();
    RetryOrchestrator.retryImmediately(ioContext, request, RetryReason.KV_NOT_MY_VBUCKET);

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
  private void handleOutdatedCollection(final KeyValueRequest<Response> request) {
    final long start = System.nanoTime();
    ioContext.core().configurationProvider().refreshCollectionMap(request.bucket(), true).subscribe(v -> {}, err -> {
      Duration duration = Duration.ofNanos(System.nanoTime() - start);
      eventBus.publish(new CollectionMapRefreshFailedEvent(duration, ioContext, err));
    });
    RetryOrchestrator.retryImmediately(ioContext, request, RetryReason.KV_COLLECTION_OUTDATED);
  }

}
