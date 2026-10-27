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

package com.couchbase.client.core.service;

import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.events.io.ChannelClosedProactivelyEvent;
import com.couchbase.client.core.cnc.events.io.CollectionOutdatedHandledEvent;
import com.couchbase.client.core.cnc.events.io.KeyValueErrorMapCodeHandledEvent;
import com.couchbase.client.core.cnc.events.io.NotMyVbucketReceivedEvent;
import com.couchbase.client.core.cnc.events.io.UnknownResponseReceivedEvent;
import com.couchbase.client.core.cnc.events.io.UnknownResponseStatusReceivedEvent;
import com.couchbase.client.core.config.ProposedBucketConfigContext;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.RangeScanPartitionFailedException;
import com.couchbase.client.core.error.context.GenericRequestErrorContext;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.io.KeyValuePacket;
import com.couchbase.client.core.io.netty.TracingUtils;
import com.couchbase.client.core.io.netty.kv.ErrorMap;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.KeyValueMessageHandler;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import com.couchbase.client.core.msg.kv.RangeScanContinueRequest;
import com.couchbase.client.core.msg.kv.RangeScanContinueResponse;
import com.couchbase.client.core.retry.RetryOrchestrator;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.service.KeyValueConnection.InFlight;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.couchbase.client.core.io.netty.kv.ErrorMap.ErrorAttribute.CONN_STATE_INVALIDATED;
import static com.couchbase.client.core.io.netty.kv.ErrorMap.ErrorAttribute.FETCH_CONFIG;
import static java.util.Objects.requireNonNull;

/**
 * Handles a {@link KeyValueConnection}'s responses, like the Netty implementation's {@link KeyValueMessageHandler}:
 * completes the requests, or retries them (for example, after NOT_MY_VBUCKET, or an outdated collection),
 * and asks the connection to close when a response says to.
 * <p>
 * Used by the connection's reader thread (and by {@link KeyValueConnection#send}, for
 * {@link #handleOutdatedCollection}). Created once the handshake is done.
 */
@NullMarked
final class KeyValueResponseHandler {
  private static final Logger log = LoggerFactory.getLogger(KeyValueResponseHandler.class);

  /**
   * What the handler needs the connection to do.
   */
  @FunctionalInterface
  interface Connection {
    /**
     * Closes the connection (if it isn't already), and publishes a {@link ChannelClosedProactivelyEvent}.
     *
     * @param discardUnreadResponses true to not handle the responses already read, because they might complete
     * the wrong requests.
     */
    void closeProactively(ChannelClosedProactivelyEvent.Reason reason, boolean discardUnreadResponses);
  }

  private final EndpointContext endpointContext;
  private final IoContext ioContext;
  private final KeyValueChannelContext channelContext;
  private final @Nullable ErrorMap errorMap;
  private final Map<Integer, InFlight> inFlight;
  private final boolean isInternalTracer;
  private final Connection connection;
  private final EventBus eventBus;
  private final @Nullable String bucketName;

  /**
   * @param inFlight the connection's requests waiting for responses, by opaque. Shared with the connection.
   */
  KeyValueResponseHandler(
    EndpointContext endpointContext,
    IoContext ioContext,
    KeyValueChannelContext channelContext,
    @Nullable ErrorMap errorMap,
    Map<Integer, InFlight> inFlight,
    boolean isInternalTracer,
    Connection connection
  ) {
    this.endpointContext = requireNonNull(endpointContext);
    this.ioContext = requireNonNull(ioContext);
    this.channelContext = requireNonNull(channelContext);
    this.errorMap = errorMap;
    this.inFlight = requireNonNull(inFlight);
    this.isInternalTracer = isInternalTracer;
    this.connection = requireNonNull(connection);
    this.eventBus = endpointContext.environment().eventBus();
    this.bucketName = channelContext.bucket().orElse(null);
  }

  /**
   * Handles a response (not a server push).
   */
  void handle(KeyValuePacket packet) {
    int opaque = packet.opaque();
    InFlight written = inFlight.remove(opaque);

    if (written == null) {
      handleUnknownResponseReceived(packet);
      return;
    }
    KeyValueRequest<Response> request = written.request;

    completeRequestTimings(written, packet);

    short statusCode = packet.status();
    ResponseStatus status = MemcacheProtocol.decodeStatus(statusCode);
    ErrorMap.ErrorCode errorCode = status != ResponseStatus.SUCCESS ? decodeErrorCode(statusCode) : null;

    if (errorCode != null) {
      request.errorCode(errorCode);
    }

    boolean errorUnknown = false;
    if (status == ResponseStatus.UNKNOWN) {
      errorUnknown = true;

      if (errorCode != null) {
        eventBus.publish(new KeyValueErrorMapCodeHandledEvent(ioContext, errorCode));
        if (errorCode.attributes().contains(CONN_STATE_INVALIDATED)) {
          connection.closeProactively(ChannelClosedProactivelyEvent.Reason.KV_RESPONSE_CONTAINED_CLOSE_INDICATION, false);
        } else {
          status = KeyValueMessageHandler.statusForErrorCode(errorCode);
        }
      }

      eventBus.publish(new UnknownResponseStatusReceivedEvent(ioContext, statusCode));
    }

    boolean isRangeScanContinue = (Request<?>) request instanceof RangeScanContinueRequest;

    if (errorUnknown && errorCode != null && errorCode.attributes().contains(FETCH_CONFIG)) {
      log.warn("Refreshing cluster config in response to error code {} ; details={}", statusCode, errorCode);
      endpointContext.core().configurationProvider().signalNewTopologyAvailable(bucketName, null, endpointContext.remoteSocket());
    }

    if (status == ResponseStatus.NOT_MY_VBUCKET && !isRangeScanContinue) {
      handleNotMyVbucket(request, packet);
    } else if (status == ResponseStatus.UNKNOWN_COLLECTION) {
      handleOutdatedCollection(request, RetryReason.KV_COLLECTION_OUTDATED);
    } else if (errorUnknown && KeyValueMessageHandler.errorMapIndicatesRetry(errorCode)) {
      RetryOrchestrator.maybeRetry(ioContext, request, RetryReason.KV_ERROR_MAP_INDICATED);
    } else if (KeyValueMessageHandler.statusIndicatesInvalidChannel(status, bucketName != null)) {
      connection.closeProactively(ChannelClosedProactivelyEvent.Reason.KV_RESPONSE_CONTAINED_CLOSE_INDICATION, false);
      // Unlike the Netty implementation (which has already forgotten the request when it retries
      // the ones in flight, so it times out), retry it like the other requests in flight.
      RetryOrchestrator.maybeRetry(ioContext, request, RetryReason.CHANNEL_CLOSED_WHILE_IN_FLIGHT);
    } else {
      retryOrComplete(written, packet, status, isRangeScanContinue);
    }
  }

  private void retryOrComplete(
    InFlight written,
    KeyValuePacket packet,
    ResponseStatus status,
    boolean isRangeScanContinue
  ) {
    KeyValueRequest<Response> request = written.request;
    RetryReason retryReason = KeyValueMessageHandler.statusCodeIndicatesRetry(status, request);
    if (retryReason != null) {
      RetryOrchestrator.maybeRetry(ioContext, request, retryReason);
    } else if (isRangeScanContinue) {
      decodeAndCompleteRangeScanContinue(written, packet);
    } else if (!request.completed()) {
      decodeAndComplete(request, packet);
    } else {
      ioContext.environment().orphanReporter().report(request);
    }
  }

  private void completeRequestTimings(InFlight written, KeyValuePacket response) {
    KeyValueRequest<Response> request = written.request;
    long serverTime = response.serverDuration();
    request.context().serverLatency(serverTime);
    request.context().dispatchLatency(System.nanoTime() - written.dispatchStartNanos);

    RequestSpan dispatchSpan = written.dispatchSpan;
    if (dispatchSpan != null) {
      if (!isInternalTracer) {
        TracingUtils.setServerDurationAttribute(request.context().coreResources().tracingDecorator(), dispatchSpan, serverTime);
      }
      dispatchSpan.end();
    }
  }

  private void decodeAndComplete(KeyValueRequest<Response> request, KeyValuePacket packet) {
    try {
      Response decoded = request.decode(packet, channelContext);
      request.succeed(decoded);
    } catch (Throwable t) {
      request.fail(new DecodingFailureException(t.getMessage(), t, new GenericRequestErrorContext(request)));
    }
  }

  private void decodeAndCompleteRangeScanContinue(InFlight written, KeyValuePacket packet) {
    KeyValueRequest<Response> request = written.request;
    RangeScanContinueResponse decoded = (RangeScanContinueResponse) request.decode(packet, channelContext);
    if (!request.completed()) {
      request.succeed(decoded);
    }

    ResponseStatus status = decoded.status();
    boolean expectedStatus = status == ResponseStatus.COMPLETE
      || status == ResponseStatus.CONTINUE
      || status == ResponseStatus.SUCCESS;

    if (!expectedStatus) {
      decoded.failFeed(new RangeScanPartitionFailedException(
        "Stream continue failed with non-successful response status",
        status
      ));
      return;
    }

    boolean hasLastItem = status == ResponseStatus.COMPLETE;
    boolean completeStream = hasLastItem || status == ResponseStatus.CONTINUE;
    decoded.feedItems(packet.body(), hasLastItem, completeStream);

    if (decoded.status() == ResponseStatus.SUCCESS) {
      // More responses are coming for this request.
      inFlight.put(request.opaque(), new InFlight(request, written.dispatchStartNanos, null));
    }
  }

  private void handleUnknownResponseReceived(KeyValuePacket response) {
    eventBus.publish(new UnknownResponseReceivedEvent(ioContext, response.toByteArray()));
    // We got a response with an opaque value that we know nothing about. There is clearly something weird
    // going on so to be sure we close the connection to avoid any further weird situations.
    // Unlike the Netty implementation, doesn't handle the rest of the responses already read: they might
    // complete the wrong requests.
    connection.closeProactively(ChannelClosedProactivelyEvent.Reason.KV_RESPONSE_CONTAINED_UNKNOWN_OPAQUE, true);
  }

  private ErrorMap.@Nullable ErrorCode decodeErrorCode(short statusCode) {
    ErrorMap map = errorMap;
    return map != null ? map.errors().get(statusCode) : null;
  }

  private void handleNotMyVbucket(KeyValueRequest<? extends Response> request, KeyValuePacket response) {
    request.indicateRejectedWithNotMyVbucket();

    eventBus.publish(new NotMyVbucketReceivedEvent(ioContext, request.partition()));

    String origin = request.context().lastDispatchedTo() != null ? request.context().lastDispatchedTo().host() : null;
    RetryOrchestrator.maybeRetry(ioContext, request, RetryReason.KV_NOT_MY_VBUCKET);

    String body = response.bodyAsString().trim();
    if (body.startsWith("{")) {
      ioContext.core().configurationProvider().proposeBucketConfig(
        new ProposedBucketConfigContext(request.bucket(), body, origin)
      );
    }
  }

  void handleOutdatedCollection(KeyValueRequest<? extends Response> request, RetryReason retryReason) {
    eventBus.publish(new CollectionOutdatedHandledEvent(
      request.collectionIdentifier(),
      retryReason,
      new KeyValueMessageHandler.OutdatedCollectionContext(ioContext, ioContext.core().configurationProvider().collectionMap())
    ));

    ioContext.core().configurationProvider().refreshCollectionId(request.collectionIdentifier());
    RetryOrchestrator.maybeRetry(ioContext, request, retryReason);
  }
}
