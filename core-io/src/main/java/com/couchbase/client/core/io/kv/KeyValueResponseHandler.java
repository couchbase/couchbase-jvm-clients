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

package com.couchbase.client.core.io.kv;

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
import com.couchbase.client.core.io.CollectionMap;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.io.netty.TracingUtils;
import com.couchbase.client.core.io.netty.kv.ErrorMap;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import com.couchbase.client.core.msg.kv.RangeScanContinueRequest;
import com.couchbase.client.core.msg.kv.RangeScanContinueResponse;
import com.couchbase.client.core.msg.kv.UnlockRequest;
import com.couchbase.client.core.retry.RetryOrchestrator;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.util.UnsignedLEB128;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static com.couchbase.client.core.io.netty.kv.ErrorMap.ErrorAttribute.AUTH;
import static com.couchbase.client.core.io.netty.kv.ErrorMap.ErrorAttribute.CONN_STATE_INVALIDATED;
import static com.couchbase.client.core.io.netty.kv.ErrorMap.ErrorAttribute.FETCH_CONFIG;
import static com.couchbase.client.core.io.netty.kv.ErrorMap.ErrorAttribute.ITEM_LOCKED;
import static com.couchbase.client.core.io.netty.kv.ErrorMap.ErrorAttribute.RETRY_LATER;
import static com.couchbase.client.core.io.netty.kv.ErrorMap.ErrorAttribute.RETRY_NOW;
import static com.couchbase.client.core.io.netty.kv.ErrorMap.ErrorAttribute.TEMP;
import static java.util.Objects.requireNonNull;

/**
 * Handles a {@link NioKeyValueConnection}'s responses: completes the requests, or retries them (for example, after NOT_MY_VBUCKET, or an outdated collection),
 * and asks the connection to close when a response says to.
 * <p>
 * Used by the connection's event loop. Created once the handshake is done.
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
  private final Map<Integer, KeyValueInFlight> inFlight;
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
    Map<Integer, KeyValueInFlight> inFlight,
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
    KeyValueInFlight written = inFlight.remove(opaque);

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
          status = statusForErrorCode(errorCode);
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
    } else if (errorUnknown && errorMapIndicatesRetry(errorCode)) {
      RetryOrchestrator.maybeRetry(ioContext, request, RetryReason.KV_ERROR_MAP_INDICATED);
    } else if (statusIndicatesInvalidChannel(status, bucketName != null)) {
      connection.closeProactively(ChannelClosedProactivelyEvent.Reason.KV_RESPONSE_CONTAINED_CLOSE_INDICATION, false);
      // Retry it like the other requests in flight.
      RetryOrchestrator.maybeRetry(ioContext, request, RetryReason.CHANNEL_CLOSED_WHILE_IN_FLIGHT);
    } else {
      retryOrComplete(written, packet, status, isRangeScanContinue);
    }
  }

  private void retryOrComplete(
    KeyValueInFlight written,
    KeyValuePacket packet,
    ResponseStatus status,
    boolean isRangeScanContinue
  ) {
    KeyValueRequest<Response> request = written.request;
    RetryReason retryReason = statusCodeIndicatesRetry(status, request);
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

  private void completeRequestTimings(KeyValueInFlight written, KeyValuePacket response) {
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

  private void decodeAndCompleteRangeScanContinue(KeyValueInFlight written, KeyValuePacket packet) {
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
      inFlight.put(request.opaque(), new KeyValueInFlight(request, written.dispatchStartNanos, null));
    }
  }

  private void handleUnknownResponseReceived(KeyValuePacket response) {
    eventBus.publish(new UnknownResponseReceivedEvent(ioContext, response.toByteArray()));
    // We got a response with an opaque value that we know nothing about. There is clearly something weird
    // going on so to be sure we close the connection to avoid any further weird situations.
    // Doesn't handle the rest of the responses already read: they might complete the wrong requests.
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
      new OutdatedCollectionContext(ioContext, ioContext.core().configurationProvider().collectionMap())
    ));

    ioContext.core().configurationProvider().refreshCollectionId(request.collectionIdentifier());
    RetryOrchestrator.maybeRetry(ioContext, request, retryReason);
  }

  /**
   * If certain status codes are returned from the server, there is a clear indication that the channel is
   * invalid and needs to be closed in order to avoid any further trouble.
   *
   * @param status the status code to check.
   * @param bucketSelected true if the connection selected a bucket.
   * @return true if it indicates an invalid channel that needs to be closed.
   */
  static boolean statusIndicatesInvalidChannel(ResponseStatus status, boolean bucketSelected) {
    return status == ResponseStatus.INTERNAL_SERVER_ERROR
      || (status == ResponseStatus.NO_BUCKET && bucketSelected)
      || status == ResponseStatus.NOT_INITIALIZED;
  }

  /**
   * Returns the status the client uses for an error code the client doesn't know, given its attributes
   * in the error map. (Doesn't check {@code CONN_STATE_INVALIDATED}, which means the caller closes the connection.)
   */
  static ResponseStatus statusForErrorCode(ErrorMap.ErrorCode errorCode) {
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
  static boolean errorMapIndicatesRetry(ErrorMap.@Nullable ErrorCode errorCode) {
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
  static @Nullable RetryReason statusCodeIndicatesRetry(ResponseStatus status, Request<?> request) {
    switch (status) {
      case LOCKED:
        return request instanceof UnlockRequest ? null : RetryReason.KV_LOCKED;
      case TEMPORARY_FAILURE:
      case SERVER_BUSY:
        return RetryReason.KV_TEMPORARY_FAILURE;
      case SYNC_WRITE_IN_PROGRESS:
        return RetryReason.KV_SYNC_WRITE_IN_PROGRESS;
      case SYNC_WRITE_RE_COMMIT_IN_PROGRESS:
        return RetryReason.KV_SYNC_WRITE_RE_COMMIT_IN_PROGRESS;
      default:
        return null;
    }
  }

  static final class OutdatedCollectionContext extends IoContext {

    private final CollectionMap collectionMap;

    OutdatedCollectionContext(IoContext ioContext, CollectionMap collectionMap) {
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
