/*
 * Copyright (c) 2021 Couchbase, Inc.
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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.tracing.TracingAttribute;
import com.couchbase.client.core.cnc.tracing.TracingDecorator;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import com.couchbase.client.core.msg.kv.SyncDurabilityRequest;
import reactor.util.annotation.Nullable;

/**
 * Contains various utils to set attributes for tracing spans.
 */
@Stability.Internal
public class TracingUtils {

  private TracingUtils() {}

  /**
   * Sets common dispatch span attributes as per spec.
   *
   * @param span the affected span.
   * @param localId the local socket ID.
   * @param localHost the local hostname or ip.
   * @param localPort the local port.
   * @param remoteCanonicalHost the canonical remote hostname or ip.
   * @param remoteCanonicalPort the canonical remote port.
   * @param remoteActualHost the actual remote hostname or ip.
   * @param remoteActualPort the actual remote port.
   * @param operationId the unique operation ID - can be null (then ignored).
   */
  public static void setCommonDispatchSpanAttributes(final TracingDecorator tip,
                                                     final RequestSpan span, @Nullable final String localId,
                                                     @Nullable final String localHost, final int localPort,
                                                     @Nullable final String remoteCanonicalHost, final int remoteCanonicalPort,
                                                     @Nullable final String remoteActualHost, final int remoteActualPort,
                                                     @Nullable final String operationId) {
    tip.provideCommonDispatchSpanAttributes(span, localId, localHost, localPort, remoteCanonicalHost, remoteCanonicalPort,
            remoteActualHost, remoteActualPort, operationId);
  }

  /**
   * Sets attributes that are usefully duplicated across multiple spans.
   */
  public static void setCommonKVSpanAttributes(final RequestSpan span, final KeyValueRequest<Response> request) {
    CollectionIdentifier collectionIdentifier = request.collectionIdentifier();
    TracingDecorator tip = request.context().coreResources().tracingDecorator();
    if (collectionIdentifier != null) {
      tip.provideLowCardinalityAttr(TracingAttribute.BUCKET_NAME, span, collectionIdentifier.bucket());
      tip.provideAttr(TracingAttribute.SCOPE_NAME, span, collectionIdentifier.scope().orElse(CollectionIdentifier.DEFAULT_SCOPE));
      tip.provideAttr(TracingAttribute.COLLECTION_NAME, span, collectionIdentifier.collection().orElse(CollectionIdentifier.DEFAULT_COLLECTION));
    }
    tip.provideAttr(TracingAttribute.DOCUMENT_ID, span, new String(request.key()));
    if (request instanceof SyncDurabilityRequest) {
      SyncDurabilityRequest syncDurabilityRequest = (SyncDurabilityRequest) request;
      if (syncDurabilityRequest.durabilityLevel() != null) {
        tip.provideLowCardinalityAttr(TracingAttribute.DURABILITY, span, syncDurabilityRequest.durabilityLevel().map(Enum::name).orElse(DurabilityLevel.NONE.name()));
      }
    }
  }

  /**
   * Sets the server duration attribute, if larger than 0 (will ignore it otherwise).
   *
   * @param span the span where it should be set.
   * @param serverDuration the actual duration.
   */
  public static void setServerDurationAttribute(final TracingDecorator tip, final RequestSpan span, final long serverDuration) {
    if (serverDuration > 0) {
      tip.provideAttr(TracingAttribute.SERVER_DURATION, span, serverDuration);
    }
  }

}
