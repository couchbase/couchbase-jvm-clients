/*
 * Copyright (c) 2025 Couchbase, Inc.
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
package com.couchbase.client.core.cnc.tracing;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;

import reactor.util.annotation.Nullable;

@Stability.Internal
public class TracingDecoratorImplV1 implements TracingDecoratorImpl {
  private static final String NET_TRANSPORT_TCP = "tcp";

  @Nullable
  public String attributeName(final TracingAttribute attribute) {
    switch (attribute) {
      case SERVICE:
        return "couchbase.service";
      case BUCKET_NAME:
        return "db.namespace";
      case SCOPE_NAME:
        return "couchbase.scope.name";
      case COLLECTION_NAME:
        return "couchbase.collection.name";
      case DOCUMENT_ID:
        return "couchbase.document_id";
      case DURABILITY:
        return "couchbase.durability";
      case OUTCOME:
        return "error.type";
      case SERVER_DURATION:
        return "couchbase.server_duration";
      case OPERATION_ID:
        return "couchbase.operation_id";
      case LOCAL_ID:
        return "couchbase.local_id";
      case LOCAL_HOSTNAME:
        return "net.host.name";
      case LOCAL_PORT:
        return "net.host.port";
      case REMOTE_HOSTNAME:
        return "server.address";
      case REMOTE_PORT:
        return "server.port";
      case PEER_HOSTNAME:
        return "network.peer.address";
      case PEER_PORT:
        return "network.peer.port";
      case RETRIES:
        return "couchbase.retries";
      case STATEMENT:
        return "db.query.text";
      case TRANSACTION_SINGLE_QUERY:
        return "couchbase.transaction.single_query";
      case OPERATION:
        return "db.operation.name";
      case TRANSACTION_ID:
        return "couchbase.transaction.id";
      case TRANSACTION_ATTEMPT_ID:
        return "couchbase.transaction.attempt_id";
      case TRANSACTION_STATE:
        return "couchbase.transaction.state";
      case TRANSACTION_AGE:
        return "couchbase.transaction.age_ms";
      case TRANSACTION_CLEANUP_CLIENT_ID:
        return "couchbase.transactions.cleanup.client_id";
      case TRANSACTION_CLEANUP_WINDOW:
        return "couchbase.transactions.cleanup.window_ms";
      case TRANSACTION_CLEANUP_NUM_ATRS:
        return "couchbase.transactions.cleanup.client_atrs";
      case TRANSACTION_CLEANUP_NUM_ACTIVE:
        return "couchbase.transactions.cleanup.clients_active";
      case TRANSACTION_CLEANUP_NUM_EXPIRED:
        return "couchbase.transactions.cleanup.clients_expired";
      case TRANSACTION_ATR_ENTRIES_COUNT:
        return "couchbase.transactions.atr.entries_count";
      case TRANSACTION_ATR_ENTRIES_EXPIRED:
        return "couchbase.transactions.atr.entries_expired";
      case SYSTEM:
        return "db.system.name";
      case CLUSTER_NAME:
        return "couchbase.cluster.name";
      case CLUSTER_UUID:
        return "couchbase.cluster.uuid";
      case NET_TRANSPORT:
        return "network.transport";
      default:
        return null;
    }
  }

  @Override
  public void provideCommonDispatchSpanAttributes(final RequestSpan span, @Nullable final String localId,
                                                  @Nullable final String localHost, final int localPort,
                                                  @Nullable final String remoteCanonicalHost, final int remoteCanonicalPort,
                                                  @Nullable final String remoteActualHost, final int remoteActualPort,
                                                  @Nullable final String operationId) {
    span.lowCardinalityAttribute(requireAttributeName(TracingAttribute.NET_TRANSPORT), NET_TRANSPORT_TCP);

    if (localId != null) {
      span.attribute(requireAttributeName(TracingAttribute.LOCAL_ID), localId);
    }

    if (remoteCanonicalHost != null) {
      span.attribute(requireAttributeName(TracingAttribute.REMOTE_HOSTNAME), remoteCanonicalHost);
    }
    if (remoteCanonicalPort != 0) {
      span.attribute(requireAttributeName(TracingAttribute.REMOTE_PORT), remoteCanonicalPort);
    }

    if (remoteActualHost != null) {
      span.attribute(requireAttributeName(TracingAttribute.PEER_HOSTNAME), remoteActualHost);
      span.attribute(requireAttributeName(TracingAttribute.PEER_PORT), remoteActualPort);
    }

    if (operationId != null) {
      span.attribute(requireAttributeName(TracingAttribute.OPERATION_ID), operationId);
    }
  }

  @Override
  public String managerOrActualService(String actualService) {
    return actualService;
  }

  @Override
  public String meterOperations() {
    return "db.client.operation.duration";
  }
}
