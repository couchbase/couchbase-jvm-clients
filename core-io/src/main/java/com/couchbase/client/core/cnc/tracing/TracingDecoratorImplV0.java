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

import com.couchbase.client.core.cnc.TracingIdentifiers;
import reactor.util.annotation.Nullable;

@Stability.Internal
public class TracingDecoratorImplV0 implements TracingDecoratorImpl {
  private static final String NET_TRANSPORT_TCP = "IP.TCP";

  @Override
  public String managerOrActualService(String actualService) {
    // V0 incorrectly used the ns-server service for some management operations.
    return TracingIdentifiers.SERVICE_MGMT;
  }

  @Override
  public String meterOperations() {
    return "db.couchbase.operations";
  }

  @Nullable
  public String attributeName(final TracingAttribute attribute) {
    switch (attribute) {
      case SERVICE:
        return "db.couchbase.service";
      case BUCKET_NAME:
        return "db.name";
      case SCOPE_NAME:
        return "db.couchbase.scope";
      case COLLECTION_NAME:
        return "db.couchbase.collection";
      case DOCUMENT_ID:
        return "db.couchbase.document_id";
      case DURABILITY:
        return "db.couchbase.durability";
      case OUTCOME:
        return "outcome";
      case SERVER_DURATION:
        return "db.couchbase.server_duration";
      case OPERATION_ID:
        return "db.couchbase.operation_id";
      case LOCAL_ID:
        return "db.couchbase.local_id";
      case LOCAL_HOSTNAME:
        return "net.host.name";
      case LOCAL_PORT:
        return "net.host.port";
      case REMOTE_HOSTNAME:
        return "net.peer.name";
      case REMOTE_PORT:
        return "net.peer.port";
      case PEER_HOSTNAME:
        return "network.peer.address";
      case PEER_PORT:
        return "network.peer.port";
      case RETRIES:
        return "db.couchbase.retries";
      case STATEMENT:
        return "db.statement";
      case TRANSACTION_SINGLE_QUERY:
        return "db.couchbase.transaction.single_query";
      case OPERATION:
        return "db.operation";
      case TRANSACTION_ID:
        return "db.couchbase.transaction.id";
      case TRANSACTION_ATTEMPT_ID:
        return "db.couchbase.transaction.attempt_id";
      case TRANSACTION_STATE:
        return "db.couchbase.transaction.state";
      case TRANSACTION_AGE:
        return "db.couchbase.transaction.age_ms";
      case TRANSACTION_CLEANUP_CLIENT_ID:
        return "db.couchbase.transactions.cleanup.client_id";
      case TRANSACTION_CLEANUP_WINDOW:
        return "db.couchbase.transactions.cleanup.window_ms";
      case TRANSACTION_CLEANUP_NUM_ATRS:
        return "db.couchbase.transactions.cleanup.client_atrs";
      case TRANSACTION_CLEANUP_NUM_ACTIVE:
        return "db.couchbase.transactions.cleanup.clients_active";
      case TRANSACTION_CLEANUP_NUM_EXPIRED:
        return "db.couchbase.transactions.cleanup.clients_expired";
      case TRANSACTION_ATR_ENTRIES_COUNT:
        return "db.couchbase.transactions.atr.entries_count";
      case TRANSACTION_ATR_ENTRIES_EXPIRED:
        return "db.couchbase.transactions.atr.entries_expired";
      case SYSTEM:
        return "db.system";
      case CLUSTER_NAME:
        return "db.couchbase.cluster_name";
      case CLUSTER_UUID:
        return "db.couchbase.cluster_uuid";
      case NET_TRANSPORT:
        return "net.transport";
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

    if (localHost != null) {
      span.attribute(requireAttributeName(TracingAttribute.LOCAL_HOSTNAME), localHost);
    }
    if (localPort != 0) {
      span.attribute(requireAttributeName(TracingAttribute.LOCAL_PORT), localPort);
    }
    if (remoteCanonicalHost != null) {
      span.attribute(requireAttributeName(TracingAttribute.REMOTE_HOSTNAME), remoteCanonicalHost);
    }
    if (remoteCanonicalPort != 0) {
      span.attribute(requireAttributeName(TracingAttribute.REMOTE_PORT), remoteCanonicalPort);
    }

    if (operationId != null) {
      span.attribute(requireAttributeName(TracingAttribute.OPERATION_ID), operationId);
    }
  }
}
