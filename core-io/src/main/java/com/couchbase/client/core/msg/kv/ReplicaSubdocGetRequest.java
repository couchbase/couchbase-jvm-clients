/*
 * Copyright (c) 2023 Couchbase, Inc.
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

package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.api.kv.CoreSubdocGetCommand;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class ReplicaSubdocGetRequest extends SubdocGetRequest {

  private static final byte SUBDOC_DOC_FLAG_REPLICA = (byte) 0x20;

  private final short replica;

  public static ReplicaSubdocGetRequest create(final Duration timeout, final CoreContext ctx, CollectionIdentifier collectionIdentifier,
                                               final RetryStrategy retryStrategy, final String key,
                                               final byte flags, final List<CoreSubdocGetCommand> commands, final short replica, final RequestSpan span) {
    byte requestFlags = (byte)(flags | SUBDOC_DOC_FLAG_REPLICA);
    return new ReplicaSubdocGetRequest(timeout, ctx, collectionIdentifier, retryStrategy, key, requestFlags, convertCommands(commands), replica, span);
  }

  ReplicaSubdocGetRequest(final Duration timeout, final CoreContext ctx, CollectionIdentifier collectionIdentifier,
                          final RetryStrategy retryStrategy, final String key,
                          final byte flags, final List<SubdocGetRequest.Command> commands, final short replica, final RequestSpan span) {
    super(timeout, ctx, collectionIdentifier, retryStrategy, key, flags, commands, span);
    this.replica = replica;
    if (span != null) {
      span.attribute(TracingIdentifiers.ATTR_OPERATION, TracingIdentifiers.SPAN_REQUEST_KV_LOOKUP_IN_REPLICA);
    }
  }

  @Override
  public int replica() {
    return replica;
  }

  @Override
  public Map<String, Object> serviceContext() {
    Map<String, Object> ctx = super.serviceContext();
    ctx.put("isReplica", true);
    ctx.put("replicaNum", replica);
    return ctx;
  }

  @Override
  public String name() {
    return "lookup_in_replica";
  }

}
