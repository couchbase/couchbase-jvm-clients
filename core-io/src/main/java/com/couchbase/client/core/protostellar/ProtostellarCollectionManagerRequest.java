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
package com.couchbase.client.core.protostellar;

import com.couchbase.client.core.CoreProtostellar;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.Map;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;

@Stability.Internal
public class ProtostellarCollectionManagerRequest<TGrpcRequest> extends ProtostellarRequest<TGrpcRequest> {
  public ProtostellarCollectionManagerRequest(
    TGrpcRequest request,
    CoreProtostellar core,
    String bucketName,
    @Nullable String scopeName,
    @Nullable String collectionName,
    String requestName,
    RequestSpan span,
    Duration timeout,
    boolean readonly,
    RetryStrategy retryStrategy,
    Map<String, Object> clientContext) {
    super(request, core, ServiceType.MANAGER, requestName, span, timeout, readonly, retryStrategy, clientContext, 0, (ctx) -> {
      ctx.put("type", ServiceType.MANAGER.ident());
      ctx.put("bucket", redactMeta(bucketName));

      if (scopeName != null) {
        ctx.put("scope", redactMeta(scopeName));
      }
      if (collectionName != null) {
        ctx.put("collection", redactMeta(collectionName));
      }
    });
  }
}
