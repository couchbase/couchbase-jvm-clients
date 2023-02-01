/*
 * Copyright 2023 Couchbase, Inc.
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
package com.couchbase.client.core.classic.kv;

import static com.couchbase.client.core.error.DefaultErrorUtil.keyValueStatusToException;

import java.util.concurrent.CompletableFuture;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreCounterResult;
import com.couchbase.client.core.api.kv.CoreDurability;
import com.couchbase.client.core.api.kv.CoreKvResponseMetadata;
import com.couchbase.client.core.api.kv.CoreMutationResult;
import com.couchbase.client.core.classic.ClassicHelper;
import com.couchbase.client.core.msg.kv.AppendRequest;
import com.couchbase.client.core.msg.kv.DecrementRequest;
import com.couchbase.client.core.msg.kv.IncrementRequest;
import com.couchbase.client.core.msg.kv.PrependRequest;

@Stability.Internal
public class BinaryAccessor {

  public static CompletableFuture<CoreMutationResult> prepend(final Core core, final PrependRequest request,
      final String key, final CoreKeyspace keyspace, final CoreDurability durability) {
    core.send(request);
    final CompletableFuture<CoreMutationResult> mutationResult = request.response().thenApply(response -> {
      if (response.status().success()) {
        return new CoreMutationResult(CoreKvResponseMetadata.from(response.flexibleExtras()), keyspace, key,
            response.cas(), response.mutationToken());
      }
      throw keyValueStatusToException(request, response);
    });
    return ClassicHelper.maybeWrapWithLegacyDurability(mutationResult, key, durability, core, request);
  }

  public static CompletableFuture<CoreMutationResult> append(final Core core, final AppendRequest request,
      final String key, final CoreKeyspace keyspace, final CoreDurability durability) {
    core.send(request);
    final CompletableFuture<CoreMutationResult> mutationResult = request.response().thenApply(response -> {
      if (response.status().success()) {
        return new CoreMutationResult(CoreKvResponseMetadata.from(response.flexibleExtras()), keyspace, key,
            response.cas(), response.mutationToken());
      }
      throw keyValueStatusToException(request, response);
    });
    return ClassicHelper.maybeWrapWithLegacyDurability(mutationResult, key, durability, core, request);

  }

  public static CompletableFuture<CoreCounterResult> decrement(Core core, DecrementRequest request, String id,
      CoreKeyspace keyspace, CoreDurability durability) {

    core.send(request);
    final CompletableFuture<CoreCounterResult> mutationResult = request.response().thenApply(response -> {
      if (response.status().success()) {
        return new CoreCounterResult(CoreKvResponseMetadata.from(response.flexibleExtras()), keyspace, id,
            response.cas(), response.value(), response.mutationToken());
      }
      throw keyValueStatusToException(request, response);
    });
    return ClassicHelper.maybeWrapWithLegacyDurability(mutationResult, id, durability, core, request);
  }

  public static CompletableFuture<CoreCounterResult> increment(Core core, IncrementRequest request, String id,
      CoreKeyspace keyspace, CoreDurability durability) {
    core.send(request);
    final CompletableFuture<CoreCounterResult> mutationResult = request.response().thenApply(response -> {
      if (response.status().success()) {
        return new CoreCounterResult(CoreKvResponseMetadata.from(response.flexibleExtras()), keyspace, id,
            response.cas(), response.value(), response.mutationToken());
      }
      throw keyValueStatusToException(request, response);
    });
    return ClassicHelper.maybeWrapWithLegacyDurability(mutationResult, id, durability, core, request);
  }
}
