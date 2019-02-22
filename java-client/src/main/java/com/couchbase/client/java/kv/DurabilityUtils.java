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

package com.couchbase.client.java.kv;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import com.couchbase.client.core.service.kv.Observe;
import com.couchbase.client.core.service.kv.ObserveContext;

import java.util.concurrent.CompletableFuture;

public enum DurabilityUtils {
  ;


  /**
   * Helper method to wrap a mutation result to perform durability requirements if needed.
   */
  public static <T extends MutationResult> CompletableFuture<T> wrapWithDurability(final CompletableFuture<T> input,
                                                                     final String key,
                                                                     final PersistTo persistTo,
                                                                     final ReplicateTo replicateTo,
                                                                     final Core core,
                                                                     final KeyValueRequest<?> request,
                                                                     boolean remove) {
    if (persistTo == PersistTo.NONE && replicateTo == ReplicateTo.NONE) {
      return input;
    } else {
      return input.thenCompose(result -> {
        final ObserveContext ctx = new ObserveContext(
          core.context(),
          persistTo.coreHandle(),
          replicateTo.coreHandle(),
          result.mutationToken(),
          result.cas(),
          request.bucket(),
          key,
          request.collection(),
          remove,
          request.timeout()
        );
        return Observe.poll(ctx).toFuture().thenApply(v -> result);
      });
    }
  }
}
