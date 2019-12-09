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
import com.couchbase.client.core.error.*;
import com.couchbase.client.core.msg.kv.DecrementRequest;
import com.couchbase.client.core.msg.kv.IncrementRequest;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.error.DefaultErrorUtil.keyValueStatusToException;
import static com.couchbase.client.java.kv.DurabilityUtils.wrapWithDurability;

public class CounterAccessor {

  public static CompletableFuture<CounterResult> increment(final Core core,
                                                           final IncrementRequest request,
                                                           final String key,
                                                           final PersistTo persistTo,
                                                           final ReplicateTo replicateTo) {
    core.send(request);
    final CompletableFuture<CounterResult> mutationResult = request
      .response()
      .thenApply(response -> {
        if (response.status().success()) {
          return new CounterResult(response.cas(), response.value(), response.mutationToken());
        }
        throw keyValueStatusToException(request, response);
      });
    return wrapWithDurability(mutationResult, key, persistTo, replicateTo, core, request, false);

  }

  public static CompletableFuture<CounterResult> decrement(final Core core,
                                                           final DecrementRequest request,
                                                           final String key,
                                                           final PersistTo persistTo,
                                                           final ReplicateTo replicateTo) {
    core.send(request);
    final CompletableFuture<CounterResult> mutationResult = request
      .response()
      .thenApply(response -> {
        if (response.status().success()) {
          return new CounterResult(response.cas(), response.value(), response.mutationToken());
        }

        final KeyValueErrorContext ctx = KeyValueErrorContext.completedRequest(request, response.status());
        switch (response.status()) {
          case NOT_FOUND: throw new DocumentNotFoundException(ctx);
          case EXISTS: throw new CasMismatchException(ctx);
          case LOCKED: throw new DocumentLockedException(ctx);
          case OUT_OF_MEMORY: throw new ServerOutOfMemoryException(ctx);
          case TEMPORARY_FAILURE: // intended fallthrough to the case below
          case SERVER_BUSY: throw new TemporaryFailureException(ctx);
          case DURABILITY_INVALID_LEVEL: throw new DurabilityLevelNotAvailableException(ctx);
          case DURABILITY_IMPOSSIBLE: throw new DurabilityImpossibleException(ctx);
          case SYNC_WRITE_AMBIGUOUS: throw new DurabilityAmbiguousException(ctx);
          case SYNC_WRITE_IN_PROGRESS: throw new DurableWriteInProgressException(ctx);
          case SYNC_WRITE_RE_COMMIT_IN_PROGRESS: throw new DurableWriteReCommitInProgressException(ctx);
          default: throw new CouchbaseException("Decrement operation failed", ctx);
        }
      });
    return wrapWithDurability(mutationResult, key, persistTo, replicateTo, core, request, false);

  }

}
