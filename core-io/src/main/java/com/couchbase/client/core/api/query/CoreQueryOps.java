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
package com.couchbase.client.core.api.query;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreAsyncResponse;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.topology.NodeIdentifier;
import com.couchbase.client.core.util.CbThrowables;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.couchbase.client.core.util.BlockingStreamingHelper.forEachBlocking;
import static com.couchbase.client.core.util.BlockingStreamingHelper.propagateAsCancellation;

@Stability.Internal
public interface CoreQueryOps {
  /**
   * Performs a query.  This can handle single-query transactions.
   *
   * @param statement the statement to execute
   * @param options query options
   * @param queryContext non-null iff this query is on a Scope
   * @param target this is required for some use-cases (transactions, notably, where all queries in a transaction attempt must be sent to the same node).  The way targets are
   * identified is currently very bound to Classic, and will need to be abstracted if we need to support the same functionality for Protostellar ultimately.  However, the nature of Protostellar makes
   * that unlikely.
   * @param errorConverter required for converting Core errors into their final SDK forms.  (It could be handled externally in most cases, but this DRYs the logic, and there is
   * at least one transactions use-case that requires doing this conversion on the rows as they stream back.
   */
  default CoreQueryResult queryBlocking(String statement,
                                        CoreQueryOptions options,
                                        @Nullable CoreQueryContext queryContext,
                                        @Nullable NodeIdentifier target,
                                        @Nullable Function<Throwable, RuntimeException> errorConverter) {
    return queryAsync(statement, options, queryContext, target, errorConverter).toBlocking();
  }

  default CoreQueryMetaData queryBlockingStreaming(
      String statement,
      CoreQueryOptions options,
      @Nullable CoreQueryContext queryContext,
      @Nullable NodeIdentifier target,
      @Nullable Function<Throwable, RuntimeException> errorConverter,
      Consumer<QueryChunkRow> callback
  ) {
    try {
      CoreReactiveQueryResult response = queryReactive(statement, options, queryContext, target, errorConverter)
          .blockOptional()
          .orElseThrow(NoSuchElementException::new);

      forEachBlocking(response.rows(), 16, callback);
      return response.metaData().block();

    } catch (Exception e) {
      // in case the thread was interrupted in a reactor "block" operator
      CbThrowables.findCause(e, InterruptedException.class)
          .ifPresent(it -> { throw propagateAsCancellation(it); });
      throw e;
    }
  }

  CoreAsyncResponse<CoreQueryResult> queryAsync(String statement,
                                                CoreQueryOptions options,
                                                @Nullable CoreQueryContext queryContext,
                                                @Nullable NodeIdentifier target,
                                                @Nullable Function<Throwable, RuntimeException> errorConverter);

  Mono<CoreReactiveQueryResult> queryReactive(String statement,
                                              CoreQueryOptions options,
                                              @Nullable CoreQueryContext queryContext,
                                              @Nullable NodeIdentifier target,
                                              @Nullable Function<Throwable, RuntimeException> errorConverter);
}
