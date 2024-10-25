/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.java.query;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.error.context.ReducedQueryErrorContext;
import com.couchbase.client.core.error.transaction.internal.CoreTransactionCommitAmbiguousException;
import com.couchbase.client.core.error.transaction.internal.CoreTransactionExpiredException;
import com.couchbase.client.core.error.transaction.internal.CoreTransactionFailedException;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.topology.NodeIdentifier;
import com.couchbase.client.java.transactions.error.TransactionCommitAmbiguousException;
import com.couchbase.client.java.transactions.error.TransactionExpiredException;
import com.couchbase.client.java.transactions.error.TransactionFailedException;
import reactor.util.annotation.Nullable;

import java.time.Duration;

import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

@Stability.Internal
public class QueryAccessor {
  /**
   * Used by the transactions library, this provides some binary interface protection against
   * QueryRequest/TargetedQueryRequest changing.
   */
  @Stability.Internal
  @SuppressWarnings("unused")
  public static QueryRequest targetedQueryRequest(String statement,
                                                  byte[] queryBytes,
                                                  String clientContextId,
                                                  @Nullable NodeIdentifier target,
                                                  boolean readonly,
                                                  RetryStrategy retryStrategy,
                                                  Duration timeout,
                                                  RequestSpan parentSpan,
                                                  Core core) {
    notNullOrEmpty(statement, "Statement", () -> new ReducedQueryErrorContext(statement));

    return new QueryRequest(timeout, core.context(), retryStrategy, core.context().authenticator(), statement,
        queryBytes, readonly, clientContextId, parentSpan, null, null, target);
  }

  public static RuntimeException convertCoreQueryError(Throwable err) {
    if (err instanceof CoreTransactionCommitAmbiguousException) {
      return new TransactionCommitAmbiguousException((CoreTransactionCommitAmbiguousException) err);
    } else if (err instanceof CoreTransactionExpiredException) {
      return new TransactionExpiredException((CoreTransactionExpiredException) err);
    } else if (err instanceof CoreTransactionFailedException) {
      return new TransactionFailedException((CoreTransactionFailedException) err);
    }

    if (err instanceof RuntimeException) {
      return (RuntimeException) err;
    }

    return new RuntimeException(err);
  }
}
