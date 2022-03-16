/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.java.transactions.internal;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.transaction.internal.CoreTransactionCommitAmbiguousException;
import com.couchbase.client.core.error.transaction.internal.CoreTransactionExpiredException;
import com.couchbase.client.core.error.transaction.internal.CoreTransactionFailedException;
import com.couchbase.client.java.transactions.error.TransactionCommitAmbiguousException;
import com.couchbase.client.java.transactions.error.TransactionExpiredException;
import com.couchbase.client.java.transactions.error.TransactionFailedException;
import reactor.core.publisher.Mono;

@Stability.Internal
public class ErrorUtil {
    private ErrorUtil() {}

    public static <T> Mono<T> convertTransactionFailedInternal(Throwable err) {
        Throwable out = err;

        if (err instanceof CoreTransactionCommitAmbiguousException) {
            out = new TransactionCommitAmbiguousException((CoreTransactionCommitAmbiguousException) err);
        } else if (err instanceof CoreTransactionExpiredException) {
            out = new TransactionExpiredException((CoreTransactionExpiredException) err);
        } else if (err instanceof CoreTransactionFailedException) {
            out = new TransactionFailedException((CoreTransactionFailedException) err);
        }

        return Mono.error(out);
    }

    public static void convertTransactionFailedSingleQuery(RuntimeException err) {
        convertTransactionFailedInternal(err).block();
    }
}
