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
package com.couchbase.client.core.transaction.support;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import com.couchbase.client.core.transaction.CoreTransactionContext;
import com.couchbase.client.core.transaction.CoreTransactionsReactive;
import com.couchbase.client.core.transaction.config.CoreMergedTransactionConfig;
import com.couchbase.client.core.transaction.util.CoreTransactionAttemptContextHooks;

import java.util.Optional;

@Stability.Internal
public class TransactionAttemptContextFactory {
    public CoreTransactionAttemptContext create(Core core,
                                                CoreTransactionContext overall,
                                                CoreMergedTransactionConfig config,
                                                String attemptId,
                                                CoreTransactionsReactive parent,
                                                Optional<SpanWrapper> parentSpan) {
        return new CoreTransactionAttemptContext(core,
                overall,
                config,
                attemptId,
                parent,
                parentSpan,
                new CoreTransactionAttemptContextHooks());
    }
}

