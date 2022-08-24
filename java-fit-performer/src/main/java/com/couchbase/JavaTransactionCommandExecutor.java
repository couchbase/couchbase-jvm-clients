/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// [skip:<3.3.0]
package com.couchbase;

import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.performer.core.commands.TransactionCommandExecutor;
import com.couchbase.client.performer.core.perf.Counters;
import com.couchbase.client.performer.core.util.TimeUtil;
import com.couchbase.client.protocol.shared.API;
import com.couchbase.client.protocol.transactions.TransactionResult;
import com.couchbase.twoway.TwoWayTransactionBlocking;
import com.couchbase.twoway.TwoWayTransactionReactive;
import com.couchbase.utils.ClusterConnection;

import java.util.concurrent.ConcurrentHashMap;


/**
 * SdkOperation performs each requested SDK operation
 */
public class JavaTransactionCommandExecutor extends TransactionCommandExecutor {

    private final ClusterConnection connection;
    private final ConcurrentHashMap<String, RequestSpan> spans;

    public JavaTransactionCommandExecutor(ClusterConnection connection, Counters counters, ConcurrentHashMap<String, RequestSpan> spans) {
        super(counters);
        this.connection = connection;
        this.spans = spans;
    }

    @Override
    protected com.couchbase.client.protocol.run.Result performOperation(com.couchbase.client.protocol.transactions.TransactionCreateRequest request, boolean performanceMode) {
        TransactionResult response;

        var initiated = TimeUtil.getTimeNow();
        long startNanos = System.nanoTime();
        if (request.getApi() == API.DEFAULT) {
            response = TwoWayTransactionBlocking.run(connection, request, this, performanceMode, spans);
        }
        else {
            throw new UnsupportedOperationException();
        }
        long elapsedNanos = System.nanoTime() - startNanos;

        return com.couchbase.client.protocol.run.Result.newBuilder()
                .setElapsedNanos(elapsedNanos)
                .setInitiated(initiated)
                .setTransaction(response)
                .build();
    }
}