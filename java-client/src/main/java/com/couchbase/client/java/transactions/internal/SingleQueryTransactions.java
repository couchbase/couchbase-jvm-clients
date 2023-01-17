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
package com.couchbase.client.java.transactions.internal;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.error.EncodingFailureException;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.transaction.CoreTransactionsReactive;
import com.couchbase.client.core.transaction.config.CoreSingleQueryTransactionOptions;
import com.couchbase.client.core.transaction.config.CoreTransactionsConfig;
import com.couchbase.client.core.transaction.support.SpanWrapper;
import com.couchbase.client.core.transaction.support.SpanWrapperUtil;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryResultHttp;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.couchbase.client.java.query.ReactiveQueryResultHttp;
import com.couchbase.client.java.transactions.error.TransactionExpiredException;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Consumer;

@Stability.Internal
public class SingleQueryTransactions {
    public static Mono<QueryResult> singleQueryTransactionBuffered(Core core,
                                                                   ClusterEnvironment environment,
                                                                   String statement,
                                                                   @Nullable String bucketName,
                                                                   @Nullable String scopeName,
                                                                   QueryOptions.Built opts) {
        if (opts.retryStrategy().isPresent()) {
            // Transactions require control of the retry strategy
            throw new IllegalArgumentException("Cannot specify retryStrategy() if using asTransaction() on QueryOptions");
        }

        CoreTransactionsReactive tri = configureTransactions(core, opts);
        final JsonObject json = JsonObject.create();
        opts.injectParams(json);
        try {
            SpanWrapper span = SpanWrapperUtil.createOp(null, core.context().environment().requestTracer(), null,
                    null, TracingIdentifiers.SPAN_REQUEST_QUERY, opts.parentSpan().map(SpanWrapper::new).orElse(null))
                    .attribute(TracingIdentifiers.ATTR_STATEMENT, statement)
                    .attribute(TracingIdentifiers.ATTR_TRANSACTION_SINGLE_QUERY, true);
            ObjectNode converted = Mapper.reader().readValue(json.toBytes(), ObjectNode.class);
            JsonSerializer serializer = opts.serializer() == null ? environment.jsonSerializer() : opts.serializer();

            return tri.queryBlocking(statement, bucketName, scopeName, converted, Optional.of(span.span()))
                    .map(qr -> (QueryResult) new QueryResultHttp(qr.header, qr.rows, qr.trailer, serializer))
                    .onErrorResume(ErrorUtil::convertTransactionFailedInternal)
                    .onErrorResume(ex -> {
                        // From a cluster.query() transaction the user will be expecting the traditional SDK errors
                        if (ex instanceof TransactionExpiredException) {
                            return Mono.error(new UnambiguousTimeoutException(ex.getMessage(), null));
                        }
                        return Mono.error(ex);
                    })
                    .doOnError(err -> span.finish(err))
                    .doOnTerminate(() -> span.finish());
        } catch (IOException e) {
            return Mono.error(new EncodingFailureException(e));
        }
    }

    private static CoreTransactionsReactive configureTransactions(Core core, QueryOptions.Built opts) {
        CoreSingleQueryTransactionOptions queryOpts = opts.asTransactionOptions();
        CoreTransactionsConfig transactionsConfig = core.context().environment().transactionsConfig();
        return new CoreTransactionsReactive(core,
                CoreTransactionsConfig.createForSingleQueryTransactions(queryOpts == null ? transactionsConfig.durabilityLevel() : queryOpts.durabilityLevel().orElse(transactionsConfig.durabilityLevel()),
                        opts.timeout().orElse(transactionsConfig.transactionExpirationTime()),
                        queryOpts == null ? null : queryOpts.attemptContextFactory().orElse(transactionsConfig.attemptContextFactory()),
                        queryOpts == null ? transactionsConfig.metadataCollection() : queryOpts.metadataCollection()));
    }


    public static Mono<ReactiveQueryResult> singleQueryTransactionStreaming(Core core,
                                                                            ClusterEnvironment environment,
                                                                            String statement,
                                                                            @Nullable String bucketName,
                                                                            @Nullable String scopeName,
                                                                            QueryOptions.Built opts,
                                                                            Consumer<RuntimeException> errorConverter) {
        if (opts.retryStrategy().isPresent()) {
            // Transactions require control of the retry strategy
            throw new IllegalArgumentException("Cannot specify retryStrategy() if using asTransaction() on QueryOptions");
        }

        CoreTransactionsReactive tri = configureTransactions(core, opts);
        final JsonObject json = JsonObject.create();
        opts.injectParams(json);
        try {
            SpanWrapper span = SpanWrapperUtil.createOp(null, core.context().environment().requestTracer(), null,
                    null, TracingIdentifiers.SPAN_REQUEST_QUERY, opts.parentSpan().map(SpanWrapper::new).orElse(null))
                    .attribute(TracingIdentifiers.ATTR_STATEMENT, statement)
                    .attribute(TracingIdentifiers.ATTR_TRANSACTION_SINGLE_QUERY, true);
            ObjectNode converted = Mapper.reader().readValue(json.toBytes(), ObjectNode.class);
            JsonSerializer serializer = opts.serializer() == null ? environment.jsonSerializer() : opts.serializer();
            return tri.query(statement, bucketName, scopeName, converted, Optional.of(span.span()), errorConverter)
                    .map(qr -> (ReactiveQueryResult) new ReactiveQueryResultHttp(qr, serializer))
                    .onErrorResume(ErrorUtil::convertTransactionFailedInternal)
                    .doOnError(err -> span.finish(err))
                    .doOnTerminate(() -> span.finish());
        } catch (IOException e) {
            return Mono.error(new EncodingFailureException(e));
        }
    }

}
