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
package com.couchbase.transactions;

import com.couchbase.InternalPerformerFailure;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.events.transaction.TransactionLogEvent;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.transaction.support.TransactionAttemptContextFactory;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryProfile;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.couchbase.client.java.transactions.config.SingleQueryTransactionOptions;
import com.couchbase.client.java.transactions.error.TransactionFailedException;
import com.couchbase.client.protocol.shared.API;
import com.couchbase.client.protocol.transactions.ExternalException;
import com.couchbase.client.protocol.transactions.TransactionException;
import com.couchbase.client.protocol.transactions.TransactionSingleQueryRequest;
import com.couchbase.client.protocol.transactions.TransactionSingleQueryResponse;
import com.couchbase.utils.ClusterConnection;
import com.couchbase.utils.HooksUtil;
import com.couchbase.utils.ResultValidation;
import com.couchbase.utils.ResultsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Executes single query transactions (tximplicit).
 */
public class SingleQueryTransactionExecutor {
    private static final Logger logger = LoggerFactory.getLogger(SingleQueryTransactionExecutor.class);

    public static TransactionSingleQueryResponse execute(TransactionSingleQueryRequest request,
                                                         ClusterConnection connection,
                                                         ConcurrentHashMap<String, RequestSpan> spans) {
        try {
            if (request.getApi() == API.DEFAULT) {
                return blocking(request, connection, spans);
            } else {
                return reactive(request, connection, spans);
            }
        } catch (TransactionFailedException err) {
            return TransactionSingleQueryResponse.newBuilder()
                    .setException(ResultsUtil.convertTransactionFailed(err))
                    .setExceptionCause(ResultsUtil.mapCause(err.getCause()))
                    .addAllLog(err.logs().stream().map(TransactionLogEvent::toString).collect(Collectors.toList()))
                    .build();
        } catch (CouchbaseException err) {
            return TransactionSingleQueryResponse.newBuilder()
                    .setException(TransactionException.EXCEPTION_UNKNOWN)
                    .setExceptionCause(ResultsUtil.mapCause(err))
                    .build();
        }
    }

    private static TransactionSingleQueryResponse blocking(TransactionSingleQueryRequest request,
                                                           ClusterConnection connection,
                                                           ConcurrentHashMap<String, RequestSpan> spans) {
        QueryOptions options = setSingleQueryTransactionOptions(request, connection, spans);
        QueryResult result;

        if (request.getQuery().hasScope()) {
            String bucketName = request.getQuery().getScope().getBucketName();
            String scopeName = request.getQuery().getScope().getScopeName();
            Scope scope = connection.cluster().bucket(bucketName).scope(scopeName);

            result = scope.query(request.getQuery().getStatement(), options);
        } else {
            result = connection.cluster().query(request.getQuery().getStatement(), options);
        }

        ResultValidation.validateQueryResult(request.getQuery(), result);

        return TransactionSingleQueryResponse.newBuilder().build();
    }

    private static TransactionSingleQueryResponse reactive(TransactionSingleQueryRequest request,
                                                           ClusterConnection connection,
                                                           ConcurrentHashMap<String, RequestSpan> spans) {
        QueryOptions options = setSingleQueryTransactionOptions(request, connection, spans);
        ReactiveQueryResult result;

        if (request.getQuery().hasScope()) {
            String bucketName = request.getQuery().getScope().getBucketName();
            String scopeName = request.getQuery().getScope().getScopeName();
            Scope scope = connection.cluster().bucket(bucketName).scope(scopeName);
            result = scope.reactive().query(request.getQuery().getStatement(), options).block();
        } else {
            result = connection.cluster().reactive().query(request.getQuery().getStatement(), options).block();
        }

        AtomicReference<TransactionException> errorDuringStreaming = new AtomicReference<>();
        AtomicReference<ExternalException> causeDuringStreaming = new AtomicReference<>();
        AtomicBoolean rowValidationPerformed = new AtomicBoolean();
        ArrayList<JsonObject> rows = new ArrayList<>();
        AtomicLong mutationCount = new AtomicLong();

        try {
            result.rowsAsObject()
                    // Avoiding .collectList() as want this to be as close to how the user would stream as possible
                    .doOnNext(rows::add)
                    .then(result.metaData()
                            .doOnNext(metaData -> {
                                mutationCount.set(metaData.metrics().get().mutationCount());
                                rowValidationPerformed.set(true);
                            }))
                    .block();
        }
        catch (TransactionFailedException err) {
            errorDuringStreaming.set(ResultsUtil.convertTransactionFailed((Exception) err));
            causeDuringStreaming.set(ResultsUtil.mapCause(err.getCause()));
        }
        catch (CouchbaseException err) {
            causeDuringStreaming.set(ResultsUtil.mapCause(err));
        }
        catch (RuntimeException err) {
            throw new InternalPerformerFailure(new IllegalArgumentException("Single query raised an illegal non-TransactionFailed error " + err));
        }

        if (errorDuringStreaming.get() == null) {
            if (!rowValidationPerformed.get()) {
                throw new InternalPerformerFailure(new IllegalStateException("Somehow row streaming did not error or do validation"));
            }

            ResultValidation.validateQueryResult(request.getQuery(), rows, mutationCount.get());
        }

        return TransactionSingleQueryResponse.newBuilder()
                .setExceptionDuringStreaming(
                        errorDuringStreaming.get() == null ? TransactionException.NO_EXCEPTION_THROWN : errorDuringStreaming.get())
                .setExceptionCauseDuringStreaming(
                        causeDuringStreaming.get() == null ? ExternalException.Unknown : causeDuringStreaming.get())
                .build();
    }

    private static QueryOptions setSingleQueryTransactionOptions(TransactionSingleQueryRequest request,
                                                                 ClusterConnection clusterConnection,
                                                                 ConcurrentHashMap<String, RequestSpan> spans) {

        if (request.hasQueryOptions()) {
            com.couchbase.client.protocol.sdk.query.QueryOptions grpcQueryOptions = request.getQueryOptions();
            com.couchbase.client.java.query.QueryOptions queryOptions = com.couchbase.client.java.query.QueryOptions.queryOptions();

            if (grpcQueryOptions.hasScanConsistency()) {
                queryOptions.scanConsistency(QueryScanConsistency.valueOf(grpcQueryOptions.getScanConsistency().name()));
            }

            if (grpcQueryOptions.getRawCount() > 0) {
                grpcQueryOptions.getRawMap().forEach(queryOptions::raw);
            }

            if (grpcQueryOptions.hasAdhoc()) {
                queryOptions.adhoc(grpcQueryOptions.getAdhoc());
            }

            if (grpcQueryOptions.hasProfile()) {
                queryOptions.profile(QueryProfile.valueOf(grpcQueryOptions.getProfile()));
            }

            if (grpcQueryOptions.hasReadonly()) {
                queryOptions.readonly(grpcQueryOptions.getReadonly());
            }

            if (grpcQueryOptions.getParametersNamedCount() > 0) {
                queryOptions.parameters(JsonArray.from(grpcQueryOptions.getParametersPositionalList()));
            }

            if (grpcQueryOptions.getParametersNamedCount() > 0) {
                queryOptions.parameters(JsonObject.from(grpcQueryOptions.getParametersNamedMap()));
            }

            if (grpcQueryOptions.hasFlexIndex()) {
                queryOptions.flexIndex(grpcQueryOptions.getFlexIndex());
            }

            if (grpcQueryOptions.hasPipelineCap()) {
                queryOptions.pipelineCap(grpcQueryOptions.getPipelineCap());
            }

            if (grpcQueryOptions.hasPipelineBatch()) {
                queryOptions.pipelineBatch(grpcQueryOptions.getPipelineBatch());
            }

            if (grpcQueryOptions.hasScanCap()) {
                queryOptions.scanCap(grpcQueryOptions.getScanCap());
            }

            if (grpcQueryOptions.hasScanWaitMillis()) {
                queryOptions.scanWait(Duration.ofMillis(grpcQueryOptions.getScanWaitMillis()));
            }

            if (grpcQueryOptions.hasTimeoutMillis()) {
                queryOptions.timeout(Duration.ofMillis(grpcQueryOptions.getTimeoutMillis()));
            }

            if (grpcQueryOptions.hasParentSpanId()) {
                queryOptions.parentSpan(spans.get(grpcQueryOptions.getParentSpanId()));
            }

            if (grpcQueryOptions.hasSingleQueryTransactionOptions()) {
                com.couchbase.client.protocol.sdk.query.SingleQueryTransactionOptions grpcSingleQueryOptions = grpcQueryOptions.getSingleQueryTransactionOptions();
                SingleQueryTransactionOptions singleQueryOptions = SingleQueryTransactionOptions.singleQueryTransactionOptions();

                if (grpcSingleQueryOptions.hasDurability()) {
                    singleQueryOptions.durabilityLevel(DurabilityLevel.valueOf(grpcSingleQueryOptions.getDurability().name()));
                }

                if (grpcSingleQueryOptions.getHookCount() > 0) {
                    var hooks = HooksUtil.configureHooks(grpcSingleQueryOptions.getHookList(), () -> clusterConnection);
                    try {
                        // Using reflection to avoid making this internal method public
                        var method = SingleQueryTransactionOptions.class.getDeclaredMethod("testFactory", TransactionAttemptContextFactory.class);
                        method.setAccessible(true);
                        method.invoke(singleQueryOptions, hooks);
                    }
                    catch (Throwable err) {
                        throw new InternalPerformerFailure(new RuntimeException(err));
                    }
                }

                return queryOptions.asTransaction(singleQueryOptions);
            }
            else {
                return queryOptions.asTransaction();
            }
        }
        else {
            return QueryOptions.queryOptions().asTransaction();
        }
    }
}