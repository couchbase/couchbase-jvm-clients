/*
 * Copyright (c) 2022 Couchbase, Inc.
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
package com.couchbase.client.core.transaction.util;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreExpiry;
import com.couchbase.client.core.api.kv.CoreReadPreference;
import com.couchbase.client.core.api.kv.CoreSubdocGetResult;
import com.couchbase.client.core.classic.ClassicExpiryHelper;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.config.BucketCapabilities;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.DocumentUnretrievableException;
import com.couchbase.client.core.error.context.ReducedKeyValueErrorContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.msg.kv.InsertRequest;
import com.couchbase.client.core.msg.kv.InsertResponse;
import com.couchbase.client.core.msg.kv.RemoveRequest;
import com.couchbase.client.core.msg.kv.RemoveResponse;
import com.couchbase.client.core.msg.kv.SubdocGetRequest;
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;
import com.couchbase.client.core.msg.kv.SubdocMutateResponse;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.service.kv.ReplicaHelper;
import com.couchbase.client.core.transaction.support.SpanWrapper;
import com.couchbase.client.core.transaction.log.CoreTransactionLogger;
import com.couchbase.client.core.transaction.support.SpanWrapperUtil;
import com.couchbase.client.core.util.BucketConfigUtil;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.error.DefaultErrorUtil.keyValueStatusToException;
import static com.couchbase.client.core.msg.kv.SubdocGetRequest.convertCommandsToCore;
import static com.couchbase.client.core.topology.BucketCapability.SUBDOC_ACCESS_DELETED;

/**
 * Transactions does a lot of KV work from core-io.  This logic is essentially a mini version of java-client, providing
 * the bare minimum of required KV functionality.
 */
@Stability.Internal
public class TransactionKVHandler {
    private TransactionKVHandler() {
    }

    public static Mono<InsertResponse> insert(final Core core,
                                              CollectionIdentifier collectionIdentifier,
                                              final String id,
                                              final byte[] transcodedContent,
                                              final int flags,
                                              final Duration timeout,
                                              final Optional<DurabilityLevel> durabilityLevel,
                                              final Map<String, Object> clientContext,
                                              final SpanWrapper pspan,
                                              final @Nullable CoreExpiry expiry) {
        return Mono.defer(() -> {
            long start = System.nanoTime();
            SpanWrapper span = SpanWrapperUtil.createOp(null, core.context().coreResources().requestTracerAndDecorator(), collectionIdentifier, id, TracingIdentifiers.SPAN_REQUEST_KV_INSERT, pspan);

            InsertRequest request = new InsertRequest(id,
                    transcodedContent,
                    expiry == null ? 0 : ClassicExpiryHelper.encode(expiry),
                    flags,
                    timeout,
                    core.context(),
                    collectionIdentifier,
                    BestEffortRetryStrategy.INSTANCE,
                    durabilityLevel,
                    span.span());
            request.context()
                    .clientContext(clientContext)
                    .encodeLatency(System.nanoTime() - start);

            core.send(request);
            return Mono.fromFuture(request
                    .response()
                    .thenApply(response -> {
                        if (response.status().success()) {
                            return response;
                        }
                        throw response.errorIfNeeded(request);
                    }).whenComplete((r, t) -> request.context().logicallyComplete(t)));
        });
    }

    public static Mono<RemoveResponse> remove(final Core core,
                                              CollectionIdentifier collectionIdentifier,
                                              final String id,
                                              final Duration timeout,
                                              long cas,
                                              final Optional<DurabilityLevel> durabilityLevel,
                                              final Map<String, Object> clientContext,
                                              final SpanWrapper pspan) {
        return Mono.defer(() -> {
            long start = System.nanoTime();
            SpanWrapper span = SpanWrapperUtil.createOp(null, core.context().coreResources().requestTracerAndDecorator(), collectionIdentifier, id, TracingIdentifiers.SPAN_REQUEST_KV_REMOVE, pspan);

            RemoveRequest request = new RemoveRequest(id,
                    cas,
                    timeout,
                    core.context(),
                    collectionIdentifier,
                    BestEffortRetryStrategy.INSTANCE,
                    durabilityLevel,
                    span.span());
            request.context()
                    .clientContext(clientContext)
                    .encodeLatency(System.nanoTime() - start);

            core.send(request);
            return Mono.fromFuture(request
                    .response()
                    .thenApply(response -> {
                        if (response.status().success()) {
                            return response;
                        }
                        throw keyValueStatusToException(request, response);
                    }).whenComplete((r, t) -> request.context().logicallyComplete(t)));
        });
    }

    public static Mono<CoreSubdocGetResult> lookupIn(final Core core,
                                                   CollectionIdentifier collectionIdentifier,
                                                   final String id,
                                                   final Duration timeout,
                                                   boolean accessDeleted,
                                                   final Map<String, Object> clientContext,
                                                   @Nullable final SpanWrapper pspan,
                                                   boolean preferredReplicaMode,
                                                   final List<SubdocGetRequest.Command> commands) {
        return Mono.defer(() -> {
            long start = System.nanoTime();
            SpanWrapper span = SpanWrapperUtil.createOp(null, core.context().coreResources().requestTracerAndDecorator(), collectionIdentifier, id, TracingIdentifiers.SPAN_REQUEST_KV_LOOKUP_IN, pspan);


            if (preferredReplicaMode) {
                CompletableFuture<CoreSubdocGetResult> replicas =
                        BucketConfigUtil.waitForBucketTopology(core, collectionIdentifier.bucket(), timeout).toFuture()
                                .thenCompose(bucketConfig -> {
                                    byte flags = 0;
                                    if (accessDeleted) {
                                        // We can only accessDeleted when the server supports it (8.0+).
                                        // Otherwise we will proceed with the operation though it is sub-optimal.
                                        if (bucketConfig.bucket().capabilities().contains(SUBDOC_ACCESS_DELETED)) {
                                            flags = SubdocMutateRequest.SUBDOC_DOC_FLAG_ACCESS_DELETED;
                                        }
                                    }
                                    return ReplicaHelper.lookupInAnyReplicaAsync(core, collectionIdentifier, id, convertCommandsToCore(commands), timeout, BestEffortRetryStrategy.INSTANCE,
                                            clientContext, pspan == null ? null : pspan.span(), CoreReadPreference.PREFERRED_SERVER_GROUP_OR_ALL_AVAILABLE, flags, (r) -> r);
                                });

                return Reactor.wrap(replicas, () -> {})
                        .switchIfEmpty(Mono.error(new DocumentUnretrievableException(ReducedKeyValueErrorContext.create(id, collectionIdentifier))))
                        .doOnError(span::recordException)
                        .doOnTerminate(span::finish);
            }

            byte flags = 0;
            if (accessDeleted) {
                flags |= SubdocMutateRequest.SUBDOC_DOC_FLAG_ACCESS_DELETED;
            }

            SubdocGetRequest request = new SubdocGetRequest(timeout,
                    core.context(),
                    collectionIdentifier,
                    BestEffortRetryStrategy.INSTANCE,
                    id,
                    flags,
                    commands,
                    span.span());

            request.context()
                    .clientContext(clientContext)
                    .encodeLatency(System.nanoTime() - start);

            core.send(request);
            return Mono.fromFuture(request
                    .response()
                    .thenApply(response -> {
                        if (response.status().success() || response.status() == ResponseStatus.SUBDOC_FAILURE) {
                            return response.toCore(CoreKeyspace.from(collectionIdentifier), id);
                        }
                        throw keyValueStatusToException(request, response);
                    })
                    .whenComplete((t, e) -> {
                      if (e == null || e instanceof DocumentNotFoundException) {
                        request.context().logicallyComplete();
                      } else {
                        request.context().logicallyComplete(e);
                      }
                    }));
        });
    }

    public static Mono<SubdocMutateResponse> mutateIn(final Core core,
                                                      CollectionIdentifier collectionIdentifier,
                                                      final String id,
                                                      final Duration timeout,
                                                      final boolean insertDocument,
                                                      final boolean upsertDocument,
                                                      final boolean reviveDocument,
                                                      final boolean accessDeleted,
                                                      final boolean createAsDeleted,
                                                      long cas,
                                                      int userFlags,
                                                      final Optional<DurabilityLevel> durabilityLevel,
                                                      final Map<String, Object> clientContext,
                                                      final SpanWrapper span,
                                                      final List<SubdocMutateRequest.Command> commands) {
        return mutateIn(core,
                collectionIdentifier,
                id,
                timeout,
                insertDocument,
                upsertDocument,
                reviveDocument,
                accessDeleted,
                createAsDeleted,
                cas,
                userFlags,
                durabilityLevel,
                clientContext,
                span,
                null,
                commands);
    }

    public static Mono<SubdocMutateResponse> mutateIn(final Core core,
                                                      CollectionIdentifier collectionIdentifier,
                                                      final String id,
                                                      final Duration timeout,
                                                      final boolean insertDocument,
                                                      final boolean upsertDocument,
                                                      final boolean reviveDocument,
                                                      final boolean accessDeleted,
                                                      final boolean createAsDeleted,
                                                      long cas,
                                                      int userFlags,
                                                      final Optional<DurabilityLevel> durabilityLevel,
                                                      final Map<String, Object> clientContext,
                                                      final SpanWrapper pspan,
                                                      @Nullable CoreExpiry expiry,
                                                      final List<SubdocMutateRequest.Command> commands) {
        return Mono.defer(() -> {
            SpanWrapper span = SpanWrapperUtil.createOp(null, core.context().coreResources().requestTracerAndDecorator(), collectionIdentifier, id, TracingIdentifiers.SPAN_REQUEST_KV_MUTATE_IN, pspan);
            long start = System.nanoTime();
            CompletableFuture<BucketConfig> bucketConfigFuture = BucketConfigUtil.waitForBucketConfig(core, collectionIdentifier.bucket(), timeout).toFuture();

            CompletableFuture<SubdocMutateResponse> future = bucketConfigFuture.thenCompose(bucketConfig -> {
                // Preserve expiry is only supported on 7.0+; use presence of collections capability as the indicator.
                boolean supportsPreserveExpiry = bucketConfig.bucketCapabilities().contains(BucketCapabilities.COLLECTIONS);
                // Sub-doc does not allow sending both preserveExpiry and an expiry, at least with StoreSemantics.Replace.
                // Also cannot send preserveExpiry with StoreSemantics.Insert.
                boolean sendPreserveExpiry = supportsPreserveExpiry && expiry == null && !insertDocument;

                SubdocMutateRequest request = new SubdocMutateRequest(timeout,
                        core.context(),
                        collectionIdentifier,
                        bucketConfig,
                        BestEffortRetryStrategy.INSTANCE,
                        id,
                        insertDocument,
                        upsertDocument,
                        reviveDocument,
                        accessDeleted,
                        createAsDeleted,
                        commands,
                        expiry == null ? 0 : ClassicExpiryHelper.encode(expiry),
                        sendPreserveExpiry,
                        cas,
                        userFlags,
                        durabilityLevel,
                        span.span()
                );
                request.context()
                        .clientContext(clientContext)
                        .encodeLatency(System.nanoTime() - start);

                core.send(request);
                return request
                        .response()
                        .thenApply(response -> {
                            if (response.status().success()) {
                                return response;
                            }
                            throw response.throwError(request, insertDocument);
                        }).whenComplete((r, t) -> request.context().logicallyComplete(t));
            });

            return Mono.fromFuture(future);
        });
    }
}
