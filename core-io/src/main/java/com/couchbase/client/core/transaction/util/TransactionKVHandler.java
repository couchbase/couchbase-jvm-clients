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
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreExpiry;
import com.couchbase.client.core.api.kv.CoreReadPreference;
import com.couchbase.client.core.api.kv.CoreSubdocGetResult;
import com.couchbase.client.core.classic.ClassicExpiryHelper;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.config.BucketCapabilities;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.msg.kv.InsertRequest;
import com.couchbase.client.core.msg.kv.InsertResponse;
import com.couchbase.client.core.msg.kv.RemoveRequest;
import com.couchbase.client.core.msg.kv.RemoveResponse;
import com.couchbase.client.core.msg.kv.SubdocGetRequest;
import com.couchbase.client.core.msg.kv.SubdocGetResponse;
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;
import com.couchbase.client.core.msg.kv.SubdocMutateResponse;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.service.kv.ReplicaHelper;
import com.couchbase.client.core.transaction.support.SpanWrapper;
import com.couchbase.client.core.transaction.support.SpanWrapperUtil;
import com.couchbase.client.core.util.BucketConfigUtil;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import java.util.concurrent.ExecutionException;

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

    public static InsertResponse insert(final Core core,
                                              CollectionIdentifier collectionIdentifier,
                                              final String id,
                                              final byte[] transcodedContent,
                                              final int flags,
                                              final Duration timeout,
                                              final Optional<DurabilityLevel> durabilityLevel,
                                              final Map<String, Object> clientContext,
                                              final SpanWrapper pspan,
                                              final @Nullable CoreExpiry expiry) {
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
        try {
            // See comments elsewhere in this file for why we do not timeout-bound the .get() here
            InsertResponse response = request.response().get();
            if (response.status().success()) {
                request.context().logicallyComplete();
                return response;
            }
            throw response.errorIfNeeded(request);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw handleBlockingException(span, request, e);
        } catch (ExecutionException e) {
            throw handleBlockingException(span, request, e);
        } finally {
            span.finish();
        }
    }

    public static RemoveResponse remove(final Core core,
                                              CollectionIdentifier collectionIdentifier,
                                              final String id,
                                              final Duration timeout,
                                              long cas,
                                              final Optional<DurabilityLevel> durabilityLevel,
                                              final Map<String, Object> clientContext,
                                              final SpanWrapper pspan) {
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
        try {
            RemoveResponse response = request.response().get();
            if (response.status().success()) {
                request.context().logicallyComplete();
                return response;
            }
            throw keyValueStatusToException(request, response);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw handleBlockingException(span, request, e);
        } catch (ExecutionException e) {
            throw handleBlockingException(span, request, e);
        } finally {
            span.finish();
        }
    }

    public static CoreSubdocGetResult lookupIn(final Core core,
                                               CollectionIdentifier collectionIdentifier,
                                               final String id,
                                               final Duration timeout,
                                               boolean accessDeleted,
                                               final Map<String, Object> clientContext,
                                               @Nullable final SpanWrapper pspan,
                                               boolean preferredReplicaMode,
                                               final List<SubdocGetRequest.Command> commands) {
        long start = System.nanoTime();
        SpanWrapper span = SpanWrapperUtil.createOp(null, core.context().coreResources().requestTracerAndDecorator(), collectionIdentifier, id, TracingIdentifiers.SPAN_REQUEST_KV_LOOKUP_IN, pspan);

        SubdocGetRequest request = null;
        try {
            if (preferredReplicaMode) {
                CompletableFuture<CoreSubdocGetResult> replicas =
                        BucketConfigUtil.waitForBucketTopology(core, collectionIdentifier.bucket(), timeout).toFuture()
                                .thenCompose(bucketConfig -> {
                                    byte flags = 0;
                                    if (accessDeleted) {
                                        if (bucketConfig.bucket().capabilities().contains(SUBDOC_ACCESS_DELETED)) {
                                            flags = SubdocMutateRequest.SUBDOC_DOC_FLAG_ACCESS_DELETED;
                                        }
                                    }
                                    return ReplicaHelper.lookupInAnyReplicaAsync(core, collectionIdentifier, id, convertCommandsToCore(commands), timeout, BestEffortRetryStrategy.INSTANCE,
                                            clientContext, pspan == null ? null : pspan.span(), CoreReadPreference.PREFERRED_SERVER_GROUP_OR_ALL_AVAILABLE, flags, (r) -> r);
                                });

                // See comments elsewhere in this file for why we do not timeout-bound the .get() here
                return replicas.get();
            }

            byte flags = 0;
            if (accessDeleted) {
                flags |= SubdocMutateRequest.SUBDOC_DOC_FLAG_ACCESS_DELETED;
            }

            request = new SubdocGetRequest(timeout,
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
            // See comments elsewhere in this file for why we do not timeout-bound the .get() here
            SubdocGetResponse response = request.response().get();
            if (response.status().success() || response.status() == ResponseStatus.SUBDOC_FAILURE) {
                CoreSubdocGetResult result = response.toCore(CoreKeyspace.from(collectionIdentifier), id);
                request.context().logicallyComplete();
                return result;
            }

            throw keyValueStatusToException(request, response);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw handleBlockingException(span, request, e);
        } catch (ExecutionException e) {
            throw handleBlockingException(span, request, e);
        } finally {
            span.finish();
        }
    }

    public static SubdocMutateResponse mutateIn(final Core core,
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
                true,
                cas,
                userFlags,
                durabilityLevel,
                clientContext,
                span,
                null,
                commands);
    }

    public static SubdocMutateResponse mutateIn(final Core core,
                                                      CollectionIdentifier collectionIdentifier,
                                                      final String id,
                                                      final Duration timeout,
                                                      final boolean insertDocument,
                                                      final boolean upsertDocument,
                                                      final boolean reviveDocument,
                                                      final boolean accessDeleted,
                                                      final boolean createAsDeleted,
                                                      final boolean allowedToSendPreserveExpiry,
                                                      long cas,
                                                      int userFlags,
                                                      final Optional<DurabilityLevel> durabilityLevel,
                                                      final Map<String, Object> clientContext,
                                                      final SpanWrapper pspan,
                                                      @Nullable CoreExpiry expiry,
                                                      final List<SubdocMutateRequest.Command> commands) {
        SpanWrapper span = SpanWrapperUtil.createOp(null, core.context().coreResources().requestTracerAndDecorator(), collectionIdentifier, id, TracingIdentifiers.SPAN_REQUEST_KV_MUTATE_IN, pspan);
        long start = System.nanoTime();
        CompletableFuture<BucketConfig> bucketConfigFuture = BucketConfigUtil.waitForBucketConfig(core, collectionIdentifier.bucket(), timeout).toFuture();
        SubdocMutateRequest request = null;

        try {
            BucketConfig bucketConfig = bucketConfigFuture.get();
            // Preserve expiry is only supported on 7.0+; use presence of collections capability as the indicator.
            boolean supportsPreserveExpiry = bucketConfig.bucketCapabilities().contains(BucketCapabilities.COLLECTIONS);
            // Sub-doc does not allow sending both preserveExpiry and an expiry, at least with StoreSemantics.Replace.
            // Also cannot send preserveExpiry with StoreSemantics.Insert.
            boolean sendPreserveExpiry = allowedToSendPreserveExpiry && supportsPreserveExpiry && expiry == null && !insertDocument;

            request = new SubdocMutateRequest(timeout,
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
            // Note we intentionally don't use a timeout on the .get() as that will raise concurrent.TimeoutException
            // and bypass the SDK's standard timeout handling in BaseRequest.cancel().  The operation is still
            // bounded by that standard timeout handling.
            SubdocMutateResponse response = request.response().get();
            if (response.status().success()) {
                request.context().logicallyComplete();
                return response;
            }
            throw response.throwError(request, insertDocument);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw handleBlockingException(span, request, e);
        } catch (ExecutionException e) {
            throw handleBlockingException(span, request, e);
        } finally {
            span.finish();
        }
    }

    private static RuntimeException handleBlockingException(SpanWrapper span, @Nullable Request<?> request, Exception e) {
        Throwable cause = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
        if (request != null) {
            request.context().logicallyComplete(cause);
        }
        span.recordException(cause);
        if (cause instanceof RuntimeException) {
            return (RuntimeException) cause;
        }
        return new RuntimeException(cause);
    }
}
